import os, logging, json
from clickhouse_driver import Client
import xml.etree.ElementTree as XML
from api_service import ApiService


def setup_logging():
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )


def fix_opt_param_format(data):
    list_keys = ["am", "hm", "q", "p", "b", "max_stock_weight"]
    if isinstance(data, list):
        return [fix_opt_param_format(item) for item in data]
    elif isinstance(data, dict):
        ret = {
            "".join(word.capitalize() if i else word for i, word in enumerate(key.split("_"))): fix_opt_param_format(
                [value] if key in list_keys and not isinstance(value, list) else value
            )
            for key, value in data.items()
        }

        return ret
    else:
        return data


def main(logger: logging.Logger) -> int:
    conf_path = f"{os.path.dirname(__file__)}/config.json"
    conf_template = {"user": "<username>", "password": "<password>"}

    if not os.path.exists(conf_path):
        with open(conf_path, "w") as f:
            f.write(json.dumps(conf_template, indent=4))

        logger.error(
            "Config not found. Constructed a new config file at path `%s`. Please fill in the express credentials and rerun the program.",
            conf_path,
        )
        return 1

    conf = json.load(open(conf_path))
    if conf["user"] == conf_template["user"] and conf["password"] == conf_template["password"]:
        logger.error("Please fill in the express credentials in the file `%s`.", conf_path)
        return 2

    if "url" not in conf:
        conf["url"] = None

    svr = ApiService(conf["user"], conf["password"], conf["url"])

    svr.login()
    logger.info("Sucessfully logged in to `%s`", svr.url)

    MISSION_NAME = "sw_production"
    PRED_TABLE = "AshareSWPred"
    PRED_VERSION = "SWorksVer1.1"
    OPT_PARAM_AMOUNT = 10_000_000_000

    # Get prediction dates
    ch_conf = XML.parse("/home/qdu/.clickhouse-client/config.xml")
    ch_config = {child.tag: child.text for child in ch_conf.getroot()}
    client = Client(database="express", **ch_config)
    dates = client.execute(f"SELECT DISTINCT Date FROM {PRED_TABLE} WHERE Stname = '{PRED_VERSION}' ORDER BY Date")
    assert isinstance(dates, list)
    dates = [x[0].strftime("%Y-%m-%d") for x in dates]
    start_date, end_date = max(dates[0], "2022-01-04"), dates[-2]
    logger.info("Found predictions in table `%s` from %s to %s", PRED_TABLE, start_date, end_date)

    # Check existing tasks and expand
    tasks = list(filter(lambda x: x["MissionName"] == MISSION_NAME, svr.get_mission_list()["data"]))

    if tasks:
        logger.info("Found %d existing task(s) with mission name `%s`", len(tasks), MISSION_NAME)

        for task in tasks:
            if task["TimeFrame"]["DateEnd"] < end_date:
                req_body = {"date_end": end_date, "mission_id": task["MissionID"]}
                logger.info("Task `%s` is expandable to %s", req_body["mission_id"], end_date)
                rsp = svr.expand_backtest(req_body)
                if rsp["code"] != 0:
                    raise ValueError(f"Run backtest failed: {rsp['data'][0]['detail']}")
                logger.info("Sucessfully sent task `%s` expanding to %s", req_body["mission_id"], req_body["date_end"])
        return 0

    # Build new task
    logger.info("No existing task found with mission name `%s`. Building a new one.", MISSION_NAME)
    opt_param_sets = fix_opt_param_format({x["name"]: x for x in svr.get_opt_params()["data"]})
    assert isinstance(opt_param_sets, dict)

    for key, param in opt_param_sets.items():
        assert isinstance(param, dict)
        param.update({"amount": OPT_PARAM_AMOUNT, "restrictSt": key})

    req_body = {
        "MissionName": MISSION_NAME,
        "TimeFrame": {"DateStart": start_date, "DateEnd": end_date},
        "SampleScope": {"ExchID": ["SS", "SZ"]},
        "RiskModel": "Multi10",
        "PredSource": [{"tableName": PRED_TABLE, "stName": PRED_VERSION, "weight": 1}],
        "TradedPriceType": "VWAPPriceNoLimit",
        "OrderType": {},
        "Groups": [
            opt_param_sets["AshareMF-On-AM-EE-CSI1000"],
            opt_param_sets["AshareMF-On-AM-EE-CSI500"],
            opt_param_sets["AshareMF-On-AM-LO"],
            opt_param_sets["AshareMF-On-Res-Index-T0"],
            opt_param_sets["AshareMF-On-Res-Index-T1"],
            {
                "name": "CSI1000-Moderate",
                "prePos": None,
                "orderType": None,
                "restrictSt": "AshareMF-On-AM-LO",
                "mvLb": -0.5,
                "mvUb": 0.5,
                "idstLb": -0.05,
                "idstUb": 0.05,
                "benchWeight": {"000852.SH": 1},
                "amount": 10_000_000_000,
                "rho": 0.0018,
                "tradeLimit": 0.07,
                "hedgeRatio": 0,
                "shrinkage": 0.3,
                "am": [0.3],
                "hm": [0.5],
                "maxStockWeight": [0.007],
                "q": [0.1],
                "p": [0.5],
                "b": [1.6],
            },
        ],
        "Departure": "pred",
        # 实际仓位或目标仓位上传自定义文件时传入接口返回的 id 值
        "MissionID": "",
        "for_real": False,
    }
    logger.info("Constructed backtest config: %s", req_body)

    # Send request
    rsp = svr.run_backtest(req_body)
    if rsp["code"] != 0:
        raise ValueError(f"Run backtest failed: {rsp['data'][0]['detail']}")
    logger.info("Sucessfully sent task with mission id `%s`", rsp["data"][0]["missionId"])
    return 0


##
if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    setup_logging()

    code = main(logger)
    if code == 0:
        logger.info("DONE")
    else:
        logger.info("ABORTED %d", code)
