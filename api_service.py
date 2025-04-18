import requests


class ApiService:
    def __init__(self, username, password, url=None):
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.cookie = None
        self.url = url if url is not None else "http://192.168.47.59"

    def login(self):
        self.session.post(f"{self.url}/user/login", data={"username": self.username, "password": self.password})
        self.cookie = self.session.cookies

    # 获取基本参数
    def get_config(self):
        url = f"{self.url}/rocket/api/backtest/config"
        data = self.session.get(url)
        return data.json()

    # 实盘对应策略的优化参数，对应UI界面所选择的实盘参数
    def get_opt_params(self):
        url = f"{self.url}/rocket/api/backtest/optparams"
        data = self.session.get(url)
        return data.json()

    # 上传自定义文件，包括预测值、实际仓位、目标仓位
    def upload_custom_file(self, req_body, files):
        url = f"{self.url}/rocket/api/upload/file"
        data = self.session.post(url, data=req_body, files=files, timeout=10000)
        return data.json()

    # 获取回测列表
    def get_mission_list(self):
        url = f"{self.url}/rocket/api/backtest/list"
        data = self.session.get(url)
        return data.json()

    # 运行回测实例
    def run_backtest(self, req_body):
        url = f"{self.url}/rocket/api/backtest/run"
        data = self.session.post(url, json=req_body)
        return data.json()

    # 扩展回测任务
    def expand_backtest(self, req_body):
        url = f"{self.url}/rocket/api/backtest/expand"
        data = self.session.post(url, json=req_body)
        return data.json()

    # 重运行回测任务
    def retry_backtest(self, req_body):
        url = f"{self.url}/rocket/api/backtest/retry"
        data = self.session.post(url, json=req_body)
        return data.json()

    # 删除回测任务
    def del_backtest(self, req_body):
        url = f"{self.url}/rocket/api/backtest/delete"
        data = self.session.post(url, json=req_body)
        return data.json()

    # 组合Barra分析
    def get_barra_analysis(self, req_body):
        url = f"{self.url}/rocket/api/analysis/barra"
        data = self.session.post(url, json=req_body)
        return data.json()

    # 日频盈亏数据
    def get_daily_pnl(self, req_body):
        url = f"{self.url}/rocket/api/analysis/dailypnl"
        data = self.session.post(url, data=req_body)
        return data.json()

    # 合约日频仓位数据
    def get_daily_position(self, req_body):
        url = f"{self.url}/rocket/api/analysis/position"
        data = self.session.post(url, data=req_body)
        return data.json()

    # 市值分组
    def get_fraction(self, req_body):
        url = f"{self.url}/rocket/api/analysis/fraction"
        data = self.session.post(url, json=req_body)
        return data.json()

    # 预测值分析
    def get_predict_analysis(self, req_body):
        url = f"{self.url}/rocket/api/analysis/predict"
        data = self.session.post(url, json=req_body)
        return data.json()

    # 由 (id, name) 查询对应组合的具体属性
    def get_portfolio_details(self, req_body):
        url = f"{self.url}/rocket/api/backtest/portfolios"
        data = self.session.post(url, json=req_body)
        return data.json()

    # 获取指数日频收益率，返回 沪深300、中证500、中证1000、中证2000、全A等权日频收益率
    def get_bench_info(self):
        url = f"{self.url}/rocket/api/benchinfo"
        data = self.session.get(url)
        return data.json()

    # 获取风险因子收益系数日频序列，用于相关性矩阵的分析
    def get_barra_factor(self, req_body):
        url = f"{self.url}/rocket/api/analysis/barrafactor"
        data = self.session.post(url, json=req_body)
        return data.json()

    # 获取多组合分析数据
    def get_group_analysis(self, req_body):
        url = f"{self.url}/rocket/api/analysis/group"
        data = self.session.post(url, json=req_body)
        return data.json()
