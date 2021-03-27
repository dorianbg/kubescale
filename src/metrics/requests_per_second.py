from utils.kubernetes_util import PodWrapper
from utils.prometheus_util import PrometheusUtil, MetricWrapper
from typing import Dict, List
import datetime
from .metric_src import MetricSource


class RequestsPerSecond(MetricSource):
    def __init__(self, prom_url: str, step_size_secs: int, params: dict):
        # look into externally supplying these 3 below metrics
        self.metric_name = "requests_per_second"
        self.cluster_name = params["cluster_name"]
        self.query_template = """
            sum(rate(envoy_cluster_upstream_rq{{envoy_cluster_name="{cluster_name}"}}[1m])) by (envoy_cluster_name) 
        """
        # + sum(envoy_cluster_upstream_rq_active{envoy_cluster_name="python_webapp_cluster"}) by (envoy_cluster_name)
        # """
        # sum(irate(http_server_requests_seconds_count{uri="/bmark/request/{id}"}[1m])) by (instance)
        # """
        # generic
        self.prom_url: str = prom_url
        self.step_size_secs: int  = step_size_secs
        self.params = params

    def get_data(self, pods: List[PodWrapper], start_time: datetime.datetime, end_time: datetime.datetime) \
            -> Dict[PodWrapper, MetricWrapper]:
        query = self.query_template.format(cluster_name=self.cluster_name)
        return PrometheusUtil.app_metrics_query_helper(url=self.prom_url, query=query,
                                                       metric_name=self.metric_name, pods=pods,
                                                       start_time=start_time, end_time=end_time,
                                                       step=self.step_size_secs)
