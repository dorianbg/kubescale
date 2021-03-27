from utils.kubernetes_util import PodWrapper
from utils.prometheus_util import PrometheusUtil, MetricWrapper
from typing import List, Dict, Optional, Union
import datetime
from metrics.metric_src import MetricSource


class Latency(MetricSource):
    def __init__(self, prom_url: str, step_size_secs: int, params: dict):
        # look into externally supplying these 3 below metrics
        self.metric_name = "latency"
        self.cluster_name = params["cluster_name"]
        self.query_template = """
            histogram_quantile(0.95, rate( envoy_cluster_upstream_rq_time_bucket{envoy_cluster_name="{cluster_name}"}[1m]))
        """
        # latency via the
        # sum(rate(http_server_requests_seconds_sum{ uri="/bmark/request/{id}"}[1m]) /
        # rate(http_server_requests_seconds_count{uri="/bmark/request/{id}"}[1m]))
        # by (instance)

        # alternatuve Latency via Envoy:
        # rate(envoy_cluster_external_upstream_rq_time_sum[1m])/rate(envoy_cluster_external_upstream_rq_time_count[1m])
        # or in seconds
        # rate(envoy_cluster_external_upstream_rq_time_sum[1m])/(rate(envoy_cluster_external_upstream_rq_time_count[1m])*1000)

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
