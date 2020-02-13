import traceback
import sys
from operations import TopOperation
from operations import JoinOperation
from operations import AggregationOperation
from operations import FormulaOperation
from operations import FilterOperation
from connectors import DBFSConnector
from connectors import CosmosDBConnector
from datatransformations import TranformationsMainFlow
from automl import tpot_execution
from core import PipelineNotification
import json

try: 
	PipelineNotification.PipelineNotification().started_notification('5e1c54c548c393f8cb6a6dbf','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
	AvgMovieRecommendation_DBFSs = DBFSConnector.DBFSConnector.fetch([], {}, "5e1c54c548c393f8cb6a6dbf", spark, "{'url': '/Demo/Marketing/MovieRatings (2).csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapib3c8e0614707f7e6d2addea6ce7c33d0', 'dbfs_domain': 'westus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")

	PipelineNotification.PipelineNotification().completed_notification('5e1c54c548c393f8cb6a6dbf','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e1c54c548c393f8cb6a6dbf','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify','http://104.40.91.74:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e1c54c548c393f8cb6a6dc0','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
	AvgMovieRecommendation_AutoFE = TranformationsMainFlow.TramformationMain.run(["5e1c54c548c393f8cb6a6dbf"],{"5e1c54c548c393f8cb6a6dbf": AvgMovieRecommendation_DBFSs}, "5e1c54c548c393f8cb6a6dc0", spark,json.dumps( {"FE": [{"feature": "UserId", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "2597", "mean": "471.84", "stddev": "268.99", "min": "1", "max": "943", "missing": "0"}}, {"feature": "MovieId", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "2597", "mean": "426.42", "stddev": "330.78", "min": "1", "max": "1643", "missing": "0"}}, {"feature": "Rating", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "2597", "mean": "3.52", "stddev": "1.11", "min": "1.0", "max": "5.0", "missing": "0"}, "transformation": ""}, {"transformationsData": {"feature_label": "Timestamp"}, "feature": "Timestamp", "type": "date", "selected": "True", "replaceby": "random", "stats": {"count": "", "mean": "", "stddev": "", "min": "", "max": "", "missing": "0"}, "transformation": "Extract Date"}, {"feature": "AvgRating", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "2597", "mean": "3.52", "stddev": "0.47", "min": "1.51", "max": "4.8", "missing": "0"}, "transformation": ""}, {"feature": "Timestamp_dayofmonth", "transformation": "", "type": "numeric", "selected": "True", "stats": {"count": "4793", "mean": "16.16", "stddev": "9.11", "min": "1", "max": "31", "missing": "0"}}, {"feature": "Timestamp_month", "transformation": "", "type": "numeric", "selected": "True", "stats": {"count": "4793", "mean": "6.86", "stddev": "4.35", "min": "1", "max": "12", "missing": "0"}}, {"feature": "Timestamp_year", "transformation": "", "type": "numeric", "selected": "True", "stats": {"count": "4793", "mean": "1997.47", "stddev": "0.5", "min": "1997", "max": "1998", "missing": "0"}}]}))

	PipelineNotification.PipelineNotification().completed_notification('5e1c54c548c393f8cb6a6dc0','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e1c54c548c393f8cb6a6dc0','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify','http://104.40.91.74:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e1c54c548c393f8cb6a6dc1','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
	AvgMovieRecommendation_AutoML = tpot_execution.Tpot_execution.run(["5e1c54c548c393f8cb6a6dc0"],{"5e1c54c548c393f8cb6a6dc0": AvgMovieRecommendation_AutoFE}, "5e1c54c548c393f8cb6a6dc1", spark,json.dumps( {"model_type": "regression", "label": "AvgRating", "features": ["UserId", "MovieId", "Rating", "Timestamp", "Timestamp_dayofmonth", "Timestamp_month", "Timestamp_year"], "percentage": "20", "executionTime": "5", "sampling": "0", "sampling_value": "", "run_id": "85d973633c0d4463a64a2e310462ccc1", "ProjectName": "Retail Scenarios", "PipelineName": "AvgMovieRecommendation", "userid": "567a95c8ca676c1d07d5e3e7", "runid": "", "url_ResultView": "http://104.40.91.74:3200", "experiment_id": "895518857185768"}))

	PipelineNotification.PipelineNotification().completed_notification('5e1c54c548c393f8cb6a6dc1','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e1c54c548c393f8cb6a6dc1','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify','http://104.40.91.74:3200/logs/getProductLogs')
	sys.exit(1)

