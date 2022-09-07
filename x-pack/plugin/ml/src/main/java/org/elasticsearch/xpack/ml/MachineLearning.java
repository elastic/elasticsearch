/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.Processors;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.indices.AssociatedIndexDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.CircuitBreakerPlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.plugins.ShutdownAwarePlugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.SetResetModeActionRequest;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.MlMetaIndex;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlStatsIndex;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.CancelJobModelSnapshotUpgradeAction;
import org.elasticsearch.xpack.core.ml.action.ClearDeploymentCacheAction;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.action.DeleteCalendarAction;
import org.elasticsearch.xpack.core.ml.action.DeleteCalendarEventAction;
import org.elasticsearch.xpack.core.ml.action.DeleteDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.DeleteDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.DeleteExpiredDataAction;
import org.elasticsearch.xpack.core.ml.action.DeleteFilterAction;
import org.elasticsearch.xpack.core.ml.action.DeleteForecastAction;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.DeleteModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.DeleteTrainedModelAction;
import org.elasticsearch.xpack.core.ml.action.DeleteTrainedModelAliasAction;
import org.elasticsearch.xpack.core.ml.action.DeleteTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.action.EstimateModelMemoryAction;
import org.elasticsearch.xpack.core.ml.action.EvaluateDataFrameAction;
import org.elasticsearch.xpack.core.ml.action.ExplainDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.FinalizeJobExecutionAction;
import org.elasticsearch.xpack.core.ml.action.FlushJobAction;
import org.elasticsearch.xpack.core.ml.action.ForecastJobAction;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.core.ml.action.GetCalendarEventsAction;
import org.elasticsearch.xpack.core.ml.action.GetCalendarsAction;
import org.elasticsearch.xpack.core.ml.action.GetCategoriesAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedRunningStateAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetDeploymentStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetFiltersAction;
import org.elasticsearch.xpack.core.ml.action.GetInfluencersAction;
import org.elasticsearch.xpack.core.ml.action.GetJobModelSnapshotsUpgradeStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.core.ml.action.GetOverallBucketsAction;
import org.elasticsearch.xpack.core.ml.action.GetRecordsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsStatsAction;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.action.InferTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.IsolateDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.KillProcessAction;
import org.elasticsearch.xpack.core.ml.action.MlInfoAction;
import org.elasticsearch.xpack.core.ml.action.MlMemoryAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PersistJobAction;
import org.elasticsearch.xpack.core.ml.action.PostCalendarEventsAction;
import org.elasticsearch.xpack.core.ml.action.PostDataAction;
import org.elasticsearch.xpack.core.ml.action.PreviewDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.PreviewDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutCalendarAction;
import org.elasticsearch.xpack.core.ml.action.PutDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutFilterAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAliasAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelDefinitionPartAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelVocabularyAction;
import org.elasticsearch.xpack.core.ml.action.ResetJobAction;
import org.elasticsearch.xpack.core.ml.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.SetResetModeAction;
import org.elasticsearch.xpack.core.ml.action.SetUpgradeModeAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.StopDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.TrainedModelCacheInfoAction;
import org.elasticsearch.xpack.core.ml.action.UpdateCalendarJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.UpdateDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateFilterAction;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.UpdateProcessAction;
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelAssignmentRoutingInfoAction;
import org.elasticsearch.xpack.core.ml.action.UpgradeJobModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.ValidateDetectorAction;
import org.elasticsearch.xpack.core.ml.action.ValidateJobConfigAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsTaskState;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.MlDataFrameAnalysisNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.dataframe.stats.AnalysisStatsNamedWriteablesProvider;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeTaskParams;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeTaskState;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.template.TemplateUtils;
import org.elasticsearch.xpack.ml.action.TransportCancelJobModelSnapshotUpgradeAction;
import org.elasticsearch.xpack.ml.action.TransportClearDeploymentCacheAction;
import org.elasticsearch.xpack.ml.action.TransportCloseJobAction;
import org.elasticsearch.xpack.ml.action.TransportCreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.ml.action.TransportDeleteCalendarAction;
import org.elasticsearch.xpack.ml.action.TransportDeleteCalendarEventAction;
import org.elasticsearch.xpack.ml.action.TransportDeleteDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.action.TransportDeleteDatafeedAction;
import org.elasticsearch.xpack.ml.action.TransportDeleteExpiredDataAction;
import org.elasticsearch.xpack.ml.action.TransportDeleteFilterAction;
import org.elasticsearch.xpack.ml.action.TransportDeleteForecastAction;
import org.elasticsearch.xpack.ml.action.TransportDeleteJobAction;
import org.elasticsearch.xpack.ml.action.TransportDeleteModelSnapshotAction;
import org.elasticsearch.xpack.ml.action.TransportDeleteTrainedModelAction;
import org.elasticsearch.xpack.ml.action.TransportDeleteTrainedModelAliasAction;
import org.elasticsearch.xpack.ml.action.TransportDeleteTrainedModelAssignmentAction;
import org.elasticsearch.xpack.ml.action.TransportEstimateModelMemoryAction;
import org.elasticsearch.xpack.ml.action.TransportEvaluateDataFrameAction;
import org.elasticsearch.xpack.ml.action.TransportExplainDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.action.TransportExternalInferModelAction;
import org.elasticsearch.xpack.ml.action.TransportFinalizeJobExecutionAction;
import org.elasticsearch.xpack.ml.action.TransportFlushJobAction;
import org.elasticsearch.xpack.ml.action.TransportForecastJobAction;
import org.elasticsearch.xpack.ml.action.TransportGetBucketsAction;
import org.elasticsearch.xpack.ml.action.TransportGetCalendarEventsAction;
import org.elasticsearch.xpack.ml.action.TransportGetCalendarsAction;
import org.elasticsearch.xpack.ml.action.TransportGetCategoriesAction;
import org.elasticsearch.xpack.ml.action.TransportGetDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.action.TransportGetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.ml.action.TransportGetDatafeedRunningStateAction;
import org.elasticsearch.xpack.ml.action.TransportGetDatafeedsAction;
import org.elasticsearch.xpack.ml.action.TransportGetDatafeedsStatsAction;
import org.elasticsearch.xpack.ml.action.TransportGetDeploymentStatsAction;
import org.elasticsearch.xpack.ml.action.TransportGetFiltersAction;
import org.elasticsearch.xpack.ml.action.TransportGetInfluencersAction;
import org.elasticsearch.xpack.ml.action.TransportGetJobModelSnapshotsUpgradeStatsAction;
import org.elasticsearch.xpack.ml.action.TransportGetJobsAction;
import org.elasticsearch.xpack.ml.action.TransportGetJobsStatsAction;
import org.elasticsearch.xpack.ml.action.TransportGetModelSnapshotsAction;
import org.elasticsearch.xpack.ml.action.TransportGetOverallBucketsAction;
import org.elasticsearch.xpack.ml.action.TransportGetRecordsAction;
import org.elasticsearch.xpack.ml.action.TransportGetTrainedModelsAction;
import org.elasticsearch.xpack.ml.action.TransportGetTrainedModelsStatsAction;
import org.elasticsearch.xpack.ml.action.TransportInferTrainedModelDeploymentAction;
import org.elasticsearch.xpack.ml.action.TransportInternalInferModelAction;
import org.elasticsearch.xpack.ml.action.TransportIsolateDatafeedAction;
import org.elasticsearch.xpack.ml.action.TransportKillProcessAction;
import org.elasticsearch.xpack.ml.action.TransportMlInfoAction;
import org.elasticsearch.xpack.ml.action.TransportMlMemoryAction;
import org.elasticsearch.xpack.ml.action.TransportOpenJobAction;
import org.elasticsearch.xpack.ml.action.TransportPersistJobAction;
import org.elasticsearch.xpack.ml.action.TransportPostCalendarEventsAction;
import org.elasticsearch.xpack.ml.action.TransportPostDataAction;
import org.elasticsearch.xpack.ml.action.TransportPreviewDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.action.TransportPreviewDatafeedAction;
import org.elasticsearch.xpack.ml.action.TransportPutCalendarAction;
import org.elasticsearch.xpack.ml.action.TransportPutDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.action.TransportPutDatafeedAction;
import org.elasticsearch.xpack.ml.action.TransportPutFilterAction;
import org.elasticsearch.xpack.ml.action.TransportPutJobAction;
import org.elasticsearch.xpack.ml.action.TransportPutTrainedModelAction;
import org.elasticsearch.xpack.ml.action.TransportPutTrainedModelAliasAction;
import org.elasticsearch.xpack.ml.action.TransportPutTrainedModelDefinitionPartAction;
import org.elasticsearch.xpack.ml.action.TransportPutTrainedModelVocabularyAction;
import org.elasticsearch.xpack.ml.action.TransportResetJobAction;
import org.elasticsearch.xpack.ml.action.TransportRevertModelSnapshotAction;
import org.elasticsearch.xpack.ml.action.TransportSetResetModeAction;
import org.elasticsearch.xpack.ml.action.TransportSetUpgradeModeAction;
import org.elasticsearch.xpack.ml.action.TransportStartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.action.TransportStartDatafeedAction;
import org.elasticsearch.xpack.ml.action.TransportStartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.ml.action.TransportStopDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.action.TransportStopDatafeedAction;
import org.elasticsearch.xpack.ml.action.TransportStopTrainedModelDeploymentAction;
import org.elasticsearch.xpack.ml.action.TransportTrainedModelCacheInfoAction;
import org.elasticsearch.xpack.ml.action.TransportUpdateCalendarJobAction;
import org.elasticsearch.xpack.ml.action.TransportUpdateDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.action.TransportUpdateDatafeedAction;
import org.elasticsearch.xpack.ml.action.TransportUpdateFilterAction;
import org.elasticsearch.xpack.ml.action.TransportUpdateJobAction;
import org.elasticsearch.xpack.ml.action.TransportUpdateModelSnapshotAction;
import org.elasticsearch.xpack.ml.action.TransportUpdateProcessAction;
import org.elasticsearch.xpack.ml.action.TransportUpdateTrainedModelAssignmentStateAction;
import org.elasticsearch.xpack.ml.action.TransportUpgradeJobModelSnapshotAction;
import org.elasticsearch.xpack.ml.action.TransportValidateDetectorAction;
import org.elasticsearch.xpack.ml.action.TransportValidateJobConfigAction;
import org.elasticsearch.xpack.ml.aggs.categorization.CategorizeTextAggregationBuilder;
import org.elasticsearch.xpack.ml.aggs.categorization.InternalCategorizationAggregation;
import org.elasticsearch.xpack.ml.aggs.changepoint.ChangePointAggregationBuilder;
import org.elasticsearch.xpack.ml.aggs.changepoint.ChangePointNamedContentProvider;
import org.elasticsearch.xpack.ml.aggs.changepoint.InternalChangePointAggregation;
import org.elasticsearch.xpack.ml.aggs.correlation.BucketCorrelationAggregationBuilder;
import org.elasticsearch.xpack.ml.aggs.correlation.CorrelationNamedContentProvider;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.FrequentItemSetsAggregationBuilder;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.FrequentItemSetsAggregatorFactory;
import org.elasticsearch.xpack.ml.aggs.heuristic.PValueScore;
import org.elasticsearch.xpack.ml.aggs.inference.InferencePipelineAggregationBuilder;
import org.elasticsearch.xpack.ml.aggs.kstest.BucketCountKSTestAggregationBuilder;
import org.elasticsearch.xpack.ml.aggs.kstest.InternalKSTestAggregation;
import org.elasticsearch.xpack.ml.annotations.AnnotationPersister;
import org.elasticsearch.xpack.ml.autoscaling.MlAutoscalingDeciderService;
import org.elasticsearch.xpack.ml.autoscaling.MlAutoscalingNamedWritableProvider;
import org.elasticsearch.xpack.ml.autoscaling.NodeAvailabilityZoneMapper;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfigAutoUpdater;
import org.elasticsearch.xpack.ml.datafeed.DatafeedContextProvider;
import org.elasticsearch.xpack.ml.datafeed.DatafeedJobBuilder;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManager;
import org.elasticsearch.xpack.ml.datafeed.DatafeedRunner;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsManager;
import org.elasticsearch.xpack.ml.dataframe.persistence.DataFrameAnalyticsConfigProvider;
import org.elasticsearch.xpack.ml.dataframe.process.AnalyticsProcessFactory;
import org.elasticsearch.xpack.ml.dataframe.process.AnalyticsProcessManager;
import org.elasticsearch.xpack.ml.dataframe.process.MemoryUsageEstimationProcessManager;
import org.elasticsearch.xpack.ml.dataframe.process.NativeAnalyticsProcessFactory;
import org.elasticsearch.xpack.ml.dataframe.process.NativeMemoryUsageEstimationProcessFactory;
import org.elasticsearch.xpack.ml.dataframe.process.results.AnalyticsResult;
import org.elasticsearch.xpack.ml.dataframe.process.results.MemoryUsageEstimationResult;
import org.elasticsearch.xpack.ml.inference.ModelAliasMetadata;
import org.elasticsearch.xpack.ml.inference.TrainedModelStatsService;
import org.elasticsearch.xpack.ml.inference.assignment.TrainedModelAssignmentClusterService;
import org.elasticsearch.xpack.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.ml.inference.assignment.TrainedModelAssignmentService;
import org.elasticsearch.xpack.ml.inference.deployment.DeploymentManager;
import org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;
import org.elasticsearch.xpack.ml.inference.modelsize.MlModelSizeNamedXContentProvider;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.inference.pytorch.process.BlackHolePyTorchProcess;
import org.elasticsearch.xpack.ml.inference.pytorch.process.NativePyTorchProcessFactory;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchProcessFactory;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.JobManagerHolder;
import org.elasticsearch.xpack.ml.job.NodeLoadDetector;
import org.elasticsearch.xpack.ml.job.UpdateJobProcessNotifier;
import org.elasticsearch.xpack.ml.job.categorization.FirstLineWithLettersCharFilter;
import org.elasticsearch.xpack.ml.job.categorization.FirstLineWithLettersCharFilterFactory;
import org.elasticsearch.xpack.ml.job.categorization.FirstNonBlankLineCharFilter;
import org.elasticsearch.xpack.ml.job.categorization.FirstNonBlankLineCharFilterFactory;
import org.elasticsearch.xpack.ml.job.categorization.MlClassicTokenizer;
import org.elasticsearch.xpack.ml.job.categorization.MlClassicTokenizerFactory;
import org.elasticsearch.xpack.ml.job.categorization.MlStandardTokenizer;
import org.elasticsearch.xpack.ml.job.categorization.MlStandardTokenizerFactory;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobDataCountsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectBuilder;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessFactory;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.job.process.autodetect.BlackHoleAutodetectProcess;
import org.elasticsearch.xpack.ml.job.process.autodetect.NativeAutodetectProcessFactory;
import org.elasticsearch.xpack.ml.job.process.normalizer.MultiplyingNormalizerProcess;
import org.elasticsearch.xpack.ml.job.process.normalizer.NativeNormalizerProcessFactory;
import org.elasticsearch.xpack.ml.job.process.normalizer.NormalizerFactory;
import org.elasticsearch.xpack.ml.job.process.normalizer.NormalizerProcessFactory;
import org.elasticsearch.xpack.ml.job.snapshot.upgrader.SnapshotUpgradeTaskExecutor;
import org.elasticsearch.xpack.ml.job.task.OpenJobPersistentTasksExecutor;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;
import org.elasticsearch.xpack.ml.notifications.SystemAuditor;
import org.elasticsearch.xpack.ml.process.DummyController;
import org.elasticsearch.xpack.ml.process.MlController;
import org.elasticsearch.xpack.ml.process.MlControllerHolder;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.process.NativeStorageProvider;
import org.elasticsearch.xpack.ml.rest.RestDeleteExpiredDataAction;
import org.elasticsearch.xpack.ml.rest.RestMlInfoAction;
import org.elasticsearch.xpack.ml.rest.RestMlMemoryAction;
import org.elasticsearch.xpack.ml.rest.RestSetUpgradeModeAction;
import org.elasticsearch.xpack.ml.rest.calendar.RestDeleteCalendarAction;
import org.elasticsearch.xpack.ml.rest.calendar.RestDeleteCalendarEventAction;
import org.elasticsearch.xpack.ml.rest.calendar.RestDeleteCalendarJobAction;
import org.elasticsearch.xpack.ml.rest.calendar.RestGetCalendarEventsAction;
import org.elasticsearch.xpack.ml.rest.calendar.RestGetCalendarsAction;
import org.elasticsearch.xpack.ml.rest.calendar.RestPostCalendarEventAction;
import org.elasticsearch.xpack.ml.rest.calendar.RestPutCalendarAction;
import org.elasticsearch.xpack.ml.rest.calendar.RestPutCalendarJobAction;
import org.elasticsearch.xpack.ml.rest.cat.RestCatDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.rest.cat.RestCatDatafeedsAction;
import org.elasticsearch.xpack.ml.rest.cat.RestCatJobsAction;
import org.elasticsearch.xpack.ml.rest.cat.RestCatTrainedModelsAction;
import org.elasticsearch.xpack.ml.rest.datafeeds.RestDeleteDatafeedAction;
import org.elasticsearch.xpack.ml.rest.datafeeds.RestGetDatafeedStatsAction;
import org.elasticsearch.xpack.ml.rest.datafeeds.RestGetDatafeedsAction;
import org.elasticsearch.xpack.ml.rest.datafeeds.RestPreviewDatafeedAction;
import org.elasticsearch.xpack.ml.rest.datafeeds.RestPutDatafeedAction;
import org.elasticsearch.xpack.ml.rest.datafeeds.RestStartDatafeedAction;
import org.elasticsearch.xpack.ml.rest.datafeeds.RestStopDatafeedAction;
import org.elasticsearch.xpack.ml.rest.datafeeds.RestUpdateDatafeedAction;
import org.elasticsearch.xpack.ml.rest.dataframe.RestDeleteDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.rest.dataframe.RestEvaluateDataFrameAction;
import org.elasticsearch.xpack.ml.rest.dataframe.RestExplainDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.rest.dataframe.RestGetDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.rest.dataframe.RestGetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.ml.rest.dataframe.RestPostDataFrameAnalyticsUpdateAction;
import org.elasticsearch.xpack.ml.rest.dataframe.RestPreviewDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.rest.dataframe.RestPutDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.rest.dataframe.RestStartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.rest.dataframe.RestStopDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.rest.filter.RestDeleteFilterAction;
import org.elasticsearch.xpack.ml.rest.filter.RestGetFiltersAction;
import org.elasticsearch.xpack.ml.rest.filter.RestPutFilterAction;
import org.elasticsearch.xpack.ml.rest.filter.RestUpdateFilterAction;
import org.elasticsearch.xpack.ml.rest.inference.RestClearDeploymentCacheAction;
import org.elasticsearch.xpack.ml.rest.inference.RestDeleteTrainedModelAction;
import org.elasticsearch.xpack.ml.rest.inference.RestDeleteTrainedModelAliasAction;
import org.elasticsearch.xpack.ml.rest.inference.RestGetTrainedModelsAction;
import org.elasticsearch.xpack.ml.rest.inference.RestGetTrainedModelsStatsAction;
import org.elasticsearch.xpack.ml.rest.inference.RestInferTrainedModelAction;
import org.elasticsearch.xpack.ml.rest.inference.RestInferTrainedModelDeploymentAction;
import org.elasticsearch.xpack.ml.rest.inference.RestPutTrainedModelAction;
import org.elasticsearch.xpack.ml.rest.inference.RestPutTrainedModelAliasAction;
import org.elasticsearch.xpack.ml.rest.inference.RestPutTrainedModelDefinitionPartAction;
import org.elasticsearch.xpack.ml.rest.inference.RestPutTrainedModelVocabularyAction;
import org.elasticsearch.xpack.ml.rest.inference.RestStartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.ml.rest.inference.RestStopTrainedModelDeploymentAction;
import org.elasticsearch.xpack.ml.rest.job.RestCloseJobAction;
import org.elasticsearch.xpack.ml.rest.job.RestDeleteForecastAction;
import org.elasticsearch.xpack.ml.rest.job.RestDeleteJobAction;
import org.elasticsearch.xpack.ml.rest.job.RestEstimateModelMemoryAction;
import org.elasticsearch.xpack.ml.rest.job.RestFlushJobAction;
import org.elasticsearch.xpack.ml.rest.job.RestForecastJobAction;
import org.elasticsearch.xpack.ml.rest.job.RestGetJobStatsAction;
import org.elasticsearch.xpack.ml.rest.job.RestGetJobsAction;
import org.elasticsearch.xpack.ml.rest.job.RestOpenJobAction;
import org.elasticsearch.xpack.ml.rest.job.RestPostDataAction;
import org.elasticsearch.xpack.ml.rest.job.RestPostJobUpdateAction;
import org.elasticsearch.xpack.ml.rest.job.RestPutJobAction;
import org.elasticsearch.xpack.ml.rest.job.RestResetJobAction;
import org.elasticsearch.xpack.ml.rest.modelsnapshots.RestDeleteModelSnapshotAction;
import org.elasticsearch.xpack.ml.rest.modelsnapshots.RestGetJobModelSnapshotsUpgradeStatsAction;
import org.elasticsearch.xpack.ml.rest.modelsnapshots.RestGetModelSnapshotsAction;
import org.elasticsearch.xpack.ml.rest.modelsnapshots.RestRevertModelSnapshotAction;
import org.elasticsearch.xpack.ml.rest.modelsnapshots.RestUpdateModelSnapshotAction;
import org.elasticsearch.xpack.ml.rest.modelsnapshots.RestUpgradeJobModelSnapshotAction;
import org.elasticsearch.xpack.ml.rest.results.RestGetBucketsAction;
import org.elasticsearch.xpack.ml.rest.results.RestGetCategoriesAction;
import org.elasticsearch.xpack.ml.rest.results.RestGetInfluencersAction;
import org.elasticsearch.xpack.ml.rest.results.RestGetOverallBucketsAction;
import org.elasticsearch.xpack.ml.rest.results.RestGetRecordsAction;
import org.elasticsearch.xpack.ml.rest.validate.RestValidateDetectorAction;
import org.elasticsearch.xpack.ml.rest.validate.RestValidateJobConfigAction;
import org.elasticsearch.xpack.ml.utils.NativeMemoryCalculator;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX;
import static org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX;
import static org.elasticsearch.xpack.ml.utils.InferenceProcessorInfoExtractor.countInferenceProcessors;

public class MachineLearning extends Plugin
    implements
        SystemIndexPlugin,
        AnalysisPlugin,
        CircuitBreakerPlugin,
        IngestPlugin,
        PersistentTaskPlugin,
        SearchPlugin,
        ShutdownAwarePlugin {
    public static final String NAME = "ml";
    public static final String BASE_PATH = "/_ml/";
    // Endpoints that were deprecated in 7.x can still be called in 8.x using the REST compatibility layer
    public static final String PRE_V7_BASE_PATH = "/_xpack/ml/";
    public static final String DATAFEED_THREAD_POOL_NAME = NAME + "_datafeed";
    public static final String JOB_COMMS_THREAD_POOL_NAME = NAME + "_job_comms";
    public static final String NATIVE_INFERENCE_COMMS_THREAD_POOL_NAME = NAME + "_native_inference_comms";
    public static final String UTILITY_THREAD_POOL_NAME = NAME + "_utility";

    public static final String TRAINED_MODEL_CIRCUIT_BREAKER_NAME = "model_inference";

    private static final long DEFAULT_MODEL_CIRCUIT_BREAKER_LIMIT = (long) ((0.50) * JvmInfo.jvmInfo().getMem().getHeapMax().getBytes());
    private static final double DEFAULT_MODEL_CIRCUIT_BREAKER_OVERHEAD = 1.0D;
    // This is for performance testing. It's not exposed to the end user.
    // Recompile if you want to compare performance with C++ tokenization.
    public static final boolean CATEGORIZATION_TOKENIZATION_IN_JAVA = true;

    public static final LicensedFeature.Persistent ML_ANOMALY_JOBS_FEATURE = LicensedFeature.persistent(
        MachineLearningField.ML_FEATURE_FAMILY,
        "anomaly-detection-job",
        License.OperationMode.PLATINUM
    );
    public static final LicensedFeature.Persistent ML_ANALYTICS_JOBS_FEATURE = LicensedFeature.persistent(
        MachineLearningField.ML_FEATURE_FAMILY,
        "data-frame-analytics-job",
        License.OperationMode.PLATINUM
    );
    public static final LicensedFeature.Persistent ML_MODEL_INFERENCE_FEATURE = LicensedFeature.persistent(
        MachineLearningField.ML_FEATURE_FAMILY,
        "model-inference",
        License.OperationMode.PLATINUM
    );
    public static final LicensedFeature.Persistent ML_PYTORCH_MODEL_INFERENCE_FEATURE = LicensedFeature.persistent(
        MachineLearningField.ML_FEATURE_FAMILY,
        "pytorch-model-inference",
        License.OperationMode.PLATINUM
    );

    private static final LicensedFeature.Momentary CATEGORIZE_TEXT_AGG_FEATURE = LicensedFeature.momentary(
        MachineLearningField.ML_FEATURE_FAMILY,
        "categorize-text-agg",
        License.OperationMode.PLATINUM
    );
    private static final LicensedFeature.Momentary FREQUENT_ITEM_SETS_AGG_FEATURE = LicensedFeature.momentary(
        MachineLearningField.ML_FEATURE_FAMILY,
        "frequent-items-agg",
        License.OperationMode.PLATINUM
    );
    public static final LicensedFeature.Momentary INFERENCE_AGG_FEATURE = LicensedFeature.momentary(
        MachineLearningField.ML_FEATURE_FAMILY,
        "inference-agg",
        License.OperationMode.PLATINUM
    );
    private static final LicensedFeature.Momentary CHANGE_POINT_AGG_FEATURE = LicensedFeature.momentary(
        MachineLearningField.ML_FEATURE_FAMILY,
        "change-point-agg",
        License.OperationMode.PLATINUM
    );
    private static final LicensedFeature.Momentary BUCKET_CORRELATION_AGG_FEATURE = LicensedFeature.momentary(
        MachineLearningField.ML_FEATURE_FAMILY,
        "bucket-correlation-agg",
        License.OperationMode.PLATINUM
    );
    private static final LicensedFeature.Momentary BUCKET_COUNT_KS_TEST_AGG_FEATURE = LicensedFeature.momentary(
        MachineLearningField.ML_FEATURE_FAMILY,
        "bucket-count-ks-test-agg",
        License.OperationMode.PLATINUM
    );

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        if (this.enabled == false) {
            return Collections.emptyMap();
        }

        InferenceProcessor.Factory inferenceFactory = new InferenceProcessor.Factory(
            parameters.client,
            parameters.ingestService.getClusterService(),
            this.settings
        );
        parameters.ingestService.addIngestClusterStateListener(inferenceFactory);
        return Collections.singletonMap(InferenceProcessor.TYPE, inferenceFactory);
    }

    // This is not used in v8 and higher, but users are still prevented from setting it directly to avoid confusion
    private static final String PRE_V8_MAX_OPEN_JOBS_NODE_ATTR = "ml.max_open_jobs";
    public static final String MACHINE_MEMORY_NODE_ATTR = "ml.machine_memory";
    public static final String MAX_JVM_SIZE_NODE_ATTR = "ml.max_jvm_size";
    public static final String ALLOCATED_PROCESSORS_NODE_ATTR = "ml.allocated_processors";
    public static final Setting<Integer> CONCURRENT_JOB_ALLOCATIONS = Setting.intSetting(
        "xpack.ml.node_concurrent_job_allocations",
        2,
        0,
        Property.OperatorDynamic,
        Property.NodeScope
    );
    /**
     * The amount of memory needed to load the ML native code shared libraries. The assumption is that the first
     * ML job to run on a given node will do this, and then subsequent ML jobs on the same node will reuse the
     * same already-loaded code.
     */
    public static final ByteSizeValue NATIVE_EXECUTABLE_CODE_OVERHEAD = ByteSizeValue.ofMb(30);
    // Values higher than 100% are allowed to accommodate use cases where swapping has been determined to be acceptable.
    // Anomaly detector jobs only use their full model memory during background persistence, and this is deliberately
    // staggered, so with large numbers of jobs few will generally be persisting state at the same time.
    // Settings higher than available memory are only recommended for OEM type situations where a wrapper tightly
    // controls the types of jobs that can be created, and each job alone is considerably smaller than what each node
    // can handle.
    public static final Setting<Integer> MAX_MACHINE_MEMORY_PERCENT = Setting.intSetting(
        "xpack.ml.max_machine_memory_percent",
        30,
        5,
        200,
        Property.OperatorDynamic,
        Property.NodeScope
    );
    /**
     * This boolean value indicates if `max_machine_memory_percent` should be ignored and a automatic calculation is used instead.
     *
     * This calculation takes into account total node size and the size of the JVM on that node.
     *
     * If the calculation fails, we fall back to `max_machine_memory_percent`.
     */
    public static final Setting<Boolean> USE_AUTO_MACHINE_MEMORY_PERCENT = Setting.boolSetting(
        "xpack.ml.use_auto_machine_memory_percent",
        false,
        Property.OperatorDynamic,
        Property.NodeScope
    );
    public static final Setting<Integer> MAX_LAZY_ML_NODES = Setting.intSetting(
        "xpack.ml.max_lazy_ml_nodes",
        0,
        0,
        Property.OperatorDynamic,
        Property.NodeScope
    );

    // Before 8.0.0 this needs to match the max allowed value for xpack.ml.max_open_jobs,
    // as the current node could be running in a cluster where some nodes are still using
    // that setting. From 8.0.0 onwards we have the flexibility to increase it...
    private static final int MAX_MAX_OPEN_JOBS_PER_NODE = 512;
    public static final int DEFAULT_MAX_OPEN_JOBS_PER_NODE = MAX_MAX_OPEN_JOBS_PER_NODE;
    // This setting is cluster-wide and can be set dynamically. However, prior to version 7.1 it was
    // a non-dynamic per-node setting. n a mixed version cluster containing 6.7 or 7.0 nodes those
    // older nodes will not react to the dynamic changes. Therefore, in such mixed version clusters
    // allocation will be based on the value first read at node startup rather than the current value.
    public static final Setting<Integer> MAX_OPEN_JOBS_PER_NODE = Setting.intSetting(
        "xpack.ml.max_open_jobs",
        DEFAULT_MAX_OPEN_JOBS_PER_NODE,
        1,
        MAX_MAX_OPEN_JOBS_PER_NODE,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<TimeValue> PROCESS_CONNECT_TIMEOUT = Setting.timeSetting(
        "xpack.ml.process_connect_timeout",
        TimeValue.timeValueSeconds(10),
        TimeValue.timeValueSeconds(5),
        Property.OperatorDynamic,
        Setting.Property.NodeScope
    );

    // Undocumented setting for integration test purposes
    public static final Setting<ByteSizeValue> MIN_DISK_SPACE_OFF_HEAP = Setting.byteSizeSetting(
        "xpack.ml.min_disk_space_off_heap",
        ByteSizeValue.ofGb(5),
        Setting.Property.NodeScope
    );

    // Requests per second throttling for the nightly maintenance task
    public static final Setting<Float> NIGHTLY_MAINTENANCE_REQUESTS_PER_SECOND = new Setting<>(
        "xpack.ml.nightly_maintenance_requests_per_second",
        (s) -> Float.toString(-1.0f),
        (s) -> {
            float value = Float.parseFloat(s);
            if (value <= 0.0f && value != -1.0f) {
                throw new IllegalArgumentException(
                    "Failed to parse value ["
                        + s
                        + "] for setting [xpack.ml.nightly_maintenance_requests_per_second] must be > 0.0 or exactly equal to -1.0"
                );
            }
            return value;
        },
        Property.OperatorDynamic,
        Property.NodeScope
    );

    /**
     * This is the maximum possible node size for a machine learning node. It is useful when determining if a job could ever be opened
     * on the cluster.
     *
     * If the value is the default special case of `0b`, that means the value is ignored when assigning jobs.
     */
    public static final Setting<ByteSizeValue> MAX_ML_NODE_SIZE = Setting.byteSizeSetting(
        "xpack.ml.max_ml_node_size",
        ByteSizeValue.ZERO,
        Property.OperatorDynamic,
        Property.NodeScope
    );

    /**
     * This is the global setting for how often datafeeds should check for delayed data.
     *
     * This is usually only modified by tests that require all datafeeds to check for delayed data more quickly
     */
    public static final Setting<TimeValue> DELAYED_DATA_CHECK_FREQ = Setting.timeSetting(
        "xpack.ml.delayed_data_check_freq",
        TimeValue.timeValueMinutes(15),
        TimeValue.timeValueSeconds(1),
        Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Each model deployment results in one or more entries in the cluster state
     * for the model allocations. In order to prevent the cluster state from
     * potentially growing uncontrollably we impose a limit on the number of
     * trained model deployments.
     */
    public static final int MAX_TRAINED_MODEL_DEPLOYMENTS = 100;

    private static final Logger logger = LogManager.getLogger(MachineLearning.class);

    private final Settings settings;
    private final boolean enabled;

    private final SetOnce<AutodetectProcessManager> autodetectProcessManager = new SetOnce<>();
    private final SetOnce<DatafeedConfigProvider> datafeedConfigProvider = new SetOnce<>();
    private final SetOnce<DatafeedRunner> datafeedRunner = new SetOnce<>();
    private final SetOnce<DataFrameAnalyticsManager> dataFrameAnalyticsManager = new SetOnce<>();
    private final SetOnce<DataFrameAnalyticsAuditor> dataFrameAnalyticsAuditor = new SetOnce<>();
    private final SetOnce<MlMemoryTracker> memoryTracker = new SetOnce<>();
    private final SetOnce<ActionFilter> mlUpgradeModeActionFilter = new SetOnce<>();
    private final SetOnce<MlLifeCycleService> mlLifeCycleService = new SetOnce<>();
    private final SetOnce<CircuitBreaker> inferenceModelBreaker = new SetOnce<>();
    private final SetOnce<ModelLoadingService> modelLoadingService = new SetOnce<>();
    private final SetOnce<MlAutoscalingDeciderService> mlAutoscalingDeciderService = new SetOnce<>();
    private final SetOnce<DeploymentManager> deploymentManager = new SetOnce<>();
    private final SetOnce<TrainedModelAssignmentClusterService> trainedModelAllocationClusterServiceSetOnce = new SetOnce<>();

    public MachineLearning(Settings settings) {
        this.settings = settings;
        this.enabled = XPackSettings.MACHINE_LEARNING_ENABLED.get(settings);
    }

    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    public static boolean isMlNode(DiscoveryNode node) {
        return node.getRoles().contains(DiscoveryNodeRole.ML_ROLE);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            MachineLearningField.AUTODETECT_PROCESS,
            PROCESS_CONNECT_TIMEOUT,
            CONCURRENT_JOB_ALLOCATIONS,
            MachineLearningField.MAX_MODEL_MEMORY_LIMIT,
            MAX_LAZY_ML_NODES,
            MAX_MACHINE_MEMORY_PERCENT,
            AutodetectBuilder.MAX_ANOMALY_RECORDS_SETTING_DYNAMIC,
            MAX_OPEN_JOBS_PER_NODE,
            MIN_DISK_SPACE_OFF_HEAP,
            MlConfigMigrationEligibilityCheck.ENABLE_CONFIG_MIGRATION,
            InferenceProcessor.MAX_INFERENCE_PROCESSORS,
            ModelLoadingService.INFERENCE_MODEL_CACHE_SIZE,
            ModelLoadingService.INFERENCE_MODEL_CACHE_TTL,
            ResultsPersisterService.PERSIST_RESULTS_MAX_RETRIES,
            NIGHTLY_MAINTENANCE_REQUESTS_PER_SECOND,
            USE_AUTO_MACHINE_MEMORY_PERCENT,
            MAX_ML_NODE_SIZE,
            DELAYED_DATA_CHECK_FREQ
        );
    }

    @Override
    public Settings additionalSettings() {
        String maxOpenJobsPerNodeNodeAttrName = "node.attr." + PRE_V8_MAX_OPEN_JOBS_NODE_ATTR;
        String machineMemoryAttrName = "node.attr." + MACHINE_MEMORY_NODE_ATTR;
        String jvmSizeAttrName = "node.attr." + MAX_JVM_SIZE_NODE_ATTR;
        String allocatedProcessorsAttrName = "node.attr." + ALLOCATED_PROCESSORS_NODE_ATTR;

        if (enabled == false) {
            disallowMlNodeAttributes(maxOpenJobsPerNodeNodeAttrName, machineMemoryAttrName, jvmSizeAttrName);
            return Settings.EMPTY;
        }

        Settings.Builder additionalSettings = Settings.builder();
        if (DiscoveryNode.hasRole(settings, DiscoveryNodeRole.ML_ROLE)) {
            addMlNodeAttribute(
                additionalSettings,
                machineMemoryAttrName,
                Long.toString(OsProbe.getInstance().osStats().getMem().getAdjustedTotal().getBytes())
            );
            addMlNodeAttribute(additionalSettings, jvmSizeAttrName, Long.toString(Runtime.getRuntime().maxMemory()));
            addMlNodeAttribute(additionalSettings, allocatedProcessorsAttrName, Double.toString(getAllocatedProcessors().count()));
            // This is not used in v8 and higher, but users are still prevented from setting it directly to avoid confusion
            disallowMlNodeAttributes(maxOpenJobsPerNodeNodeAttrName);
        } else {
            disallowMlNodeAttributes(maxOpenJobsPerNodeNodeAttrName, machineMemoryAttrName, jvmSizeAttrName, allocatedProcessorsAttrName);
        }
        return additionalSettings.build();
    }

    private void addMlNodeAttribute(Settings.Builder additionalSettings, String attrName, String value) {
        String oldValue = settings.get(attrName);
        if (oldValue == null) {
            additionalSettings.put(attrName, value);
        } else {
            reportClashingNodeAttribute(attrName);
        }
    }

    private Processors getAllocatedProcessors() {
        return EsExecutors.nodeProcessors(settings);
    }

    private void disallowMlNodeAttributes(String... mlNodeAttributes) {
        for (String attrName : mlNodeAttributes) {
            if (settings.get(attrName) != null) {
                reportClashingNodeAttribute(attrName);
            }
        }
    }

    private void reportClashingNodeAttribute(String attrName) {
        throw new IllegalArgumentException(
            "Directly setting ["
                + attrName
                + "] is not permitted - "
                + "it is reserved for machine learning. If your intention was to customize machine learning, set the ["
                + attrName.replace("node.attr.", "xpack.")
                + "] setting instead."
        );
    }

    // overridable by tests
    protected Clock getClock() {
        return Clock.systemUTC();
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Tracer tracer,
        AllocationDeciders allocationDeciders
    ) {
        if (enabled == false) {
            // special holder for @link(MachineLearningFeatureSetUsage) which needs access to job manager, empty if ML is disabled
            return singletonList(new JobManagerHolder());
        }

        this.mlUpgradeModeActionFilter.set(new MlUpgradeModeActionFilter(clusterService));

        MlIndexTemplateRegistry registry = new MlIndexTemplateRegistry(settings, clusterService, threadPool, client, xContentRegistry);
        registry.initialize();

        AnomalyDetectionAuditor anomalyDetectionAuditor = new AnomalyDetectionAuditor(client, clusterService);
        DataFrameAnalyticsAuditor dataFrameAnalyticsAuditor = new DataFrameAnalyticsAuditor(client, clusterService);
        InferenceAuditor inferenceAuditor = new InferenceAuditor(client, clusterService);
        this.dataFrameAnalyticsAuditor.set(dataFrameAnalyticsAuditor);
        OriginSettingClient originSettingClient = new OriginSettingClient(client, ML_ORIGIN);
        ResultsPersisterService resultsPersisterService = new ResultsPersisterService(
            threadPool,
            originSettingClient,
            clusterService,
            settings
        );
        AnnotationPersister anomalyDetectionAnnotationPersister = new AnnotationPersister(resultsPersisterService);
        JobResultsProvider jobResultsProvider = new JobResultsProvider(client, settings, indexNameExpressionResolver);
        JobResultsPersister jobResultsPersister = new JobResultsPersister(originSettingClient, resultsPersisterService);
        JobDataCountsPersister jobDataCountsPersister = new JobDataCountsPersister(
            client,
            resultsPersisterService,
            anomalyDetectionAuditor
        );
        JobConfigProvider jobConfigProvider = new JobConfigProvider(client, xContentRegistry);
        DatafeedConfigProvider datafeedConfigProvider = new DatafeedConfigProvider(client, xContentRegistry, clusterService);
        this.datafeedConfigProvider.set(datafeedConfigProvider);
        UpdateJobProcessNotifier notifier = new UpdateJobProcessNotifier(client, clusterService, threadPool);
        JobManager jobManager = new JobManager(
            jobResultsProvider,
            jobResultsPersister,
            clusterService,
            anomalyDetectionAuditor,
            threadPool,
            client,
            notifier,
            xContentRegistry,
            indexNameExpressionResolver,
            () -> NativeMemoryCalculator.getMaxModelMemoryLimit(clusterService)
        );
        DatafeedManager datafeedManager = new DatafeedManager(
            datafeedConfigProvider,
            jobConfigProvider,
            xContentRegistry,
            settings,
            client
        );

        // special holder for @link(MachineLearningFeatureSetUsage) which needs access to job manager if ML is enabled
        JobManagerHolder jobManagerHolder = new JobManagerHolder(jobManager);

        NativeStorageProvider nativeStorageProvider = new NativeStorageProvider(environment, MIN_DISK_SPACE_OFF_HEAP.get(settings));

        final MlController mlController;
        final AutodetectProcessFactory autodetectProcessFactory;
        final NormalizerProcessFactory normalizerProcessFactory;
        final AnalyticsProcessFactory<AnalyticsResult> analyticsProcessFactory;
        final AnalyticsProcessFactory<MemoryUsageEstimationResult> memoryEstimationProcessFactory;
        final PyTorchProcessFactory pyTorchProcessFactory;
        if (MachineLearningField.AUTODETECT_PROCESS.get(settings)) {
            try {
                NativeController nativeController = NativeController.makeNativeController(
                    clusterService.getNodeName(),
                    environment,
                    xContentRegistry
                );
                autodetectProcessFactory = new NativeAutodetectProcessFactory(
                    environment,
                    settings,
                    nativeController,
                    clusterService,
                    resultsPersisterService,
                    anomalyDetectionAuditor
                );
                normalizerProcessFactory = new NativeNormalizerProcessFactory(environment, nativeController, clusterService);
                analyticsProcessFactory = new NativeAnalyticsProcessFactory(
                    environment,
                    nativeController,
                    clusterService,
                    xContentRegistry,
                    resultsPersisterService,
                    dataFrameAnalyticsAuditor
                );
                memoryEstimationProcessFactory = new NativeMemoryUsageEstimationProcessFactory(
                    environment,
                    nativeController,
                    clusterService
                );
                pyTorchProcessFactory = new NativePyTorchProcessFactory(environment, nativeController, clusterService);
                mlController = nativeController;
            } catch (IOException e) {
                // The low level cause of failure from the named pipe helper's perspective is almost never the real root cause, so
                // only log this at the lowest level of detail. It's almost always "file not found" on a named pipe we expect to be
                // able to connect to, but the thing we really need to know is what stopped the native process creating the named pipe.
                logger.trace("Failed to connect to ML native controller", e);
                throw new ElasticsearchException(
                    "Failure running machine learning native code. This could be due to running "
                        + "on an unsupported OS or distribution, missing OS libraries, or a problem with the temp directory. To "
                        + "bypass this problem by running Elasticsearch without machine learning functionality set ["
                        + XPackSettings.MACHINE_LEARNING_ENABLED.getKey()
                        + ": false]."
                );
            }
        } else {
            mlController = new DummyController();
            autodetectProcessFactory = (
                pipelineId,
                job,
                autodetectParams,
                executorService,
                onProcessCrash) -> new BlackHoleAutodetectProcess(pipelineId, onProcessCrash);
            // factor of 1.0 makes renormalization a no-op
            normalizerProcessFactory = (jobId, quantilesState, bucketSpan, executorService) -> new MultiplyingNormalizerProcess(1.0);
            analyticsProcessFactory = (jobId, analyticsProcessConfig, hasState, executorService, onProcessCrash) -> null;
            memoryEstimationProcessFactory = (jobId, analyticsProcessConfig, hasState, executorService, onProcessCrash) -> null;
            pyTorchProcessFactory = (task, executorService, onProcessCrash) -> new BlackHolePyTorchProcess();
        }
        NormalizerFactory normalizerFactory = new NormalizerFactory(
            normalizerProcessFactory,
            threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)
        );
        AutodetectProcessManager autodetectProcessManager = new AutodetectProcessManager(
            settings,
            client,
            threadPool,
            xContentRegistry,
            anomalyDetectionAuditor,
            clusterService,
            jobManager,
            jobResultsProvider,
            jobResultsPersister,
            jobDataCountsPersister,
            anomalyDetectionAnnotationPersister,
            autodetectProcessFactory,
            normalizerFactory,
            nativeStorageProvider,
            indexNameExpressionResolver
        );
        this.autodetectProcessManager.set(autodetectProcessManager);
        DatafeedJobBuilder datafeedJobBuilder = new DatafeedJobBuilder(
            client,
            xContentRegistry,
            anomalyDetectionAuditor,
            anomalyDetectionAnnotationPersister,
            System::currentTimeMillis,
            jobResultsPersister,
            settings,
            clusterService
        );
        DatafeedContextProvider datafeedContextProvider = new DatafeedContextProvider(
            jobConfigProvider,
            datafeedConfigProvider,
            jobResultsProvider
        );
        DatafeedRunner datafeedRunner = new DatafeedRunner(
            threadPool,
            client,
            clusterService,
            datafeedJobBuilder,
            System::currentTimeMillis,
            anomalyDetectionAuditor,
            autodetectProcessManager,
            datafeedContextProvider
        );
        this.datafeedRunner.set(datafeedRunner);

        // Inference components
        final TrainedModelStatsService trainedModelStatsService = new TrainedModelStatsService(
            resultsPersisterService,
            originSettingClient,
            indexNameExpressionResolver,
            clusterService,
            threadPool
        );
        final TrainedModelProvider trainedModelProvider = new TrainedModelProvider(client, xContentRegistry);
        final ModelLoadingService modelLoadingService = new ModelLoadingService(
            trainedModelProvider,
            inferenceAuditor,
            threadPool,
            clusterService,
            trainedModelStatsService,
            settings,
            clusterService.getNodeName(),
            inferenceModelBreaker.get(),
            getLicenseState()
        );
        this.modelLoadingService.set(modelLoadingService);
        this.deploymentManager.set(new DeploymentManager(client, xContentRegistry, threadPool, pyTorchProcessFactory));

        // Data frame analytics components
        AnalyticsProcessManager analyticsProcessManager = new AnalyticsProcessManager(
            settings,
            client,
            threadPool,
            analyticsProcessFactory,
            dataFrameAnalyticsAuditor,
            trainedModelProvider,
            resultsPersisterService,
            EsExecutors.allocatedProcessors(settings)
        );
        MemoryUsageEstimationProcessManager memoryEstimationProcessManager = new MemoryUsageEstimationProcessManager(
            threadPool.generic(),
            threadPool.executor(UTILITY_THREAD_POOL_NAME),
            memoryEstimationProcessFactory
        );
        DataFrameAnalyticsConfigProvider dataFrameAnalyticsConfigProvider = new DataFrameAnalyticsConfigProvider(
            client,
            xContentRegistry,
            dataFrameAnalyticsAuditor,
            clusterService
        );
        assert client instanceof NodeClient;
        DataFrameAnalyticsManager dataFrameAnalyticsManager = new DataFrameAnalyticsManager(
            settings,
            (NodeClient) client,
            threadPool,
            clusterService,
            dataFrameAnalyticsConfigProvider,
            analyticsProcessManager,
            dataFrameAnalyticsAuditor,
            indexNameExpressionResolver,
            resultsPersisterService,
            modelLoadingService
        );
        this.dataFrameAnalyticsManager.set(dataFrameAnalyticsManager);

        // Components shared by anomaly detection and data frame analytics
        MlMemoryTracker memoryTracker = new MlMemoryTracker(
            settings,
            clusterService,
            threadPool,
            jobManager,
            jobResultsProvider,
            dataFrameAnalyticsConfigProvider
        );
        this.memoryTracker.set(memoryTracker);
        MlLifeCycleService mlLifeCycleService = new MlLifeCycleService(
            clusterService,
            datafeedRunner,
            mlController,
            autodetectProcessManager,
            dataFrameAnalyticsManager,
            memoryTracker
        );
        this.mlLifeCycleService.set(mlLifeCycleService);
        MlAssignmentNotifier mlAssignmentNotifier = new MlAssignmentNotifier(
            anomalyDetectionAuditor,
            dataFrameAnalyticsAuditor,
            threadPool,
            clusterService
        );

        MlAutoUpdateService mlAutoUpdateService = new MlAutoUpdateService(
            threadPool,
            List.of(new DatafeedConfigAutoUpdater(datafeedConfigProvider, indexNameExpressionResolver))
        );
        clusterService.addListener(mlAutoUpdateService);
        // this object registers as a license state listener, and is never removed, so there's no need to retain another reference to it
        final InvalidLicenseEnforcer enforcer = new InvalidLicenseEnforcer(
            getLicenseState(),
            threadPool,
            datafeedRunner,
            autodetectProcessManager
        );
        enforcer.listenForLicenseStateChanges();

        // Perform node startup operations
        nativeStorageProvider.cleanupLocalTmpStorageInCaseOfUncleanShutdown();

        NodeAvailabilityZoneMapper nodeAvailabilityZoneMapper = new NodeAvailabilityZoneMapper(
            settings,
            clusterService.getClusterSettings()
        );
        clusterService.addListener(nodeAvailabilityZoneMapper);

        // allocation service objects
        final TrainedModelAssignmentService trainedModelAssignmentService = new TrainedModelAssignmentService(
            client,
            clusterService,
            threadPool
        );
        trainedModelAllocationClusterServiceSetOnce.set(
            new TrainedModelAssignmentClusterService(
                settings,
                clusterService,
                threadPool,
                new NodeLoadDetector(memoryTracker),
                new SystemAuditor(client, clusterService),
                nodeAvailabilityZoneMapper
            )
        );

        mlAutoscalingDeciderService.set(
            new MlAutoscalingDeciderService(memoryTracker, settings, nodeAvailabilityZoneMapper, clusterService)
        );

        return Arrays.asList(
            mlLifeCycleService,
            new MlControllerHolder(mlController),
            jobResultsProvider,
            jobResultsPersister,
            jobConfigProvider,
            datafeedConfigProvider,
            jobManager,
            jobManagerHolder,
            autodetectProcessManager,
            new MlInitializationService(settings, threadPool, clusterService, client, mlAssignmentNotifier),
            jobDataCountsPersister,
            datafeedRunner,
            datafeedManager,
            anomalyDetectionAuditor,
            dataFrameAnalyticsAuditor,
            inferenceAuditor,
            mlAssignmentNotifier,
            mlAutoUpdateService,
            memoryTracker,
            analyticsProcessManager,
            memoryEstimationProcessManager,
            dataFrameAnalyticsConfigProvider,
            nativeStorageProvider,
            modelLoadingService,
            trainedModelProvider,
            trainedModelAssignmentService,
            trainedModelAllocationClusterServiceSetOnce.get(),
            deploymentManager.get(),
            nodeAvailabilityZoneMapper
        );
    }

    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        SettingsModule settingsModule,
        IndexNameExpressionResolver expressionResolver
    ) {
        if (enabled == false) {
            return emptyList();
        }

        return Arrays.asList(
            new OpenJobPersistentTasksExecutor(
                settings,
                clusterService,
                autodetectProcessManager.get(),
                datafeedConfigProvider.get(),
                memoryTracker.get(),
                client,
                expressionResolver,
                getLicenseState()
            ),
            new TransportStartDatafeedAction.StartDatafeedPersistentTasksExecutor(datafeedRunner.get(), expressionResolver),
            new TransportStartDataFrameAnalyticsAction.TaskExecutor(
                settings,
                client,
                clusterService,
                dataFrameAnalyticsManager.get(),
                dataFrameAnalyticsAuditor.get(),
                memoryTracker.get(),
                expressionResolver,
                getLicenseState()
            ),
            new SnapshotUpgradeTaskExecutor(
                settings,
                clusterService,
                autodetectProcessManager.get(),
                memoryTracker.get(),
                expressionResolver,
                client,
                getLicenseState()
            )
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings unused,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        if (false == enabled) {
            return emptyList();
        }
        return Arrays.asList(
            new RestGetJobsAction(),
            new RestGetJobStatsAction(),
            new RestMlInfoAction(),
            new RestMlMemoryAction(),
            new RestPutJobAction(),
            new RestPostJobUpdateAction(),
            new RestDeleteJobAction(),
            new RestOpenJobAction(),
            new RestGetFiltersAction(),
            new RestPutFilterAction(),
            new RestUpdateFilterAction(),
            new RestDeleteFilterAction(),
            new RestGetInfluencersAction(),
            new RestGetRecordsAction(),
            new RestGetBucketsAction(),
            new RestGetOverallBucketsAction(),
            new RestPostDataAction(),
            new RestCloseJobAction(),
            new RestFlushJobAction(),
            new RestResetJobAction(),
            new RestValidateDetectorAction(),
            new RestValidateJobConfigAction(),
            new RestEstimateModelMemoryAction(),
            new RestGetCategoriesAction(),
            new RestGetModelSnapshotsAction(),
            new RestRevertModelSnapshotAction(),
            new RestUpdateModelSnapshotAction(),
            new RestGetDatafeedsAction(),
            new RestGetDatafeedStatsAction(),
            new RestPutDatafeedAction(),
            new RestUpdateDatafeedAction(),
            new RestDeleteDatafeedAction(),
            new RestPreviewDatafeedAction(),
            new RestStartDatafeedAction(),
            new RestStopDatafeedAction(),
            new RestDeleteModelSnapshotAction(),
            new RestDeleteExpiredDataAction(),
            new RestForecastJobAction(),
            new RestDeleteForecastAction(),
            new RestGetCalendarsAction(),
            new RestPutCalendarAction(),
            new RestDeleteCalendarAction(),
            new RestDeleteCalendarEventAction(),
            new RestDeleteCalendarJobAction(),
            new RestPutCalendarJobAction(),
            new RestGetCalendarEventsAction(),
            new RestPostCalendarEventAction(),
            new RestSetUpgradeModeAction(),
            new RestGetDataFrameAnalyticsAction(),
            new RestGetDataFrameAnalyticsStatsAction(),
            new RestPutDataFrameAnalyticsAction(),
            new RestPostDataFrameAnalyticsUpdateAction(),
            new RestDeleteDataFrameAnalyticsAction(),
            new RestStartDataFrameAnalyticsAction(),
            new RestStopDataFrameAnalyticsAction(),
            new RestEvaluateDataFrameAction(),
            new RestExplainDataFrameAnalyticsAction(),
            new RestGetTrainedModelsAction(),
            new RestDeleteTrainedModelAction(),
            new RestGetTrainedModelsStatsAction(),
            new RestPutTrainedModelAction(),
            new RestUpgradeJobModelSnapshotAction(),
            new RestGetJobModelSnapshotsUpgradeStatsAction(),
            new RestPutTrainedModelAliasAction(),
            new RestDeleteTrainedModelAliasAction(),
            new RestPreviewDataFrameAnalyticsAction(),
            new RestStartTrainedModelDeploymentAction(),
            new RestStopTrainedModelDeploymentAction(),
            new RestInferTrainedModelDeploymentAction(),
            new RestPutTrainedModelDefinitionPartAction(),
            new RestPutTrainedModelVocabularyAction(),
            new RestInferTrainedModelAction(),
            new RestClearDeploymentCacheAction(),
            // CAT Handlers
            new RestCatJobsAction(),
            new RestCatTrainedModelsAction(),
            new RestCatDatafeedsAction(),
            new RestCatDataFrameAnalyticsAction()
        );
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        var usageAction = new ActionHandler<>(XPackUsageFeatureAction.MACHINE_LEARNING, MachineLearningUsageTransportAction.class);
        var infoAction = new ActionHandler<>(XPackInfoFeatureAction.MACHINE_LEARNING, MachineLearningInfoTransportAction.class);
        if (false == enabled) {
            return List.of(usageAction, infoAction);
        }
        return List.of(
            new ActionHandler<>(GetJobsAction.INSTANCE, TransportGetJobsAction.class),
            new ActionHandler<>(GetJobsStatsAction.INSTANCE, TransportGetJobsStatsAction.class),
            new ActionHandler<>(MlInfoAction.INSTANCE, TransportMlInfoAction.class),
            new ActionHandler<>(MlMemoryAction.INSTANCE, TransportMlMemoryAction.class),
            new ActionHandler<>(PutJobAction.INSTANCE, TransportPutJobAction.class),
            new ActionHandler<>(UpdateJobAction.INSTANCE, TransportUpdateJobAction.class),
            new ActionHandler<>(DeleteJobAction.INSTANCE, TransportDeleteJobAction.class),
            new ActionHandler<>(OpenJobAction.INSTANCE, TransportOpenJobAction.class),
            new ActionHandler<>(GetFiltersAction.INSTANCE, TransportGetFiltersAction.class),
            new ActionHandler<>(PutFilterAction.INSTANCE, TransportPutFilterAction.class),
            new ActionHandler<>(UpdateFilterAction.INSTANCE, TransportUpdateFilterAction.class),
            new ActionHandler<>(DeleteFilterAction.INSTANCE, TransportDeleteFilterAction.class),
            new ActionHandler<>(KillProcessAction.INSTANCE, TransportKillProcessAction.class),
            new ActionHandler<>(GetBucketsAction.INSTANCE, TransportGetBucketsAction.class),
            new ActionHandler<>(GetInfluencersAction.INSTANCE, TransportGetInfluencersAction.class),
            new ActionHandler<>(GetOverallBucketsAction.INSTANCE, TransportGetOverallBucketsAction.class),
            new ActionHandler<>(GetRecordsAction.INSTANCE, TransportGetRecordsAction.class),
            new ActionHandler<>(PostDataAction.INSTANCE, TransportPostDataAction.class),
            new ActionHandler<>(CloseJobAction.INSTANCE, TransportCloseJobAction.class),
            new ActionHandler<>(FinalizeJobExecutionAction.INSTANCE, TransportFinalizeJobExecutionAction.class),
            new ActionHandler<>(FlushJobAction.INSTANCE, TransportFlushJobAction.class),
            new ActionHandler<>(ResetJobAction.INSTANCE, TransportResetJobAction.class),
            new ActionHandler<>(ValidateDetectorAction.INSTANCE, TransportValidateDetectorAction.class),
            new ActionHandler<>(ValidateJobConfigAction.INSTANCE, TransportValidateJobConfigAction.class),
            new ActionHandler<>(EstimateModelMemoryAction.INSTANCE, TransportEstimateModelMemoryAction.class),
            new ActionHandler<>(GetCategoriesAction.INSTANCE, TransportGetCategoriesAction.class),
            new ActionHandler<>(GetModelSnapshotsAction.INSTANCE, TransportGetModelSnapshotsAction.class),
            new ActionHandler<>(RevertModelSnapshotAction.INSTANCE, TransportRevertModelSnapshotAction.class),
            new ActionHandler<>(UpdateModelSnapshotAction.INSTANCE, TransportUpdateModelSnapshotAction.class),
            new ActionHandler<>(GetDatafeedsAction.INSTANCE, TransportGetDatafeedsAction.class),
            new ActionHandler<>(GetDatafeedsStatsAction.INSTANCE, TransportGetDatafeedsStatsAction.class),
            new ActionHandler<>(PutDatafeedAction.INSTANCE, TransportPutDatafeedAction.class),
            new ActionHandler<>(UpdateDatafeedAction.INSTANCE, TransportUpdateDatafeedAction.class),
            new ActionHandler<>(DeleteDatafeedAction.INSTANCE, TransportDeleteDatafeedAction.class),
            new ActionHandler<>(PreviewDatafeedAction.INSTANCE, TransportPreviewDatafeedAction.class),
            new ActionHandler<>(StartDatafeedAction.INSTANCE, TransportStartDatafeedAction.class),
            new ActionHandler<>(StopDatafeedAction.INSTANCE, TransportStopDatafeedAction.class),
            new ActionHandler<>(IsolateDatafeedAction.INSTANCE, TransportIsolateDatafeedAction.class),
            new ActionHandler<>(DeleteModelSnapshotAction.INSTANCE, TransportDeleteModelSnapshotAction.class),
            new ActionHandler<>(UpdateProcessAction.INSTANCE, TransportUpdateProcessAction.class),
            new ActionHandler<>(DeleteExpiredDataAction.INSTANCE, TransportDeleteExpiredDataAction.class),
            new ActionHandler<>(ForecastJobAction.INSTANCE, TransportForecastJobAction.class),
            new ActionHandler<>(DeleteForecastAction.INSTANCE, TransportDeleteForecastAction.class),
            new ActionHandler<>(GetCalendarsAction.INSTANCE, TransportGetCalendarsAction.class),
            new ActionHandler<>(PutCalendarAction.INSTANCE, TransportPutCalendarAction.class),
            new ActionHandler<>(DeleteCalendarAction.INSTANCE, TransportDeleteCalendarAction.class),
            new ActionHandler<>(DeleteCalendarEventAction.INSTANCE, TransportDeleteCalendarEventAction.class),
            new ActionHandler<>(UpdateCalendarJobAction.INSTANCE, TransportUpdateCalendarJobAction.class),
            new ActionHandler<>(GetCalendarEventsAction.INSTANCE, TransportGetCalendarEventsAction.class),
            new ActionHandler<>(PostCalendarEventsAction.INSTANCE, TransportPostCalendarEventsAction.class),
            new ActionHandler<>(PersistJobAction.INSTANCE, TransportPersistJobAction.class),
            new ActionHandler<>(SetUpgradeModeAction.INSTANCE, TransportSetUpgradeModeAction.class),
            new ActionHandler<>(GetDataFrameAnalyticsAction.INSTANCE, TransportGetDataFrameAnalyticsAction.class),
            new ActionHandler<>(GetDataFrameAnalyticsStatsAction.INSTANCE, TransportGetDataFrameAnalyticsStatsAction.class),
            new ActionHandler<>(PutDataFrameAnalyticsAction.INSTANCE, TransportPutDataFrameAnalyticsAction.class),
            new ActionHandler<>(UpdateDataFrameAnalyticsAction.INSTANCE, TransportUpdateDataFrameAnalyticsAction.class),
            new ActionHandler<>(DeleteDataFrameAnalyticsAction.INSTANCE, TransportDeleteDataFrameAnalyticsAction.class),
            new ActionHandler<>(StartDataFrameAnalyticsAction.INSTANCE, TransportStartDataFrameAnalyticsAction.class),
            new ActionHandler<>(StopDataFrameAnalyticsAction.INSTANCE, TransportStopDataFrameAnalyticsAction.class),
            new ActionHandler<>(EvaluateDataFrameAction.INSTANCE, TransportEvaluateDataFrameAction.class),
            new ActionHandler<>(ExplainDataFrameAnalyticsAction.INSTANCE, TransportExplainDataFrameAnalyticsAction.class),
            new ActionHandler<>(InferModelAction.INSTANCE, TransportInternalInferModelAction.class),
            new ActionHandler<>(InferModelAction.EXTERNAL_INSTANCE, TransportExternalInferModelAction.class),
            new ActionHandler<>(TrainedModelCacheInfoAction.INSTANCE, TransportTrainedModelCacheInfoAction.class),
            new ActionHandler<>(GetTrainedModelsAction.INSTANCE, TransportGetTrainedModelsAction.class),
            new ActionHandler<>(DeleteTrainedModelAction.INSTANCE, TransportDeleteTrainedModelAction.class),
            new ActionHandler<>(GetTrainedModelsStatsAction.INSTANCE, TransportGetTrainedModelsStatsAction.class),
            new ActionHandler<>(PutTrainedModelAction.INSTANCE, TransportPutTrainedModelAction.class),
            new ActionHandler<>(UpgradeJobModelSnapshotAction.INSTANCE, TransportUpgradeJobModelSnapshotAction.class),
            new ActionHandler<>(CancelJobModelSnapshotUpgradeAction.INSTANCE, TransportCancelJobModelSnapshotUpgradeAction.class),
            new ActionHandler<>(GetJobModelSnapshotsUpgradeStatsAction.INSTANCE, TransportGetJobModelSnapshotsUpgradeStatsAction.class),
            new ActionHandler<>(PutTrainedModelAliasAction.INSTANCE, TransportPutTrainedModelAliasAction.class),
            new ActionHandler<>(DeleteTrainedModelAliasAction.INSTANCE, TransportDeleteTrainedModelAliasAction.class),
            new ActionHandler<>(PreviewDataFrameAnalyticsAction.INSTANCE, TransportPreviewDataFrameAnalyticsAction.class),
            new ActionHandler<>(SetResetModeAction.INSTANCE, TransportSetResetModeAction.class),
            new ActionHandler<>(StartTrainedModelDeploymentAction.INSTANCE, TransportStartTrainedModelDeploymentAction.class),
            new ActionHandler<>(StopTrainedModelDeploymentAction.INSTANCE, TransportStopTrainedModelDeploymentAction.class),
            new ActionHandler<>(InferTrainedModelDeploymentAction.INSTANCE, TransportInferTrainedModelDeploymentAction.class),
            new ActionHandler<>(GetDeploymentStatsAction.INSTANCE, TransportGetDeploymentStatsAction.class),
            new ActionHandler<>(GetDatafeedRunningStateAction.INSTANCE, TransportGetDatafeedRunningStateAction.class),
            new ActionHandler<>(CreateTrainedModelAssignmentAction.INSTANCE, TransportCreateTrainedModelAssignmentAction.class),
            new ActionHandler<>(DeleteTrainedModelAssignmentAction.INSTANCE, TransportDeleteTrainedModelAssignmentAction.class),
            new ActionHandler<>(PutTrainedModelDefinitionPartAction.INSTANCE, TransportPutTrainedModelDefinitionPartAction.class),
            new ActionHandler<>(PutTrainedModelVocabularyAction.INSTANCE, TransportPutTrainedModelVocabularyAction.class),
            new ActionHandler<>(
                UpdateTrainedModelAssignmentRoutingInfoAction.INSTANCE,
                TransportUpdateTrainedModelAssignmentStateAction.class
            ),
            new ActionHandler<>(ClearDeploymentCacheAction.INSTANCE, TransportClearDeploymentCacheAction.class),
            usageAction,
            infoAction
        );
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        if (enabled == false) {
            return emptyList();
        }

        return singletonList(this.mlUpgradeModeActionFilter.get());
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings unused) {
        if (false == enabled) {
            return emptyList();
        }

        // These thread pools scale such that they can accommodate the maximum number of jobs per node
        // that is permitted to be configured. It is up to other code to enforce the configured maximum
        // number of jobs per node.

        // 4 threads per job process: for input, c++ logger output, result processing and state processing.
        // Only use this thread pool for the main long-running process associated with an anomaly detection
        // job or a data frame analytics job. (Using it for some other purpose could mean that an unrelated
        // job fails to start or that whatever needed the thread for another purpose has to queue for a very
        // long time.)
        ScalingExecutorBuilder jobComms = new ScalingExecutorBuilder(
            JOB_COMMS_THREAD_POOL_NAME,
            4,
            MAX_MAX_OPEN_JOBS_PER_NODE * 4,
            TimeValue.timeValueMinutes(1),
            false,
            "xpack.ml.job_comms_thread_pool"
        );

        // 3 threads per native inference process: for input, c++ logger output, and result processing.
        // As we cannot assign more models than the number of allocated processors, this thread pool's
        // size is limited by the number of allocated processors on this node.
        // Only use this thread pool for the main long-running process associated with a native inference model deployment.
        // (Using it for some other purpose could mean that an unrelated pytorch model assignment fails to start
        // or that whatever needed the thread for another purpose has to queue for a very long time.)
        ScalingExecutorBuilder pytorchComms = new ScalingExecutorBuilder(
            NATIVE_INFERENCE_COMMS_THREAD_POOL_NAME,
            3,
            getAllocatedProcessors().roundUp() * 3,
            TimeValue.timeValueMinutes(1),
            false,
            "xpack.ml.native_inference_comms_thread_pool"
        );

        // This pool is used by renormalization, data frame analytics memory estimation, plus some other parts
        // of ML that need to kick off non-trivial activities that mustn't block other threads.
        ScalingExecutorBuilder utility = new ScalingExecutorBuilder(
            UTILITY_THREAD_POOL_NAME,
            1,
            MAX_MAX_OPEN_JOBS_PER_NODE * 4,
            TimeValue.timeValueMinutes(10),
            false,
            "xpack.ml.utility_thread_pool"
        );

        ScalingExecutorBuilder datafeed = new ScalingExecutorBuilder(
            DATAFEED_THREAD_POOL_NAME,
            1,
            MAX_MAX_OPEN_JOBS_PER_NODE,
            TimeValue.timeValueMinutes(1),
            false,
            "xpack.ml.datafeed_thread_pool"
        );

        return List.of(jobComms, pytorchComms, utility, datafeed);
    }

    @Override
    public Map<String, AnalysisProvider<CharFilterFactory>> getCharFilters() {
        return MapBuilder.<String, AnalysisProvider<CharFilterFactory>>newMapBuilder()
            .put(FirstNonBlankLineCharFilter.NAME, FirstNonBlankLineCharFilterFactory::new)
            .put(FirstLineWithLettersCharFilter.NAME, FirstLineWithLettersCharFilterFactory::new)
            .map();
    }

    @Override
    public Map<String, AnalysisProvider<TokenizerFactory>> getTokenizers() {
        return Map.of(MlClassicTokenizer.NAME, MlClassicTokenizerFactory::new, MlStandardTokenizer.NAME, MlStandardTokenizerFactory::new);
    }

    @Override
    public List<PipelineAggregationSpec> getPipelineAggregations() {
        return List.of(
            InferencePipelineAggregationBuilder.buildSpec(modelLoadingService, getLicenseState(), settings),
            new SearchPlugin.PipelineAggregationSpec(
                BucketCorrelationAggregationBuilder.NAME,
                BucketCorrelationAggregationBuilder::new,
                checkAggLicense(BucketCorrelationAggregationBuilder.PARSER, BUCKET_CORRELATION_AGG_FEATURE)
            ),
            new SearchPlugin.PipelineAggregationSpec(
                BucketCountKSTestAggregationBuilder.NAME,
                BucketCountKSTestAggregationBuilder::new,
                checkAggLicense(BucketCountKSTestAggregationBuilder.PARSER, BUCKET_COUNT_KS_TEST_AGG_FEATURE)
            ).addResultReader(InternalKSTestAggregation::new),
            new SearchPlugin.PipelineAggregationSpec(
                ChangePointAggregationBuilder.NAME,
                ChangePointAggregationBuilder::new,
                checkAggLicense(ChangePointAggregationBuilder.PARSER, CHANGE_POINT_AGG_FEATURE)
            ).addResultReader(InternalChangePointAggregation::new)
        );
    }

    @Override
    public List<SignificanceHeuristicSpec<?>> getSignificanceHeuristics() {
        return List.of(new SignificanceHeuristicSpec<>(PValueScore.NAME, PValueScore::new, PValueScore.PARSER));
    }

    private <T> ContextParser<String, T> checkAggLicense(ContextParser<String, T> realParser, LicensedFeature.Momentary feature) {
        return (parser, name) -> {
            if (feature.check(getLicenseState()) == false) {
                throw LicenseUtils.newComplianceException(feature.getName());
            }
            return realParser.parse(parser, name);
        };
    }

    @Override
    public List<AggregationSpec> getAggregations() {
        return List.of(
            new AggregationSpec(
                CategorizeTextAggregationBuilder.NAME,
                CategorizeTextAggregationBuilder::new,
                checkAggLicense(CategorizeTextAggregationBuilder.PARSER, CATEGORIZE_TEXT_AGG_FEATURE)
            ).addResultReader(InternalCategorizationAggregation::new)
                .setAggregatorRegistrar(s -> s.registerUsage(CategorizeTextAggregationBuilder.NAME)),
            new AggregationSpec(
                FrequentItemSetsAggregationBuilder.NAME,
                FrequentItemSetsAggregationBuilder::new,
                checkAggLicense(FrequentItemSetsAggregationBuilder.PARSER, FREQUENT_ITEM_SETS_AGG_FEATURE)
            ).addResultReader(FrequentItemSetsAggregatorFactory.getResultReader())
                .setAggregatorRegistrar(FrequentItemSetsAggregationBuilder::registerAggregators)
        );
    }

    public static boolean criticalTemplatesInstalled(ClusterState clusterState) {
        boolean allPresent = true;
        // The templates for the notifications and stats indices are not critical up-front because
        // every notification and stats update checks if the appropriate template is installed and
        // installs it if necessary
        List<String> templateNames = List.of(STATE_INDEX_PREFIX, AnomalyDetectorsIndex.jobResultsIndexPrefix());
        for (String templateName : templateNames) {
            allPresent = allPresent && TemplateUtils.checkTemplateExistsAndVersionIsGTECurrentVersion(templateName, clusterState);
        }

        return allPresent;
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlEvaluationNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new MlDataFrameAnalysisNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new MlModelSizeNamedXContentProvider().getNamedXContentParsers());
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField(ModelAliasMetadata.NAME),
                ModelAliasMetadata::fromXContent
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField(TrainedModelAssignmentMetadata.NAME),
                TrainedModelAssignmentMetadata::fromXContent
            )
        );
        // This is so old cluster state can be read on a new node. We are not making it a deprecated parse field as the user
        // has no control over this. So, simply read it without logging a deprecation warning
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField(TrainedModelAssignmentMetadata.DEPRECATED_NAME),
                TrainedModelAssignmentMetadata::fromXContent
            )
        );
        namedXContent.addAll(new CorrelationNamedContentProvider().getNamedXContentParsers());
        return namedXContent;
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();

        // Custom metadata
        namedWriteables.add(new NamedWriteableRegistry.Entry(Metadata.Custom.class, "ml", MlMetadata::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(NamedDiff.class, "ml", MlMetadata.MlMetadataDiff::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(Metadata.Custom.class, ModelAliasMetadata.NAME, ModelAliasMetadata::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(NamedDiff.class, ModelAliasMetadata.NAME, ModelAliasMetadata::readDiffFrom));
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                Metadata.Custom.class,
                TrainedModelAssignmentMetadata.NAME,
                TrainedModelAssignmentMetadata::fromStream
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                NamedDiff.class,
                TrainedModelAssignmentMetadata.NAME,
                TrainedModelAssignmentMetadata::readDiffFrom
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                Metadata.Custom.class,
                TrainedModelAssignmentMetadata.DEPRECATED_NAME,
                TrainedModelAssignmentMetadata::fromStreamOld
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                NamedDiff.class,
                TrainedModelAssignmentMetadata.DEPRECATED_NAME,
                TrainedModelAssignmentMetadata::readDiffFromOld
            )
        );

        // Persistent tasks params
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                PersistentTaskParams.class,
                MlTasks.DATAFEED_TASK_NAME,
                StartDatafeedAction.DatafeedParams::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(PersistentTaskParams.class, MlTasks.JOB_TASK_NAME, OpenJobAction.JobParams::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                PersistentTaskParams.class,
                MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
                StartDataFrameAnalyticsAction.TaskParams::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                PersistentTaskParams.class,
                MlTasks.JOB_SNAPSHOT_UPGRADE_TASK_NAME,
                SnapshotUpgradeTaskParams::new
            )
        );

        // Persistent task states
        namedWriteables.add(new NamedWriteableRegistry.Entry(PersistentTaskState.class, JobTaskState.NAME, JobTaskState::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(PersistentTaskState.class, DatafeedState.NAME, DatafeedState::fromStream));
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(PersistentTaskState.class, DataFrameAnalyticsTaskState.NAME, DataFrameAnalyticsTaskState::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(PersistentTaskState.class, SnapshotUpgradeTaskState.NAME, SnapshotUpgradeTaskState::new)
        );

        namedWriteables.addAll(new MlDataFrameAnalysisNamedXContentProvider().getNamedWriteables());
        namedWriteables.addAll(new AnalysisStatsNamedWriteablesProvider().getNamedWriteables());
        namedWriteables.addAll(MlEvaluationNamedXContentProvider.getNamedWriteables());
        namedWriteables.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        namedWriteables.addAll(MlAutoscalingNamedWritableProvider.getNamedWriteables());
        namedWriteables.addAll(new CorrelationNamedContentProvider().getNamedWriteables());
        namedWriteables.addAll(new ChangePointNamedContentProvider().getNamedWriteables());

        return namedWriteables;
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings unused) {
        return List.of(
            SystemIndexDescriptor.builder()
                .setIndexPattern(MlMetaIndex.indexName() + "*")
                .setPrimaryIndex(MlMetaIndex.indexName())
                .setDescription("Contains scheduling and anomaly tracking metadata")
                .setMappings(MlMetaIndex.mapping())
                .setSettings(MlMetaIndex.settings())
                .setVersionMetaKey("version")
                .setOrigin(ML_ORIGIN)
                .build(),
            SystemIndexDescriptor.builder()
                .setIndexPattern(MlConfigIndex.indexName() + "*")
                .setPrimaryIndex(MlConfigIndex.indexName())
                .setDescription("Contains ML configuration data")
                .setMappings(MlConfigIndex.mapping())
                .setSettings(MlConfigIndex.settings())
                .setVersionMetaKey("version")
                .setOrigin(ML_ORIGIN)
                .build(),
            getInferenceIndexSystemIndexDescriptor()
        );
    }

    public static SystemIndexDescriptor getInferenceIndexSystemIndexDescriptor() {
        return SystemIndexDescriptor.builder()
            .setIndexPattern(InferenceIndexConstants.INDEX_PATTERN)
            .setPrimaryIndex(InferenceIndexConstants.LATEST_INDEX_NAME)
            .setDescription("Contains ML model configuration and statistics")
            .setMappings(InferenceIndexConstants.mapping())
            .setSettings(InferenceIndexConstants.settings())
            .setVersionMetaKey("version")
            .setOrigin(ML_ORIGIN)
            .build();
    }

    @Override
    public void prepareForIndicesMigration(ClusterService clusterService, Client client, ActionListener<Map<String, Object>> listener) {
        boolean isAlreadyInUpgradeMode = MlMetadata.getMlMetadata(clusterService.state()).isUpgradeMode();
        if (isAlreadyInUpgradeMode) {
            // ML is already in upgrade mode, so nothing will write to the ML system indices during their upgrade
            listener.onResponse(Collections.singletonMap("already_in_upgrade_mode", true));
            return;
        }

        // Enable ML upgrade mode before upgrading the ML system indices to ensure nothing writes to them during the upgrade
        Client originClient = new OriginSettingClient(client, ML_ORIGIN);
        originClient.execute(
            SetUpgradeModeAction.INSTANCE,
            new SetUpgradeModeAction.Request(true),
            ActionListener.wrap(r -> listener.onResponse(Collections.singletonMap("already_in_upgrade_mode", false)), listener::onFailure)
        );
    }

    @Override
    public void indicesMigrationComplete(
        Map<String, Object> preUpgradeMetadata,
        ClusterService clusterService,
        Client client,
        ActionListener<Boolean> listener
    ) {
        boolean wasAlreadyInUpgradeMode = (boolean) preUpgradeMetadata.getOrDefault("already_in_upgrade_mode", false);
        if (wasAlreadyInUpgradeMode) {
            // ML was already in upgrade mode before system indices upgrade started - we shouldn't disable it
            listener.onResponse(true);
            return;
        }

        Client originClient = new OriginSettingClient(client, ML_ORIGIN);
        originClient.execute(
            SetUpgradeModeAction.INSTANCE,
            new SetUpgradeModeAction.Request(false),
            ActionListener.wrap(r -> listener.onResponse(r.isAcknowledged()), listener::onFailure)
        );
    }

    /**
     * These are the ML hidden indices. They are "associated" in the sense that if the ML system indices
     * are backed up or deleted then these hidden indices should also be backed up or deleted.
     */
    private static final Collection<AssociatedIndexDescriptor> ASSOCIATED_INDEX_DESCRIPTORS = List.of(
        new AssociatedIndexDescriptor(RESULTS_INDEX_PREFIX + "*", "Results indices"),
        new AssociatedIndexDescriptor(STATE_INDEX_PREFIX + "*", "State indices"),
        new AssociatedIndexDescriptor(MlStatsIndex.indexPattern(), "ML stats index"),
        new AssociatedIndexDescriptor(".ml-notifications*", "ML notifications indices"),
        new AssociatedIndexDescriptor(".ml-annotations*", "ML annotations indices")
    );

    @Override
    public Collection<AssociatedIndexDescriptor> getAssociatedIndexDescriptors() {
        return ASSOCIATED_INDEX_DESCRIPTORS;
    }

    public static String[] getMlHiddenIndexPatterns() {
        return ASSOCIATED_INDEX_DESCRIPTORS.stream().map(AssociatedIndexDescriptor::getIndexPattern).toArray(String[]::new);
    }

    @Override
    public UnaryOperator<Map<String, IndexTemplateMetadata>> getIndexTemplateMetadataUpgrader() {
        return templates -> {
            // These are all legacy templates that were created in old versions. None are needed now. The
            // indices they were associated with either became system indices or now use composable templates.
            templates.remove(".ml-anomalies-");
            templates.remove(".ml-config");
            templates.remove(".ml-inference-000001");
            templates.remove(".ml-inference-000002");
            templates.remove(".ml-inference-000003");
            templates.remove(".ml-meta");
            templates.remove(".ml-notifications");
            templates.remove(".ml-notifications-000001");
            templates.remove(".ml-state");
            templates.remove(".ml-stats");
            return templates;
        };
    }

    @Override
    public String getFeatureName() {
        return "machine_learning";
    }

    @Override
    public String getFeatureDescription() {
        return "Provides anomaly detection and forecasting functionality";
    }

    @Override
    public void cleanUpFeature(
        ClusterService clusterService,
        Client unwrappedClient,
        ActionListener<ResetFeatureStateResponse.ResetFeatureStateStatus> finalListener
    ) {
        logger.info("Starting machine learning feature reset");
        OriginSettingClient client = new OriginSettingClient(unwrappedClient, ML_ORIGIN);

        final Map<String, Boolean> results = new ConcurrentHashMap<>();

        ActionListener<ResetFeatureStateResponse.ResetFeatureStateStatus> unsetResetModeListener = ActionListener.wrap(
            success -> client.execute(
                SetResetModeAction.INSTANCE,
                SetResetModeActionRequest.disabled(true),
                ActionListener.wrap(resetSuccess -> {
                    finalListener.onResponse(success);
                    logger.info("Finished machine learning feature reset");
                }, resetFailure -> {
                    logger.error("failed to disable reset mode after state otherwise successful machine learning reset", resetFailure);
                    finalListener.onFailure(
                        ExceptionsHelper.serverError(
                            "failed to disable reset mode after state otherwise successful machine learning reset",
                            resetFailure
                        )
                    );
                })
            ),
            failure -> {
                logger.error("failed to reset machine learning", failure);
                client.execute(
                    SetResetModeAction.INSTANCE,
                    SetResetModeActionRequest.disabled(false),
                    ActionListener.wrap(resetSuccess -> finalListener.onFailure(failure), resetFailure -> {
                        logger.error("failed to disable reset mode after state clean up failure", resetFailure);
                        finalListener.onFailure(failure);
                    })
                );
            }
        );

        ActionListener<ListTasksResponse> afterWaitingForTasks = ActionListener.wrap(listTasksResponse -> {
            listTasksResponse.rethrowFailures("Waiting for indexing requests for .ml-* indices");
            if (results.values().stream().allMatch(b -> b)) {
                if (memoryTracker.get() != null) {
                    memoryTracker.get()
                        .awaitAndClear(
                            ActionListener.wrap(
                                cacheCleared -> SystemIndexPlugin.super.cleanUpFeature(clusterService, client, unsetResetModeListener),
                                clearFailed -> {
                                    logger.error(
                                        "failed to clear memory tracker cache via machine learning reset feature API",
                                        clearFailed
                                    );
                                    SystemIndexPlugin.super.cleanUpFeature(clusterService, client, unsetResetModeListener);
                                }
                            )
                        );
                    return;
                }
                // Call into the original listener to clean up the indices and then clear ml memory cache
                SystemIndexPlugin.super.cleanUpFeature(clusterService, client, unsetResetModeListener);
            } else {
                final List<String> failedComponents = results.entrySet()
                    .stream()
                    .filter(result -> result.getValue() == false)
                    .map(Map.Entry::getKey)
                    .toList();
                unsetResetModeListener.onFailure(
                    new RuntimeException("Some machine learning components failed to reset: " + failedComponents)
                );
            }
        }, unsetResetModeListener::onFailure);

        ActionListener<StopDataFrameAnalyticsAction.Response> afterDataframesStopped = ActionListener.wrap(dataFrameStopResponse -> {
            // Handle the response
            results.put("data_frame/analytics", dataFrameStopResponse.isStopped());
            if (results.values().stream().allMatch(b -> b)) {
                client.admin()
                    .cluster()
                    .prepareListTasks()
                    // This waits for all xpack actions including: allocations, anomaly detections, analytics
                    .setActions("xpack/ml/*")
                    .setWaitForCompletion(true)
                    .execute(ActionListener.wrap(listMlTasks -> {
                        listMlTasks.rethrowFailures("Waiting for machine learning tasks");
                        client.admin()
                            .cluster()
                            .prepareListTasks()
                            .setActions("indices:data/write/bulk")
                            .setDetailed(true)
                            .setWaitForCompletion(true)
                            .setDescriptions("*.ml-*")
                            .execute(afterWaitingForTasks);
                    }, unsetResetModeListener::onFailure));
            } else {
                final List<String> failedComponents = results.entrySet()
                    .stream()
                    .filter(result -> result.getValue() == false)
                    .map(Map.Entry::getKey)
                    .toList();
                unsetResetModeListener.onFailure(
                    new RuntimeException("Some machine learning components failed to reset: " + failedComponents)
                );
            }
        }, unsetResetModeListener::onFailure);

        ActionListener<CloseJobAction.Response> afterAnomalyDetectionClosed = ActionListener.wrap(closeJobResponse -> {
            // Handle the response
            results.put("anomaly_detectors", closeJobResponse.isClosed());

            // Stop data frame analytics
            StopDataFrameAnalyticsAction.Request stopDataFramesReq = new StopDataFrameAnalyticsAction.Request("_all").setAllowNoMatch(true);
            client.execute(
                StopDataFrameAnalyticsAction.INSTANCE,
                stopDataFramesReq,
                ActionListener.wrap(afterDataframesStopped::onResponse, failure -> {
                    logger.warn(
                        "failed stopping data frame analytics jobs for machine learning feature reset. Attempting with force=true",
                        failure
                    );
                    client.execute(StopDataFrameAnalyticsAction.INSTANCE, stopDataFramesReq.setForce(true), afterDataframesStopped);
                })
            );
        }, unsetResetModeListener::onFailure);

        // Close anomaly detection jobs
        ActionListener<StopDatafeedAction.Response> afterDataFeedsStopped = ActionListener.wrap(datafeedResponse -> {
            // Handle the response
            results.put("datafeeds", datafeedResponse.isStopped());

            CloseJobAction.Request closeJobsRequest = new CloseJobAction.Request().setAllowNoMatch(true).setJobId("_all");
            // First attempt to kill all anomaly jobs
            client.execute(
                KillProcessAction.INSTANCE,
                new KillProcessAction.Request("*"),
                ActionListener.wrap(
                    // If successful, close and wait for jobs
                    success -> client.execute(
                        CloseJobAction.INSTANCE,
                        closeJobsRequest,
                        ActionListener.wrap(afterAnomalyDetectionClosed::onResponse, failure -> {
                            logger.warn(
                                "failed closing anomaly jobs for machine learning feature reset. Attempting with force=true",
                                failure
                            );
                            client.execute(CloseJobAction.INSTANCE, closeJobsRequest.setForce(true), afterAnomalyDetectionClosed);
                        })
                    ),
                    unsetResetModeListener::onFailure
                )
            );
        }, unsetResetModeListener::onFailure);

        // Stop data feeds
        ActionListener<CancelJobModelSnapshotUpgradeAction.Response> cancelSnapshotUpgradesListener = ActionListener.wrap(
            cancelUpgradesResponse -> {
                StopDatafeedAction.Request stopDatafeedsReq = new StopDatafeedAction.Request("_all").setAllowNoMatch(true);
                client.execute(
                    StopDatafeedAction.INSTANCE,
                    stopDatafeedsReq,
                    ActionListener.wrap(afterDataFeedsStopped::onResponse, failure -> {
                        logger.warn("failed stopping datafeeds for machine learning feature reset. Attempting with force=true", failure);
                        client.execute(StopDatafeedAction.INSTANCE, stopDatafeedsReq.setForce(true), afterDataFeedsStopped);
                    })
                );
            },
            unsetResetModeListener::onFailure
        );

        // Cancel model snapshot upgrades
        ActionListener<AcknowledgedResponse> stopDeploymentsListener = ActionListener.wrap(acknowledgedResponse -> {
            CancelJobModelSnapshotUpgradeAction.Request cancelSnapshotUpgradesReq = new CancelJobModelSnapshotUpgradeAction.Request(
                "_all",
                "_all"
            );
            client.execute(CancelJobModelSnapshotUpgradeAction.INSTANCE, cancelSnapshotUpgradesReq, cancelSnapshotUpgradesListener);
        }, unsetResetModeListener::onFailure);

        // Stop all model deployments
        ActionListener<AcknowledgedResponse> pipelineValidation = ActionListener.wrap(acknowledgedResponse -> {
            if (trainedModelAllocationClusterServiceSetOnce.get() == null) {
                stopDeploymentsListener.onResponse(AcknowledgedResponse.TRUE);
                return;
            }
            trainedModelAllocationClusterServiceSetOnce.get().removeAllModelAssignments(stopDeploymentsListener);
        }, unsetResetModeListener::onFailure);

        // validate no pipelines are using machine learning models
        ActionListener<AcknowledgedResponse> afterResetModeSet = ActionListener.wrap(acknowledgedResponse -> {
            int numberInferenceProcessors = countInferenceProcessors(clusterService.state());
            if (numberInferenceProcessors > 0) {
                unsetResetModeListener.onFailure(
                    new RuntimeException(
                        "Unable to reset machine learning feature as there are ingest pipelines "
                            + "still referencing trained machine learning models"
                    )
                );
                return;
            }
            pipelineValidation.onResponse(AcknowledgedResponse.of(true));
        }, finalListener::onFailure);

        // Indicate that a reset is now in progress
        client.execute(SetResetModeAction.INSTANCE, SetResetModeActionRequest.enabled(), afterResetModeSet);
    }

    @Override
    public BreakerSettings getCircuitBreaker(Settings settingsToUse) {
        return BreakerSettings.updateFromSettings(
            new BreakerSettings(
                TRAINED_MODEL_CIRCUIT_BREAKER_NAME,
                DEFAULT_MODEL_CIRCUIT_BREAKER_LIMIT,
                DEFAULT_MODEL_CIRCUIT_BREAKER_OVERHEAD,
                CircuitBreaker.Type.MEMORY,
                CircuitBreaker.Durability.TRANSIENT
            ),
            settingsToUse
        );
    }

    @Override
    public void setCircuitBreaker(CircuitBreaker circuitBreaker) {
        assert circuitBreaker.getName().equals(TRAINED_MODEL_CIRCUIT_BREAKER_NAME);
        this.inferenceModelBreaker.set(circuitBreaker);
    }

    public Collection<AutoscalingDeciderService> deciders() {
        if (enabled) {
            assert mlAutoscalingDeciderService.get() != null;
            return List.of(mlAutoscalingDeciderService.get());
        } else {
            return List.of();
        }
    }

    @Override
    public boolean safeToShutdown(String nodeId, SingleNodeShutdownMetadata.Type shutdownType) {
        if (enabled == false) {
            return true;
        }
        return mlLifeCycleService.get().isNodeSafeToShutdown(nodeId);
    }

    @Override
    public void signalShutdown(Collection<String> shutdownNodeIds) {
        if (enabled) {
            mlLifeCycleService.get().signalGracefulShutdown(shutdownNodeIds);
        }
    }
}
