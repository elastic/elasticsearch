/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.MlMetaIndex;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
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
import org.elasticsearch.xpack.core.ml.action.EvaluateDataFrameAction;
import org.elasticsearch.xpack.core.ml.action.ExplainDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.FinalizeJobExecutionAction;
import org.elasticsearch.xpack.core.ml.action.FindFileStructureAction;
import org.elasticsearch.xpack.core.ml.action.FlushJobAction;
import org.elasticsearch.xpack.core.ml.action.ForecastJobAction;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.core.ml.action.GetCalendarEventsAction;
import org.elasticsearch.xpack.core.ml.action.GetCalendarsAction;
import org.elasticsearch.xpack.core.ml.action.GetCategoriesAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetFiltersAction;
import org.elasticsearch.xpack.core.ml.action.GetInfluencersAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.core.ml.action.GetOverallBucketsAction;
import org.elasticsearch.xpack.core.ml.action.GetRecordsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsStatsAction;
import org.elasticsearch.xpack.core.ml.action.InternalInferModelAction;
import org.elasticsearch.xpack.core.ml.action.IsolateDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.KillProcessAction;
import org.elasticsearch.xpack.core.ml.action.MlInfoAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PersistJobAction;
import org.elasticsearch.xpack.core.ml.action.PostCalendarEventsAction;
import org.elasticsearch.xpack.core.ml.action.PostDataAction;
import org.elasticsearch.xpack.core.ml.action.PreviewDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutCalendarAction;
import org.elasticsearch.xpack.core.ml.action.PutDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutFilterAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction;
import org.elasticsearch.xpack.core.ml.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.SetUpgradeModeAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateCalendarJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateFilterAction;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.UpdateProcessAction;
import org.elasticsearch.xpack.core.ml.action.ValidateDetectorAction;
import org.elasticsearch.xpack.core.ml.action.ValidateJobConfigAction;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.MlDataFrameAnalysisNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndexFields;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.notifications.AuditorField;
import org.elasticsearch.xpack.core.template.TemplateUtils;
import org.elasticsearch.xpack.ml.action.TransportCloseJobAction;
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
import org.elasticsearch.xpack.ml.action.TransportEvaluateDataFrameAction;
import org.elasticsearch.xpack.ml.action.TransportExplainDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.action.TransportFinalizeJobExecutionAction;
import org.elasticsearch.xpack.ml.action.TransportFindFileStructureAction;
import org.elasticsearch.xpack.ml.action.TransportFlushJobAction;
import org.elasticsearch.xpack.ml.action.TransportForecastJobAction;
import org.elasticsearch.xpack.ml.action.TransportGetBucketsAction;
import org.elasticsearch.xpack.ml.action.TransportGetCalendarEventsAction;
import org.elasticsearch.xpack.ml.action.TransportGetCalendarsAction;
import org.elasticsearch.xpack.ml.action.TransportGetCategoriesAction;
import org.elasticsearch.xpack.ml.action.TransportGetDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.action.TransportGetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.ml.action.TransportGetDatafeedsAction;
import org.elasticsearch.xpack.ml.action.TransportGetDatafeedsStatsAction;
import org.elasticsearch.xpack.ml.action.TransportGetFiltersAction;
import org.elasticsearch.xpack.ml.action.TransportGetInfluencersAction;
import org.elasticsearch.xpack.ml.action.TransportGetJobsAction;
import org.elasticsearch.xpack.ml.action.TransportGetJobsStatsAction;
import org.elasticsearch.xpack.ml.action.TransportGetModelSnapshotsAction;
import org.elasticsearch.xpack.ml.action.TransportGetOverallBucketsAction;
import org.elasticsearch.xpack.ml.action.TransportGetRecordsAction;
import org.elasticsearch.xpack.ml.action.TransportGetTrainedModelsAction;
import org.elasticsearch.xpack.ml.action.TransportGetTrainedModelsStatsAction;
import org.elasticsearch.xpack.ml.action.TransportInternalInferModelAction;
import org.elasticsearch.xpack.ml.action.TransportIsolateDatafeedAction;
import org.elasticsearch.xpack.ml.action.TransportKillProcessAction;
import org.elasticsearch.xpack.ml.action.TransportMlInfoAction;
import org.elasticsearch.xpack.ml.action.TransportOpenJobAction;
import org.elasticsearch.xpack.ml.action.TransportPersistJobAction;
import org.elasticsearch.xpack.ml.action.TransportPostCalendarEventsAction;
import org.elasticsearch.xpack.ml.action.TransportPostDataAction;
import org.elasticsearch.xpack.ml.action.TransportPreviewDatafeedAction;
import org.elasticsearch.xpack.ml.action.TransportPutCalendarAction;
import org.elasticsearch.xpack.ml.action.TransportPutDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.action.TransportPutDatafeedAction;
import org.elasticsearch.xpack.ml.action.TransportPutFilterAction;
import org.elasticsearch.xpack.ml.action.TransportPutJobAction;
import org.elasticsearch.xpack.ml.action.TransportPutTrainedModelAction;
import org.elasticsearch.xpack.ml.action.TransportRevertModelSnapshotAction;
import org.elasticsearch.xpack.ml.action.TransportSetUpgradeModeAction;
import org.elasticsearch.xpack.ml.action.TransportStartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.action.TransportStartDatafeedAction;
import org.elasticsearch.xpack.ml.action.TransportStopDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.action.TransportStopDatafeedAction;
import org.elasticsearch.xpack.ml.action.TransportUpdateCalendarJobAction;
import org.elasticsearch.xpack.ml.action.TransportUpdateDatafeedAction;
import org.elasticsearch.xpack.ml.action.TransportUpdateFilterAction;
import org.elasticsearch.xpack.ml.action.TransportUpdateJobAction;
import org.elasticsearch.xpack.ml.action.TransportUpdateModelSnapshotAction;
import org.elasticsearch.xpack.ml.action.TransportUpdateProcessAction;
import org.elasticsearch.xpack.ml.action.TransportValidateDetectorAction;
import org.elasticsearch.xpack.ml.action.TransportValidateJobConfigAction;
import org.elasticsearch.xpack.ml.datafeed.DatafeedJobBuilder;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManager;
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
import org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;
import org.elasticsearch.xpack.ml.inference.persistence.InferenceInternalIndex;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.JobManagerHolder;
import org.elasticsearch.xpack.ml.job.UpdateJobProcessNotifier;
import org.elasticsearch.xpack.ml.job.categorization.MlClassicTokenizer;
import org.elasticsearch.xpack.ml.job.categorization.MlClassicTokenizerFactory;
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
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;
import org.elasticsearch.xpack.ml.process.DummyController;
import org.elasticsearch.xpack.ml.process.MlController;
import org.elasticsearch.xpack.ml.process.MlControllerHolder;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.process.NativeStorageProvider;
import org.elasticsearch.xpack.ml.rest.RestDeleteExpiredDataAction;
import org.elasticsearch.xpack.ml.rest.RestFindFileStructureAction;
import org.elasticsearch.xpack.ml.rest.RestMlInfoAction;
import org.elasticsearch.xpack.ml.rest.RestSetUpgradeModeAction;
import org.elasticsearch.xpack.ml.rest.calendar.RestDeleteCalendarAction;
import org.elasticsearch.xpack.ml.rest.calendar.RestDeleteCalendarEventAction;
import org.elasticsearch.xpack.ml.rest.calendar.RestDeleteCalendarJobAction;
import org.elasticsearch.xpack.ml.rest.calendar.RestGetCalendarEventsAction;
import org.elasticsearch.xpack.ml.rest.calendar.RestGetCalendarsAction;
import org.elasticsearch.xpack.ml.rest.calendar.RestPostCalendarEventAction;
import org.elasticsearch.xpack.ml.rest.calendar.RestPutCalendarAction;
import org.elasticsearch.xpack.ml.rest.calendar.RestPutCalendarJobAction;
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
import org.elasticsearch.xpack.ml.rest.dataframe.RestPutDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.rest.dataframe.RestStartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.rest.dataframe.RestStopDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.rest.filter.RestDeleteFilterAction;
import org.elasticsearch.xpack.ml.rest.filter.RestGetFiltersAction;
import org.elasticsearch.xpack.ml.rest.filter.RestPutFilterAction;
import org.elasticsearch.xpack.ml.rest.filter.RestUpdateFilterAction;
import org.elasticsearch.xpack.ml.rest.inference.RestDeleteTrainedModelAction;
import org.elasticsearch.xpack.ml.rest.inference.RestGetTrainedModelsAction;
import org.elasticsearch.xpack.ml.rest.inference.RestGetTrainedModelsStatsAction;
import org.elasticsearch.xpack.ml.rest.inference.RestPutTrainedModelAction;
import org.elasticsearch.xpack.ml.rest.job.RestCloseJobAction;
import org.elasticsearch.xpack.ml.rest.job.RestDeleteForecastAction;
import org.elasticsearch.xpack.ml.rest.job.RestDeleteJobAction;
import org.elasticsearch.xpack.ml.rest.job.RestFlushJobAction;
import org.elasticsearch.xpack.ml.rest.job.RestForecastJobAction;
import org.elasticsearch.xpack.ml.rest.job.RestGetJobStatsAction;
import org.elasticsearch.xpack.ml.rest.job.RestGetJobsAction;
import org.elasticsearch.xpack.ml.rest.job.RestOpenJobAction;
import org.elasticsearch.xpack.ml.rest.job.RestPostDataAction;
import org.elasticsearch.xpack.ml.rest.job.RestPostJobUpdateAction;
import org.elasticsearch.xpack.ml.rest.job.RestPutJobAction;
import org.elasticsearch.xpack.ml.rest.modelsnapshots.RestDeleteModelSnapshotAction;
import org.elasticsearch.xpack.ml.rest.modelsnapshots.RestGetModelSnapshotsAction;
import org.elasticsearch.xpack.ml.rest.modelsnapshots.RestRevertModelSnapshotAction;
import org.elasticsearch.xpack.ml.rest.modelsnapshots.RestUpdateModelSnapshotAction;
import org.elasticsearch.xpack.ml.rest.results.RestGetBucketsAction;
import org.elasticsearch.xpack.ml.rest.results.RestGetCategoriesAction;
import org.elasticsearch.xpack.ml.rest.results.RestGetInfluencersAction;
import org.elasticsearch.xpack.ml.rest.results.RestGetOverallBucketsAction;
import org.elasticsearch.xpack.ml.rest.results.RestGetRecordsAction;
import org.elasticsearch.xpack.ml.rest.validate.RestValidateDetectorAction;
import org.elasticsearch.xpack.ml.rest.validate.RestValidateJobConfigAction;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static java.util.Collections.emptyList;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;

public class MachineLearning extends Plugin implements SystemIndexPlugin, AnalysisPlugin, IngestPlugin, PersistentTaskPlugin {
    public static final String NAME = "ml";
    public static final String BASE_PATH = "/_ml/";
    public static final String PRE_V7_BASE_PATH = "/_xpack/ml/";
    public static final String DATAFEED_THREAD_POOL_NAME = NAME + "_datafeed";
    public static final String JOB_COMMS_THREAD_POOL_NAME = NAME + "_job_comms";
    public static final String UTILITY_THREAD_POOL_NAME = NAME + "_utility";

    // This is for performance testing.  It's not exposed to the end user.
    // Recompile if you want to compare performance with C++ tokenization.
    public static final boolean CATEGORIZATION_TOKENIZATION_IN_JAVA = true;

    public static final Setting<Boolean> ML_ENABLED =
            Setting.boolSetting("node.ml", XPackSettings.MACHINE_LEARNING_ENABLED, Property.NodeScope);

    public static final DiscoveryNodeRole ML_ROLE = new DiscoveryNodeRole("ml", "l") {

        @Override
        protected Setting<Boolean> roleSetting() {
            return ML_ENABLED;
        }

    };

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        if (this.enabled == false) {
            return Collections.emptyMap();
        }

        InferenceProcessor.Factory inferenceFactory = new InferenceProcessor.Factory(parameters.client,
            parameters.ingestService.getClusterService(),
            this.settings,
            parameters.ingestService);
        parameters.ingestService.addIngestClusterStateListener(inferenceFactory);
        return Collections.singletonMap(InferenceProcessor.TYPE, inferenceFactory);
    }

    @Override
    public Set<DiscoveryNodeRole> getRoles() {
        return Collections.singleton(ML_ROLE);
    }

    // This is not used in v7 and higher, but users are still prevented from setting it directly to avoid confusion
    private static final String PRE_V7_ML_ENABLED_NODE_ATTR = "ml.enabled";
    public static final String MAX_OPEN_JOBS_NODE_ATTR = "ml.max_open_jobs";
    public static final String MACHINE_MEMORY_NODE_ATTR = "ml.machine_memory";
    public static final Setting<Integer> CONCURRENT_JOB_ALLOCATIONS =
            Setting.intSetting("xpack.ml.node_concurrent_job_allocations", 2, 0, Property.Dynamic, Property.NodeScope);
    /**
     * The amount of memory needed to load the ML native code shared libraries. The assumption is that the first
     * ML job to run on a given node will do this, and then subsequent ML jobs on the same node will reuse the
     * same already-loaded code.
     */
    public static final ByteSizeValue NATIVE_EXECUTABLE_CODE_OVERHEAD = new ByteSizeValue(30, ByteSizeUnit.MB);
    // Values higher than 100% are allowed to accommodate use cases where swapping has been determined to be acceptable.
    // Anomaly detector jobs only use their full model memory during background persistence, and this is deliberately
    // staggered, so with large numbers of jobs few will generally be persisting state at the same time.
    // Settings higher than available memory are only recommended for OEM type situations where a wrapper tightly
    // controls the types of jobs that can be created, and each job alone is considerably smaller than what each node
    // can handle.
    public static final Setting<Integer> MAX_MACHINE_MEMORY_PERCENT =
            Setting.intSetting("xpack.ml.max_machine_memory_percent", 30, 5, 200, Property.Dynamic, Property.NodeScope);
    public static final Setting<Integer> MAX_LAZY_ML_NODES =
            Setting.intSetting("xpack.ml.max_lazy_ml_nodes", 0, 0, 3, Property.Dynamic, Property.NodeScope);

    // Before 8.0.0 this needs to match the max allowed value for xpack.ml.max_open_jobs,
    // as the current node could be running in a cluster where some nodes are still using
    // that setting.  From 8.0.0 onwards we have the flexibility to increase it...
    private static final int MAX_MAX_OPEN_JOBS_PER_NODE = 512;
    // This setting is cluster-wide and can be set dynamically. However, prior to version 7.1 it was
    // a non-dynamic per-node setting. n a mixed version cluster containing 6.7 or 7.0 nodes those
    // older nodes will not react to the dynamic changes. Therefore, in such mixed version clusters
    // allocation will be based on the value first read at node startup rather than the current value.
    public static final Setting<Integer> MAX_OPEN_JOBS_PER_NODE =
            Setting.intSetting("xpack.ml.max_open_jobs", 20, 1, MAX_MAX_OPEN_JOBS_PER_NODE, Property.Dynamic, Property.NodeScope);

    public static final Setting<TimeValue> PROCESS_CONNECT_TIMEOUT =
        Setting.timeSetting("xpack.ml.process_connect_timeout", TimeValue.timeValueSeconds(10),
            TimeValue.timeValueSeconds(5), Setting.Property.Dynamic, Setting.Property.NodeScope);

    // Undocumented setting for integration test purposes
    public static final Setting<ByteSizeValue> MIN_DISK_SPACE_OFF_HEAP =
        Setting.byteSizeSetting("xpack.ml.min_disk_space_off_heap", new ByteSizeValue(5, ByteSizeUnit.GB), Setting.Property.NodeScope);

    private static final Logger logger = LogManager.getLogger(MachineLearning.class);

    private final Settings settings;
    private final Environment env;
    private final boolean enabled;

    private final SetOnce<AutodetectProcessManager> autodetectProcessManager = new SetOnce<>();
    private final SetOnce<DatafeedManager> datafeedManager = new SetOnce<>();
    private final SetOnce<DataFrameAnalyticsManager> dataFrameAnalyticsManager = new SetOnce<>();
    private final SetOnce<DataFrameAnalyticsAuditor> dataFrameAnalyticsAuditor = new SetOnce<>();
    private final SetOnce<MlMemoryTracker> memoryTracker = new SetOnce<>();

    public MachineLearning(Settings settings, Path configPath) {
        this.settings = settings;
        this.enabled = XPackSettings.MACHINE_LEARNING_ENABLED.get(settings);
        this.env = new Environment(settings, configPath);
    }

    protected XPackLicenseState getLicenseState() { return XPackPlugin.getSharedLicenseState(); }

    public static boolean isMlNode(DiscoveryNode node) {
        Map<String, String> nodeAttributes = node.getAttributes();
        try {
            return Integer.parseInt(nodeAttributes.get(MAX_OPEN_JOBS_NODE_ATTR)) > 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public List<Setting<?>> getSettings() {
        return List.of(
                MachineLearningField.AUTODETECT_PROCESS,
                PROCESS_CONNECT_TIMEOUT,
                ML_ENABLED,
                CONCURRENT_JOB_ALLOCATIONS,
                MachineLearningField.MAX_MODEL_MEMORY_LIMIT,
                MAX_LAZY_ML_NODES,
                MAX_MACHINE_MEMORY_PERCENT,
                AutodetectBuilder.DONT_PERSIST_MODEL_STATE_SETTING,
                AutodetectBuilder.MAX_ANOMALY_RECORDS_SETTING_DYNAMIC,
                MAX_OPEN_JOBS_PER_NODE,
                MIN_DISK_SPACE_OFF_HEAP,
                MlConfigMigrationEligibilityCheck.ENABLE_CONFIG_MIGRATION,
                InferenceProcessor.MAX_INFERENCE_PROCESSORS,
                ModelLoadingService.INFERENCE_MODEL_CACHE_SIZE,
                ModelLoadingService.INFERENCE_MODEL_CACHE_TTL,
                ResultsPersisterService.PERSIST_RESULTS_MAX_RETRIES
            );
    }

    public Settings additionalSettings() {
        String mlEnabledNodeAttrName = "node.attr." + PRE_V7_ML_ENABLED_NODE_ATTR;
        String maxOpenJobsPerNodeNodeAttrName = "node.attr." + MAX_OPEN_JOBS_NODE_ATTR;
        String machineMemoryAttrName = "node.attr." + MACHINE_MEMORY_NODE_ATTR;

        if (enabled == false) {
            disallowMlNodeAttributes(mlEnabledNodeAttrName, maxOpenJobsPerNodeNodeAttrName, machineMemoryAttrName);
            return Settings.EMPTY;
        }

        Settings.Builder additionalSettings = Settings.builder();
        Boolean allocationEnabled = ML_ENABLED.get(settings);
        if (allocationEnabled != null && allocationEnabled) {
            // TODO: stop setting this attribute in 8.0.0 but disallow it (like mlEnabledNodeAttrName below)
            // The ML UI will need to be changed to check machineMemoryAttrName instead before this is done
            addMlNodeAttribute(additionalSettings, maxOpenJobsPerNodeNodeAttrName,
                    String.valueOf(MAX_OPEN_JOBS_PER_NODE.get(settings)));
            addMlNodeAttribute(additionalSettings, machineMemoryAttrName,
                    Long.toString(machineMemoryFromStats(OsProbe.getInstance().osStats())));
            // This is not used in v7 and higher, but users are still prevented from setting it directly to avoid confusion
            disallowMlNodeAttributes(mlEnabledNodeAttrName);
        } else {
            disallowMlNodeAttributes(mlEnabledNodeAttrName, maxOpenJobsPerNodeNodeAttrName, machineMemoryAttrName);
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

    private void disallowMlNodeAttributes(String... mlNodeAttributes) {
        for (String attrName : mlNodeAttributes) {
            if (settings.get(attrName) != null) {
                reportClashingNodeAttribute(attrName);
            }
        }
    }

    private void reportClashingNodeAttribute(String attrName) {
        throw new IllegalArgumentException("Directly setting [" + attrName + "] is not permitted - " +
                "it is reserved for machine learning. If your intention was to customize machine learning, set the [" +
                attrName.replace("node.attr.", "xpack.") + "] setting instead.");
    }

    // overridable by tests
    protected Clock getClock() {
        return Clock.systemUTC();
    }


    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
        if (enabled == false) {
            // special holder for @link(MachineLearningFeatureSetUsage) which needs access to job manager, empty if ML is disabled
            return Collections.singletonList(new JobManagerHolder());
        }

        AnomalyDetectionAuditor anomalyDetectionAuditor = new AnomalyDetectionAuditor(client, clusterService.getNodeName());
        DataFrameAnalyticsAuditor dataFrameAnalyticsAuditor = new DataFrameAnalyticsAuditor(client, clusterService.getNodeName());
        InferenceAuditor inferenceAuditor = new InferenceAuditor(client, clusterService.getNodeName());
        this.dataFrameAnalyticsAuditor.set(dataFrameAnalyticsAuditor);
        OriginSettingClient originSettingClient = new OriginSettingClient(client, ClientHelper.ML_ORIGIN);
        ResultsPersisterService resultsPersisterService = new ResultsPersisterService(originSettingClient, clusterService, settings);
        JobResultsProvider jobResultsProvider = new JobResultsProvider(client, settings);
        JobResultsPersister jobResultsPersister =
            new JobResultsPersister(originSettingClient, resultsPersisterService, anomalyDetectionAuditor);
        JobDataCountsPersister jobDataCountsPersister = new JobDataCountsPersister(client,
            resultsPersisterService,
            anomalyDetectionAuditor);
        JobConfigProvider jobConfigProvider = new JobConfigProvider(client, xContentRegistry);
        DatafeedConfigProvider datafeedConfigProvider = new DatafeedConfigProvider(client, xContentRegistry);
        UpdateJobProcessNotifier notifier = new UpdateJobProcessNotifier(client, clusterService, threadPool);
        JobManager jobManager = new JobManager(env,
            settings,
            jobResultsProvider,
            jobResultsPersister,
            clusterService,
            anomalyDetectionAuditor,
            threadPool,
            client,
            notifier,
            xContentRegistry);

        // special holder for @link(MachineLearningFeatureSetUsage) which needs access to job manager if ML is enabled
        JobManagerHolder jobManagerHolder = new JobManagerHolder(jobManager);

        NativeStorageProvider nativeStorageProvider = new NativeStorageProvider(environment, MIN_DISK_SPACE_OFF_HEAP.get(settings));

        MlController mlController;
        AutodetectProcessFactory autodetectProcessFactory;
        NormalizerProcessFactory normalizerProcessFactory;
        AnalyticsProcessFactory<AnalyticsResult> analyticsProcessFactory;
        AnalyticsProcessFactory<MemoryUsageEstimationResult> memoryEstimationProcessFactory;
        if (MachineLearningField.AUTODETECT_PROCESS.get(settings)) {
            try {
                NativeController nativeController = NativeController.makeNativeController(clusterService.getNodeName(), environment);
                autodetectProcessFactory = new NativeAutodetectProcessFactory(
                    environment,
                    settings,
                    nativeController,
                    clusterService,
                    resultsPersisterService,
                    anomalyDetectionAuditor);
                normalizerProcessFactory = new NativeNormalizerProcessFactory(environment, nativeController, clusterService);
                analyticsProcessFactory = new NativeAnalyticsProcessFactory(
                    environment,
                    nativeController,
                    clusterService,
                    xContentRegistry,
                    resultsPersisterService,
                    dataFrameAnalyticsAuditor);
                memoryEstimationProcessFactory =
                    new NativeMemoryUsageEstimationProcessFactory(environment, nativeController, clusterService);
                mlController = nativeController;
            } catch (IOException e) {
                // The low level cause of failure from the named pipe helper's perspective is almost never the real root cause, so
                // only log this at the lowest level of detail.  It's almost always "file not found" on a named pipe we expect to be
                // able to connect to, but the thing we really need to know is what stopped the native process creating the named pipe.
                logger.trace("Failed to connect to ML native controller", e);
                throw new ElasticsearchException("Failure running machine learning native code. This could be due to running "
                    + "on an unsupported OS or distribution, missing OS libraries, or a problem with the temp directory. To "
                    + "bypass this problem by running Elasticsearch without machine learning functionality set ["
                    + XPackSettings.MACHINE_LEARNING_ENABLED.getKey() + ": false].");
            }
        } else {
            mlController = new DummyController();
            autodetectProcessFactory = (job, autodetectParams, executorService, onProcessCrash) ->
                    new BlackHoleAutodetectProcess(job.getId(), onProcessCrash);
            // factor of 1.0 makes renormalization a no-op
            normalizerProcessFactory = (jobId, quantilesState, bucketSpan, executorService) -> new MultiplyingNormalizerProcess(1.0);
            analyticsProcessFactory = (jobId, analyticsProcessConfig, state, executorService, onProcessCrash) -> null;
            memoryEstimationProcessFactory = (jobId, analyticsProcessConfig, state, executorService, onProcessCrash) -> null;
        }
        NormalizerFactory normalizerFactory = new NormalizerFactory(normalizerProcessFactory,
                threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME));
        AutodetectProcessManager autodetectProcessManager = new AutodetectProcessManager(env, settings, client, threadPool,
                xContentRegistry, anomalyDetectionAuditor, clusterService, jobManager, jobResultsProvider, jobResultsPersister,
                jobDataCountsPersister, autodetectProcessFactory, normalizerFactory, nativeStorageProvider);
        this.autodetectProcessManager.set(autodetectProcessManager);
        DatafeedJobBuilder datafeedJobBuilder =
            new DatafeedJobBuilder(
                client,
                xContentRegistry,
                anomalyDetectionAuditor,
                System::currentTimeMillis,
                jobConfigProvider,
                jobResultsProvider,
                datafeedConfigProvider,
                jobResultsPersister,
                settings,
                clusterService.getNodeName());
        DatafeedManager datafeedManager = new DatafeedManager(threadPool, client, clusterService, datafeedJobBuilder,
                System::currentTimeMillis, anomalyDetectionAuditor, autodetectProcessManager);
        this.datafeedManager.set(datafeedManager);

        // Inference components
        final TrainedModelProvider trainedModelProvider = new TrainedModelProvider(client, xContentRegistry);
        final ModelLoadingService modelLoadingService = new ModelLoadingService(trainedModelProvider,
            inferenceAuditor,
            threadPool,
            clusterService,
            xContentRegistry,
            settings);

        // Data frame analytics components
        AnalyticsProcessManager analyticsProcessManager = new AnalyticsProcessManager(client, threadPool, analyticsProcessFactory,
            dataFrameAnalyticsAuditor, trainedModelProvider, resultsPersisterService);
        MemoryUsageEstimationProcessManager memoryEstimationProcessManager =
            new MemoryUsageEstimationProcessManager(
                threadPool.generic(), threadPool.executor(MachineLearning.JOB_COMMS_THREAD_POOL_NAME), memoryEstimationProcessFactory);
        DataFrameAnalyticsConfigProvider dataFrameAnalyticsConfigProvider = new DataFrameAnalyticsConfigProvider(client, xContentRegistry);
        assert client instanceof NodeClient;
        DataFrameAnalyticsManager dataFrameAnalyticsManager = new DataFrameAnalyticsManager(
            (NodeClient) client, dataFrameAnalyticsConfigProvider, analyticsProcessManager, dataFrameAnalyticsAuditor);
        this.dataFrameAnalyticsManager.set(dataFrameAnalyticsManager);

        // Components shared by anomaly detection and data frame analytics
        MlMemoryTracker memoryTracker = new MlMemoryTracker(settings, clusterService, threadPool, jobManager, jobResultsProvider,
            dataFrameAnalyticsConfigProvider);
        this.memoryTracker.set(memoryTracker);
        MlLifeCycleService mlLifeCycleService = new MlLifeCycleService(clusterService, datafeedManager, mlController,
            autodetectProcessManager, memoryTracker);
        MlAssignmentNotifier mlAssignmentNotifier = new MlAssignmentNotifier(anomalyDetectionAuditor, dataFrameAnalyticsAuditor, threadPool,
            new MlConfigMigrator(settings, client, clusterService), clusterService);

        // this object registers as a license state listener, and is never removed, so there's no need to retain another reference to it
        final InvalidLicenseEnforcer enforcer =
                new InvalidLicenseEnforcer(getLicenseState(), threadPool, datafeedManager, autodetectProcessManager);
        enforcer.listenForLicenseStateChanges();

        // Perform node startup operations
        nativeStorageProvider.cleanupLocalTmpStorageInCaseOfUncleanShutdown();

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
                datafeedManager,
                anomalyDetectionAuditor,
                dataFrameAnalyticsAuditor,
                inferenceAuditor,
                mlAssignmentNotifier,
                memoryTracker,
                analyticsProcessManager,
                memoryEstimationProcessManager,
                dataFrameAnalyticsConfigProvider,
                nativeStorageProvider,
                modelLoadingService,
                trainedModelProvider
        );
    }

    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(ClusterService clusterService,
                                                                       ThreadPool threadPool,
                                                                       Client client,
                                                                       SettingsModule settingsModule) {
        if (enabled == false) {
            return emptyList();
        }

        return Arrays.asList(
                new TransportOpenJobAction.OpenJobPersistentTasksExecutor(settings, clusterService, autodetectProcessManager.get(),
                    memoryTracker.get(), client),
                new TransportStartDatafeedAction.StartDatafeedPersistentTasksExecutor(datafeedManager.get()),
                new TransportStartDataFrameAnalyticsAction.TaskExecutor(settings, client, clusterService, dataFrameAnalyticsManager.get(),
                    dataFrameAnalyticsAuditor.get(), memoryTracker.get())
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        if (false == enabled) {
            return emptyList();
        }
        return Arrays.asList(
            new RestGetJobsAction(),
            new RestGetJobStatsAction(),
            new RestMlInfoAction(),
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
            new RestValidateDetectorAction(),
            new RestValidateJobConfigAction(),
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
            new RestFindFileStructureAction(),
            new RestSetUpgradeModeAction(),
            new RestGetDataFrameAnalyticsAction(),
            new RestGetDataFrameAnalyticsStatsAction(),
            new RestPutDataFrameAnalyticsAction(),
            new RestDeleteDataFrameAnalyticsAction(),
            new RestStartDataFrameAnalyticsAction(),
            new RestStopDataFrameAnalyticsAction(),
            new RestEvaluateDataFrameAction(),
            new RestExplainDataFrameAnalyticsAction(),
            new RestGetTrainedModelsAction(),
            new RestDeleteTrainedModelAction(),
            new RestGetTrainedModelsStatsAction(),
            new RestPutTrainedModelAction(),
            // CAT Handlers
            new RestCatJobsAction(),
            new RestCatTrainedModelsAction(),
            new RestCatDatafeedsAction()
        );
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        var usageAction =
            new ActionHandler<>(XPackUsageFeatureAction.MACHINE_LEARNING, MachineLearningUsageTransportAction.class);
        var infoAction =
            new ActionHandler<>(XPackInfoFeatureAction.MACHINE_LEARNING, MachineLearningInfoTransportAction.class);
        if (false == enabled) {
            return Arrays.asList(usageAction, infoAction);
        }
        return Arrays.asList(
                new ActionHandler<>(GetJobsAction.INSTANCE, TransportGetJobsAction.class),
                new ActionHandler<>(GetJobsStatsAction.INSTANCE, TransportGetJobsStatsAction.class),
                new ActionHandler<>(MlInfoAction.INSTANCE, TransportMlInfoAction.class),
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
                new ActionHandler<>(ValidateDetectorAction.INSTANCE, TransportValidateDetectorAction.class),
                new ActionHandler<>(ValidateJobConfigAction.INSTANCE, TransportValidateJobConfigAction.class),
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
                new ActionHandler<>(FindFileStructureAction.INSTANCE, TransportFindFileStructureAction.class),
                new ActionHandler<>(SetUpgradeModeAction.INSTANCE, TransportSetUpgradeModeAction.class),
                new ActionHandler<>(GetDataFrameAnalyticsAction.INSTANCE, TransportGetDataFrameAnalyticsAction.class),
                new ActionHandler<>(GetDataFrameAnalyticsStatsAction.INSTANCE, TransportGetDataFrameAnalyticsStatsAction.class),
                new ActionHandler<>(PutDataFrameAnalyticsAction.INSTANCE, TransportPutDataFrameAnalyticsAction.class),
                new ActionHandler<>(DeleteDataFrameAnalyticsAction.INSTANCE, TransportDeleteDataFrameAnalyticsAction.class),
                new ActionHandler<>(StartDataFrameAnalyticsAction.INSTANCE, TransportStartDataFrameAnalyticsAction.class),
                new ActionHandler<>(StopDataFrameAnalyticsAction.INSTANCE, TransportStopDataFrameAnalyticsAction.class),
                new ActionHandler<>(EvaluateDataFrameAction.INSTANCE, TransportEvaluateDataFrameAction.class),
                new ActionHandler<>(ExplainDataFrameAnalyticsAction.INSTANCE, TransportExplainDataFrameAnalyticsAction.class),
                new ActionHandler<>(InternalInferModelAction.INSTANCE, TransportInternalInferModelAction.class),
                new ActionHandler<>(GetTrainedModelsAction.INSTANCE, TransportGetTrainedModelsAction.class),
                new ActionHandler<>(DeleteTrainedModelAction.INSTANCE, TransportDeleteTrainedModelAction.class),
                new ActionHandler<>(GetTrainedModelsStatsAction.INSTANCE, TransportGetTrainedModelsStatsAction.class),
                new ActionHandler<>(PutTrainedModelAction.INSTANCE, TransportPutTrainedModelAction.class),
                usageAction,
                infoAction);
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        if (false == enabled) {
            return emptyList();
        }

        // These thread pools scale such that they can accommodate the maximum number of jobs per node
        // that is permitted to be configured.  It is up to other code to enforce the configured maximum
        // number of jobs per node.

        // 4 threads per job process: for input, c++ logger output, result processing and state processing.
        ScalingExecutorBuilder jobComms = new ScalingExecutorBuilder(JOB_COMMS_THREAD_POOL_NAME,
            4, MAX_MAX_OPEN_JOBS_PER_NODE * 4, TimeValue.timeValueMinutes(1), "xpack.ml.job_comms_thread_pool");

        // This pool is used by renormalization, plus some other parts of ML that
        // need to kick off non-trivial activities that mustn't block other threads.
        ScalingExecutorBuilder utility = new ScalingExecutorBuilder(UTILITY_THREAD_POOL_NAME,
            1, MAX_MAX_OPEN_JOBS_PER_NODE * 4, TimeValue.timeValueMinutes(10), "xpack.ml.utility_thread_pool");

        ScalingExecutorBuilder datafeed = new ScalingExecutorBuilder(DATAFEED_THREAD_POOL_NAME,
            1, MAX_MAX_OPEN_JOBS_PER_NODE, TimeValue.timeValueMinutes(1), "xpack.ml.datafeed_thread_pool");

        return Arrays.asList(jobComms, utility, datafeed);
    }

    @Override
    public Map<String, AnalysisProvider<TokenizerFactory>> getTokenizers() {
        return Collections.singletonMap(MlClassicTokenizer.NAME, MlClassicTokenizerFactory::new);
    }

    @Override
    public UnaryOperator<Map<String, IndexTemplateMetaData>> getIndexTemplateMetaDataUpgrader() {
        return templates -> {

            try (XContentBuilder auditMapping = ElasticsearchMappings.auditMessageMapping()) {
                IndexTemplateMetaData notificationMessageTemplate =
                    IndexTemplateMetaData.builder(AuditorField.NOTIFICATIONS_INDEX)
                        .putMapping(SINGLE_MAPPING_NAME, Strings.toString(auditMapping))
                        .patterns(Collections.singletonList(AuditorField.NOTIFICATIONS_INDEX))
                        .version(Version.CURRENT.id)
                        .settings(Settings.builder()
                                // Our indexes are small and one shard puts the
                                // least possible burden on Elasticsearch
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, "0-1"))
                        .build();
                templates.put(AuditorField.NOTIFICATIONS_INDEX, notificationMessageTemplate);
            } catch (IOException e) {
                logger.warn("Error loading the template for the notification message index", e);
            }

            try (XContentBuilder docMapping = MlMetaIndex.docMapping()) {
                IndexTemplateMetaData metaTemplate =
                    IndexTemplateMetaData.builder(MlMetaIndex.INDEX_NAME)
                        .patterns(Collections.singletonList(MlMetaIndex.INDEX_NAME))
                        .settings(Settings.builder()
                                // Our indexes are small and one shard puts the
                                // least possible burden on Elasticsearch
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, "0-1"))
                        .version(Version.CURRENT.id)
                        .putMapping(SINGLE_MAPPING_NAME, Strings.toString(docMapping))
                        .build();
                templates.put(MlMetaIndex.INDEX_NAME, metaTemplate);
            } catch (IOException e) {
                logger.warn("Error loading the template for the " + MlMetaIndex.INDEX_NAME + " index", e);
            }

            try (XContentBuilder configMapping = ElasticsearchMappings.configMapping()) {
                IndexTemplateMetaData configTemplate =
                    IndexTemplateMetaData.builder(AnomalyDetectorsIndex.configIndexName())
                        .patterns(Collections.singletonList(AnomalyDetectorsIndex.configIndexName()))
                        .settings(Settings.builder()
                                // Our indexes are small and one shard puts the
                                // least possible burden on Elasticsearch
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
                                .put(IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(),
                                        AnomalyDetectorsIndex.CONFIG_INDEX_MAX_RESULTS_WINDOW))
                        .version(Version.CURRENT.id)
                        .putMapping(SINGLE_MAPPING_NAME, Strings.toString(configMapping))
                        .build();
                templates.put(AnomalyDetectorsIndex.configIndexName(), configTemplate);
            } catch (IOException e) {
                logger.warn("Error loading the template for the " + AnomalyDetectorsIndex.configIndexName() + " index", e);
            }

            try (XContentBuilder stateMapping = ElasticsearchMappings.stateMapping()) {
                IndexTemplateMetaData stateTemplate =
                    IndexTemplateMetaData.builder(AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX)
                        .patterns(Collections.singletonList(AnomalyDetectorsIndex.jobStateIndexPattern()))
                        // TODO review these settings
                        .settings(Settings.builder()
                                .put(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, "0-1"))
                        .putMapping(SINGLE_MAPPING_NAME, Strings.toString(stateMapping))
                        .version(Version.CURRENT.id)
                        .build();

                templates.put(AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX, stateTemplate);
            } catch (IOException e) {
                logger.error("Error loading the template for the " + AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + " index", e);
            }

            try (XContentBuilder docMapping = ElasticsearchMappings.resultsMapping()) {
                IndexTemplateMetaData jobResultsTemplate =
                    IndexTemplateMetaData.builder(AnomalyDetectorsIndex.jobResultsIndexPrefix())
                        .patterns(Collections.singletonList(AnomalyDetectorsIndex.jobResultsIndexPrefix() + "*"))
                        .settings(Settings.builder()
                                .put(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
                                // Sacrifice durability for performance: in the event of power
                                // failure we can lose the last 5 seconds of changes, but it's
                                // much faster
                                .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), "async")
                                // set the default all search field
                                .put(IndexSettings.DEFAULT_FIELD_SETTING.getKey(), ElasticsearchMappings.ALL_FIELD_VALUES))
                        .putMapping(SINGLE_MAPPING_NAME, Strings.toString(docMapping))
                        .version(Version.CURRENT.id)
                        .build();
                templates.put(AnomalyDetectorsIndex.jobResultsIndexPrefix(), jobResultsTemplate);
            } catch (IOException e) {
                logger.error("Error loading the template for the " + AnomalyDetectorsIndex.jobResultsIndexPrefix() + " indices", e);
            }

            try {
                templates.put(InferenceIndexConstants.LATEST_INDEX_NAME, InferenceInternalIndex.getIndexTemplateMetaData());
            } catch (IOException e) {
                logger.error("Error loading the template for the " + InferenceIndexConstants.LATEST_INDEX_NAME + " index", e);
            }

            return templates;
        };
    }

    public static boolean allTemplatesInstalled(ClusterState clusterState) {
        boolean allPresent = true;
        List<String> templateNames =
            Arrays.asList(
                AuditorField.NOTIFICATIONS_INDEX,
                MlMetaIndex.INDEX_NAME,
                AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX,
                AnomalyDetectorsIndex.jobResultsIndexPrefix(),
                InferenceIndexConstants.LATEST_INDEX_NAME);
        for (String templateName : templateNames) {
            allPresent = allPresent && TemplateUtils.checkTemplateExistsAndVersionIsGTECurrentVersion(templateName, clusterState);
        }

        return allPresent;
    }

    /**
     * Find the memory size (in bytes) of the machine this node is running on.
     * Takes container limits (as used by Docker for example) into account.
     */
    static long machineMemoryFromStats(OsStats stats) {
        long mem = stats.getMem().getTotal().getBytes();
        OsStats.Cgroup cgroup = stats.getCgroup();
        if (cgroup != null) {
            String containerLimitStr = cgroup.getMemoryLimitInBytes();
            if (containerLimitStr != null) {
                BigInteger containerLimit = new BigInteger(containerLimitStr);
                if ((containerLimit.compareTo(BigInteger.valueOf(mem)) < 0 && containerLimit.compareTo(BigInteger.ZERO) > 0)
                        // mem <= 0 means the value couldn't be obtained for some reason
                        || (mem <= 0 && containerLimit.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) < 0)) {
                    mem = containerLimit.longValue();
                }
            }
        }
        return mem;
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlEvaluationNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new MlDataFrameAnalysisNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        return namedXContent;
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors() {
        return Collections.unmodifiableList(Arrays.asList(
            new SystemIndexDescriptor(MlMetaIndex.INDEX_NAME, "Contains scheduling and anomaly tracking metadata"),
            new SystemIndexDescriptor(AnomalyDetectorsIndexFields.STATE_INDEX_PATTERN, "Contains ML model state"),
            new SystemIndexDescriptor(AnomalyDetectorsIndexFields.CONFIG_INDEX, "Contains ML configuration data"),
            new SystemIndexDescriptor(InferenceIndexConstants.INDEX_PATTERN, "Contains ML model configuration and statistics")
        ));
    }
}
