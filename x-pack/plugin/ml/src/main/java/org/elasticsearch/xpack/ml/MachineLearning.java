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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.MlMetaIndex;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.DeleteCalendarAction;
import org.elasticsearch.xpack.core.ml.action.DeleteCalendarEventAction;
import org.elasticsearch.xpack.core.ml.action.DeleteDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.DeleteExpiredDataAction;
import org.elasticsearch.xpack.core.ml.action.DeleteFilterAction;
import org.elasticsearch.xpack.core.ml.action.DeleteForecastAction;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.DeleteModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.FinalizeJobExecutionAction;
import org.elasticsearch.xpack.core.ml.action.FindFileStructureAction;
import org.elasticsearch.xpack.core.ml.action.FlushJobAction;
import org.elasticsearch.xpack.core.ml.action.ForecastJobAction;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.core.ml.action.GetCalendarEventsAction;
import org.elasticsearch.xpack.core.ml.action.GetCalendarsAction;
import org.elasticsearch.xpack.core.ml.action.GetCategoriesAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetFiltersAction;
import org.elasticsearch.xpack.core.ml.action.GetInfluencersAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.core.ml.action.GetOverallBucketsAction;
import org.elasticsearch.xpack.core.ml.action.GetRecordsAction;
import org.elasticsearch.xpack.core.ml.action.IsolateDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.KillProcessAction;
import org.elasticsearch.xpack.core.ml.action.MlInfoAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PersistJobAction;
import org.elasticsearch.xpack.core.ml.action.PostCalendarEventsAction;
import org.elasticsearch.xpack.core.ml.action.PostDataAction;
import org.elasticsearch.xpack.core.ml.action.PreviewDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutCalendarAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutFilterAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.MlUpgradeAction;
import org.elasticsearch.xpack.core.ml.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateCalendarJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateFilterAction;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.UpdateProcessAction;
import org.elasticsearch.xpack.core.ml.action.ValidateDetectorAction;
import org.elasticsearch.xpack.core.ml.action.ValidateJobConfigAction;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.notifications.AuditMessage;
import org.elasticsearch.xpack.core.ml.notifications.AuditorField;
import org.elasticsearch.xpack.core.template.TemplateUtils;
import org.elasticsearch.xpack.ml.action.TransportCloseJobAction;
import org.elasticsearch.xpack.ml.action.TransportDeleteCalendarAction;
import org.elasticsearch.xpack.ml.action.TransportDeleteCalendarEventAction;
import org.elasticsearch.xpack.ml.action.TransportDeleteDatafeedAction;
import org.elasticsearch.xpack.ml.action.TransportDeleteExpiredDataAction;
import org.elasticsearch.xpack.ml.action.TransportDeleteFilterAction;
import org.elasticsearch.xpack.ml.action.TransportDeleteForecastAction;
import org.elasticsearch.xpack.ml.action.TransportDeleteJobAction;
import org.elasticsearch.xpack.ml.action.TransportDeleteModelSnapshotAction;
import org.elasticsearch.xpack.ml.action.TransportFinalizeJobExecutionAction;
import org.elasticsearch.xpack.ml.action.TransportFindFileStructureAction;
import org.elasticsearch.xpack.ml.action.TransportFlushJobAction;
import org.elasticsearch.xpack.ml.action.TransportForecastJobAction;
import org.elasticsearch.xpack.ml.action.TransportGetBucketsAction;
import org.elasticsearch.xpack.ml.action.TransportGetCalendarEventsAction;
import org.elasticsearch.xpack.ml.action.TransportGetCalendarsAction;
import org.elasticsearch.xpack.ml.action.TransportGetCategoriesAction;
import org.elasticsearch.xpack.ml.action.TransportGetDatafeedsAction;
import org.elasticsearch.xpack.ml.action.TransportGetDatafeedsStatsAction;
import org.elasticsearch.xpack.ml.action.TransportGetFiltersAction;
import org.elasticsearch.xpack.ml.action.TransportGetInfluencersAction;
import org.elasticsearch.xpack.ml.action.TransportGetJobsAction;
import org.elasticsearch.xpack.ml.action.TransportGetJobsStatsAction;
import org.elasticsearch.xpack.ml.action.TransportGetModelSnapshotsAction;
import org.elasticsearch.xpack.ml.action.TransportGetOverallBucketsAction;
import org.elasticsearch.xpack.ml.action.TransportGetRecordsAction;
import org.elasticsearch.xpack.ml.action.TransportIsolateDatafeedAction;
import org.elasticsearch.xpack.ml.action.TransportKillProcessAction;
import org.elasticsearch.xpack.ml.action.TransportMlInfoAction;
import org.elasticsearch.xpack.ml.action.TransportOpenJobAction;
import org.elasticsearch.xpack.ml.action.TransportPersistJobAction;
import org.elasticsearch.xpack.ml.action.TransportPostCalendarEventsAction;
import org.elasticsearch.xpack.ml.action.TransportPostDataAction;
import org.elasticsearch.xpack.ml.action.TransportPreviewDatafeedAction;
import org.elasticsearch.xpack.ml.action.TransportPutCalendarAction;
import org.elasticsearch.xpack.ml.action.TransportPutDatafeedAction;
import org.elasticsearch.xpack.ml.action.TransportPutFilterAction;
import org.elasticsearch.xpack.ml.action.TransportPutJobAction;
import org.elasticsearch.xpack.ml.action.TransportMlUpgradeAction;
import org.elasticsearch.xpack.ml.action.TransportRevertModelSnapshotAction;
import org.elasticsearch.xpack.ml.action.TransportStartDatafeedAction;
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
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.process.NativeControllerHolder;
import org.elasticsearch.xpack.ml.rest.RestDeleteExpiredDataAction;
import org.elasticsearch.xpack.ml.rest.RestFindFileStructureAction;
import org.elasticsearch.xpack.ml.rest.RestMlInfoAction;
import org.elasticsearch.xpack.ml.rest.calendar.RestDeleteCalendarAction;
import org.elasticsearch.xpack.ml.rest.calendar.RestDeleteCalendarEventAction;
import org.elasticsearch.xpack.ml.rest.calendar.RestDeleteCalendarJobAction;
import org.elasticsearch.xpack.ml.rest.calendar.RestGetCalendarEventsAction;
import org.elasticsearch.xpack.ml.rest.calendar.RestGetCalendarsAction;
import org.elasticsearch.xpack.ml.rest.calendar.RestPostCalendarEventAction;
import org.elasticsearch.xpack.ml.rest.calendar.RestPutCalendarAction;
import org.elasticsearch.xpack.ml.rest.calendar.RestPutCalendarJobAction;
import org.elasticsearch.xpack.ml.rest.datafeeds.RestDeleteDatafeedAction;
import org.elasticsearch.xpack.ml.rest.datafeeds.RestGetDatafeedStatsAction;
import org.elasticsearch.xpack.ml.rest.datafeeds.RestGetDatafeedsAction;
import org.elasticsearch.xpack.ml.rest.datafeeds.RestPreviewDatafeedAction;
import org.elasticsearch.xpack.ml.rest.datafeeds.RestPutDatafeedAction;
import org.elasticsearch.xpack.ml.rest.datafeeds.RestStartDatafeedAction;
import org.elasticsearch.xpack.ml.rest.datafeeds.RestStopDatafeedAction;
import org.elasticsearch.xpack.ml.rest.datafeeds.RestUpdateDatafeedAction;
import org.elasticsearch.xpack.ml.rest.filter.RestDeleteFilterAction;
import org.elasticsearch.xpack.ml.rest.filter.RestGetFiltersAction;
import org.elasticsearch.xpack.ml.rest.filter.RestPutFilterAction;
import org.elasticsearch.xpack.ml.rest.filter.RestUpdateFilterAction;
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
import org.elasticsearch.xpack.ml.rest.results.RestUpgradeMlAction;
import org.elasticsearch.xpack.ml.rest.validate.RestValidateDetectorAction;
import org.elasticsearch.xpack.ml.rest.validate.RestValidateJobConfigAction;

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
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static java.util.Collections.emptyList;

public class MachineLearning extends Plugin implements ActionPlugin, AnalysisPlugin, PersistentTaskPlugin {
    public static final String NAME = "ml";
    public static final String BASE_PATH = "/_ml/";
    public static final String PRE_V7_BASE_PATH = "/_xpack/ml/";
    public static final String DATAFEED_THREAD_POOL_NAME = NAME + "_datafeed";
    public static final String AUTODETECT_THREAD_POOL_NAME = NAME + "_autodetect";
    public static final String UTILITY_THREAD_POOL_NAME = NAME + "_utility";

    // This is for performance testing.  It's not exposed to the end user.
    // Recompile if you want to compare performance with C++ tokenization.
    public static final boolean CATEGORIZATION_TOKENIZATION_IN_JAVA = true;

    public static final Setting<Boolean> ML_ENABLED =
            Setting.boolSetting("node.ml", XPackSettings.MACHINE_LEARNING_ENABLED, Property.NodeScope);
    // This is not used in v7 and higher, but users are still prevented from setting it directly to avoid confusion
    private static final String PRE_V7_ML_ENABLED_NODE_ATTR = "ml.enabled";
    public static final String MAX_OPEN_JOBS_NODE_ATTR = "ml.max_open_jobs";
    public static final String MACHINE_MEMORY_NODE_ATTR = "ml.machine_memory";
    public static final Setting<Integer> CONCURRENT_JOB_ALLOCATIONS =
            Setting.intSetting("xpack.ml.node_concurrent_job_allocations", 2, 0, Property.Dynamic, Property.NodeScope);
    public static final Setting<Integer> MAX_MACHINE_MEMORY_PERCENT =
            Setting.intSetting("xpack.ml.max_machine_memory_percent", 30, 5, 90, Property.Dynamic, Property.NodeScope);
    public static final Setting<Integer> MAX_LAZY_ML_NODES =
            Setting.intSetting("xpack.ml.max_lazy_ml_nodes", 0, 0, 3, Property.Dynamic, Property.NodeScope);

    private static final Logger logger = LogManager.getLogger(XPackPlugin.class);

    private final Settings settings;
    private final Environment env;
    private final boolean enabled;
    private final boolean transportClientMode;

    private final SetOnce<AutodetectProcessManager> autodetectProcessManager = new SetOnce<>();
    private final SetOnce<DatafeedManager> datafeedManager = new SetOnce<>();
    private final SetOnce<MlMemoryTracker> memoryTracker = new SetOnce<>();

    public MachineLearning(Settings settings, Path configPath) {
        this.settings = settings;
        this.enabled = XPackSettings.MACHINE_LEARNING_ENABLED.get(settings);
        this.transportClientMode = XPackPlugin.transportClientMode(settings);
        this.env = transportClientMode ? null : new Environment(settings, configPath);
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
        return Collections.unmodifiableList(
                Arrays.asList(MachineLearningField.AUTODETECT_PROCESS,
                        ML_ENABLED,
                        CONCURRENT_JOB_ALLOCATIONS,
                        MachineLearningField.MAX_MODEL_MEMORY_LIMIT,
                        MAX_LAZY_ML_NODES,
                        MAX_MACHINE_MEMORY_PERCENT,
                        AutodetectBuilder.DONT_PERSIST_MODEL_STATE_SETTING,
                        AutodetectBuilder.MAX_ANOMALY_RECORDS_SETTING_DYNAMIC,
                        AutodetectProcessManager.MAX_OPEN_JOBS_PER_NODE,
                        AutodetectProcessManager.MIN_DISK_SPACE_OFF_HEAP,
                        MlConfigMigrationEligibilityCheck.ENABLE_CONFIG_MIGRATION));
    }

    public Settings additionalSettings() {
        String mlEnabledNodeAttrName = "node.attr." + PRE_V7_ML_ENABLED_NODE_ATTR;
        String maxOpenJobsPerNodeNodeAttrName = "node.attr." + MAX_OPEN_JOBS_NODE_ATTR;
        String machineMemoryAttrName = "node.attr." + MACHINE_MEMORY_NODE_ATTR;

        if (enabled == false || transportClientMode) {
            disallowMlNodeAttributes(mlEnabledNodeAttrName, maxOpenJobsPerNodeNodeAttrName, machineMemoryAttrName);
            return Settings.EMPTY;
        }

        Settings.Builder additionalSettings = Settings.builder();
        Boolean allocationEnabled = ML_ENABLED.get(settings);
        if (allocationEnabled != null && allocationEnabled) {
            addMlNodeAttribute(additionalSettings, maxOpenJobsPerNodeNodeAttrName,
                    String.valueOf(AutodetectProcessManager.MAX_OPEN_JOBS_PER_NODE.get(settings)));
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
        if (enabled == false || transportClientMode) {
            // special holder for @link(MachineLearningFeatureSetUsage) which needs access to job manager, empty if ML is disabled
            return Collections.singletonList(new JobManagerHolder());
        }

        Auditor auditor = new Auditor(client, clusterService.getNodeName());
        JobResultsProvider jobResultsProvider = new JobResultsProvider(client, settings);
        JobConfigProvider jobConfigProvider = new JobConfigProvider(client);
        DatafeedConfigProvider datafeedConfigProvider = new DatafeedConfigProvider(client, xContentRegistry);
        UpdateJobProcessNotifier notifier = new UpdateJobProcessNotifier(client, clusterService, threadPool);
        JobManager jobManager = new JobManager(env, settings, jobResultsProvider, clusterService, auditor, threadPool, client, notifier);

        // special holder for @link(MachineLearningFeatureSetUsage) which needs access to job manager if ML is enabled
        JobManagerHolder jobManagerHolder = new JobManagerHolder(jobManager);

        JobDataCountsPersister jobDataCountsPersister = new JobDataCountsPersister(client);
        JobResultsPersister jobResultsPersister = new JobResultsPersister(client);

        AutodetectProcessFactory autodetectProcessFactory;
        NormalizerProcessFactory normalizerProcessFactory;
        if (MachineLearningField.AUTODETECT_PROCESS.get(settings) && MachineLearningFeatureSet.isRunningOnMlPlatform(true)) {
            try {
                NativeController nativeController = NativeControllerHolder.getNativeController(environment);
                if (nativeController == null) {
                    // This will only only happen when path.home is not set, which is disallowed in production
                    throw new ElasticsearchException("Failed to create native process controller for Machine Learning");
                }
                autodetectProcessFactory = new NativeAutodetectProcessFactory(
                    environment,
                    settings,
                    nativeController,
                    client,
                    clusterService);
                normalizerProcessFactory = new NativeNormalizerProcessFactory(environment, nativeController);
            } catch (IOException e) {
                // This also should not happen in production, as the MachineLearningFeatureSet should have
                // hit the same error first and brought down the node with a friendlier error message
                throw new ElasticsearchException("Failed to create native process factories for Machine Learning", e);
            }
        } else {
            autodetectProcessFactory = (job, autodetectParams, executorService, onProcessCrash) ->
                    new BlackHoleAutodetectProcess(job.getId());
            // factor of 1.0 makes renormalization a no-op
            normalizerProcessFactory = (jobId, quantilesState, bucketSpan, executorService) -> new MultiplyingNormalizerProcess(1.0);
        }
        NormalizerFactory normalizerFactory = new NormalizerFactory(normalizerProcessFactory,
                threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME));
        AutodetectProcessManager autodetectProcessManager = new AutodetectProcessManager(env, settings, client, threadPool,
                jobManager, jobResultsProvider, jobResultsPersister, jobDataCountsPersister, autodetectProcessFactory,
                normalizerFactory, xContentRegistry, auditor);
        this.autodetectProcessManager.set(autodetectProcessManager);
        DatafeedJobBuilder datafeedJobBuilder = new DatafeedJobBuilder(client, settings, xContentRegistry,
                auditor, System::currentTimeMillis);
        DatafeedManager datafeedManager = new DatafeedManager(threadPool, client, clusterService, datafeedJobBuilder,
                System::currentTimeMillis, auditor);
        this.datafeedManager.set(datafeedManager);
        MlLifeCycleService mlLifeCycleService = new MlLifeCycleService(environment, clusterService, datafeedManager,
                autodetectProcessManager);
        MlMemoryTracker memoryTracker = new MlMemoryTracker(settings, clusterService, threadPool, jobManager, jobResultsProvider);
        this.memoryTracker.set(memoryTracker);

        // This object's constructor attaches to the license state, so there's no need to retain another reference to it
        new InvalidLicenseEnforcer(getLicenseState(), threadPool, datafeedManager, autodetectProcessManager);

        // run node startup tasks
        autodetectProcessManager.onNodeStartup();

        return Arrays.asList(
                mlLifeCycleService,
                jobResultsProvider,
                jobConfigProvider,
                datafeedConfigProvider,
                jobManager,
                jobManagerHolder,
                autodetectProcessManager,
                new MlInitializationService(settings, threadPool, clusterService, client),
                jobDataCountsPersister,
                datafeedManager,
                auditor,
                new MlAssignmentNotifier(settings, auditor, threadPool, client, clusterService),
                memoryTracker
        );
    }

    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(ClusterService clusterService,
                                                                       ThreadPool threadPool,
                                                                       Client client,
                                                                       SettingsModule settingsModule) {
        if (enabled == false || transportClientMode) {
            return emptyList();
        }

        return Arrays.asList(
                new TransportOpenJobAction.OpenJobPersistentTasksExecutor(settings, clusterService, autodetectProcessManager.get(),
                    memoryTracker.get(), client),
                new TransportStartDatafeedAction.StartDatafeedPersistentTasksExecutor( datafeedManager.get())
        );
    }

    public Collection<Module> createGuiceModules() {
        List<Module> modules = new ArrayList<>();

        if (transportClientMode) {
            return modules;
        }

        modules.add(b -> {
            XPackPlugin.bindFeatureSet(b, MachineLearningFeatureSet.class);
        });

        return modules;
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
            new RestGetJobsAction(settings, restController),
            new RestGetJobStatsAction(settings, restController),
            new RestMlInfoAction(settings, restController),
            new RestPutJobAction(settings, restController),
            new RestPostJobUpdateAction(settings, restController),
            new RestDeleteJobAction(settings, restController),
            new RestOpenJobAction(settings, restController),
            new RestGetFiltersAction(settings, restController),
            new RestPutFilterAction(settings, restController),
            new RestUpdateFilterAction(settings, restController),
            new RestDeleteFilterAction(settings, restController),
            new RestGetInfluencersAction(settings, restController),
            new RestGetRecordsAction(settings, restController),
            new RestGetBucketsAction(settings, restController),
            new RestGetOverallBucketsAction(settings, restController),
            new RestPostDataAction(settings, restController),
            new RestCloseJobAction(settings, restController),
            new RestFlushJobAction(settings, restController),
            new RestValidateDetectorAction(settings, restController),
            new RestValidateJobConfigAction(settings, restController),
            new RestGetCategoriesAction(settings, restController),
            new RestGetModelSnapshotsAction(settings, restController),
            new RestRevertModelSnapshotAction(settings, restController),
            new RestUpdateModelSnapshotAction(settings, restController),
            new RestGetDatafeedsAction(settings, restController),
            new RestGetDatafeedStatsAction(settings, restController),
            new RestPutDatafeedAction(settings, restController),
            new RestUpdateDatafeedAction(settings, restController),
            new RestDeleteDatafeedAction(settings, restController),
            new RestPreviewDatafeedAction(settings, restController),
            new RestStartDatafeedAction(settings, restController),
            new RestStopDatafeedAction(settings, restController),
            new RestDeleteModelSnapshotAction(settings, restController),
            new RestDeleteExpiredDataAction(settings, restController),
            new RestForecastJobAction(settings, restController),
            new RestDeleteForecastAction(settings, restController),
            new RestGetCalendarsAction(settings, restController),
            new RestPutCalendarAction(settings, restController),
            new RestDeleteCalendarAction(settings, restController),
            new RestDeleteCalendarEventAction(settings, restController),
            new RestDeleteCalendarJobAction(settings, restController),
            new RestPutCalendarJobAction(settings, restController),
            new RestGetCalendarEventsAction(settings, restController),
            new RestPostCalendarEventAction(settings, restController),
            new RestFindFileStructureAction(settings, restController),
            new RestUpgradeMlAction(settings, restController)
        );
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (false == enabled) {
            return emptyList();
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
                new ActionHandler<>(MlUpgradeAction.INSTANCE, TransportMlUpgradeAction.class)
        );
    }
    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        if (false == enabled || transportClientMode) {
            return emptyList();
        }
        int maxNumberOfJobs = AutodetectProcessManager.MAX_OPEN_JOBS_PER_NODE.get(settings);
        // 4 threads per job: for cpp logging, result processing, state processing and
        // AutodetectProcessManager worker thread:
        FixedExecutorBuilder autoDetect = new FixedExecutorBuilder(settings, AUTODETECT_THREAD_POOL_NAME,
                maxNumberOfJobs * 4, maxNumberOfJobs * 4, "xpack.ml.autodetect_thread_pool");

        // 4 threads per job: processing logging, result and state of the renormalization process.
        // Renormalization does't run for the entire lifetime of a job, so additionally autodetect process
        // based operation (open, close, flush, post data), datafeed based operations (start and stop)
        // and deleting expired data use this threadpool too and queue up if all threads are busy.
        FixedExecutorBuilder renormalizer = new FixedExecutorBuilder(settings, UTILITY_THREAD_POOL_NAME,
                maxNumberOfJobs * 4, 500, "xpack.ml.utility_thread_pool");

        // TODO: if datafeed and non datafeed jobs are considered more equal and the datafeed and
        // autodetect process are created at the same time then these two different TPs can merge.
        FixedExecutorBuilder datafeed = new FixedExecutorBuilder(settings, DATAFEED_THREAD_POOL_NAME,
                maxNumberOfJobs, 200, "xpack.ml.datafeed_thread_pool");
        return Arrays.asList(autoDetect, renormalizer, datafeed);
    }

    @Override
    public Map<String, AnalysisProvider<TokenizerFactory>> getTokenizers() {
        return Collections.singletonMap(MlClassicTokenizer.NAME, MlClassicTokenizerFactory::new);
    }
    @Override
    public UnaryOperator<Map<String, IndexTemplateMetaData>> getIndexTemplateMetaDataUpgrader() {
        return templates -> {
            final TimeValue delayedNodeTimeOutSetting;
            // Whether we are using native process is a good way to detect whether we are in dev / test mode:
            if (MachineLearningField.AUTODETECT_PROCESS.get(settings)) {
                delayedNodeTimeOutSetting = UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.get(settings);
            } else {
                delayedNodeTimeOutSetting = TimeValue.timeValueNanos(0);
            }

            try (XContentBuilder auditMapping = ElasticsearchMappings.auditMessageMapping()) {
                IndexTemplateMetaData notificationMessageTemplate = IndexTemplateMetaData.builder(AuditorField.NOTIFICATIONS_INDEX)
                        .putMapping(AuditMessage.TYPE.getPreferredName(), Strings.toString(auditMapping))
                        .patterns(Collections.singletonList(AuditorField.NOTIFICATIONS_INDEX))
                        .version(Version.CURRENT.id)
                        .settings(Settings.builder()
                                // Our indexes are small and one shard puts the
                                // least possible burden on Elasticsearch
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
                                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), delayedNodeTimeOutSetting))
                        .build();
                templates.put(AuditorField.NOTIFICATIONS_INDEX, notificationMessageTemplate);
            } catch (IOException e) {
                logger.warn("Error loading the template for the notification message index", e);
            }

            try (XContentBuilder docMapping = MlMetaIndex.docMapping()) {
                IndexTemplateMetaData metaTemplate = IndexTemplateMetaData.builder(MlMetaIndex.INDEX_NAME)
                        .patterns(Collections.singletonList(MlMetaIndex.INDEX_NAME))
                        .settings(Settings.builder()
                                // Our indexes are small and one shard puts the
                                // least possible burden on Elasticsearch
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
                                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), delayedNodeTimeOutSetting))
                        .version(Version.CURRENT.id)
                        .putMapping(MlMetaIndex.TYPE, Strings.toString(docMapping))
                        .build();
                templates.put(MlMetaIndex.INDEX_NAME, metaTemplate);
            } catch (IOException e) {
                logger.warn("Error loading the template for the " + MlMetaIndex.INDEX_NAME + " index", e);
            }

            try (XContentBuilder configMapping = ElasticsearchMappings.configMapping()) {
                IndexTemplateMetaData configTemplate = IndexTemplateMetaData.builder(AnomalyDetectorsIndex.configIndexName())
                        .patterns(Collections.singletonList(AnomalyDetectorsIndex.configIndexName()))
                        .settings(Settings.builder()
                                // Our indexes are small and one shard puts the
                                // least possible burden on Elasticsearch
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
                                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), delayedNodeTimeOutSetting)
                                .put(IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(),
                                        AnomalyDetectorsIndex.CONFIG_INDEX_MAX_RESULTS_WINDOW))
                        .version(Version.CURRENT.id)
                        .putMapping(ElasticsearchMappings.DOC_TYPE, Strings.toString(configMapping))
                        .build();
                templates.put(AnomalyDetectorsIndex.configIndexName(), configTemplate);
            } catch (IOException e) {
                logger.warn("Error loading the template for the " + AnomalyDetectorsIndex.configIndexName() + " index", e);
            }

            try (XContentBuilder stateMapping = ElasticsearchMappings.stateMapping()) {
                IndexTemplateMetaData stateTemplate = IndexTemplateMetaData.builder(AnomalyDetectorsIndex.jobStateIndexName())
                        .patterns(Collections.singletonList(AnomalyDetectorsIndex.jobStateIndexName()))
                        // TODO review these settings
                        .settings(Settings.builder()
                                .put(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
                                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), delayedNodeTimeOutSetting))
                        .putMapping(ElasticsearchMappings.DOC_TYPE, Strings.toString(stateMapping))
                        .version(Version.CURRENT.id)
                        .build();
                templates.put(AnomalyDetectorsIndex.jobStateIndexName(), stateTemplate);
            } catch (IOException e) {
                logger.error("Error loading the template for the " + AnomalyDetectorsIndex.jobStateIndexName() + " index", e);
            }

            try (XContentBuilder docMapping = ElasticsearchMappings.resultsMapping()) {
                IndexTemplateMetaData jobResultsTemplate = IndexTemplateMetaData.builder(AnomalyDetectorsIndex.jobResultsIndexPrefix())
                        .patterns(Collections.singletonList(AnomalyDetectorsIndex.jobResultsIndexPrefix() + "*"))
                        .settings(Settings.builder()
                                .put(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
                                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), delayedNodeTimeOutSetting)
                                // Sacrifice durability for performance: in the event of power
                                // failure we can lose the last 5 seconds of changes, but it's
                                // much faster
                                .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), "async")
                                // set the default all search field
                                .put(IndexSettings.DEFAULT_FIELD_SETTING.getKey(), ElasticsearchMappings.ALL_FIELD_VALUES))
                        .putMapping(ElasticsearchMappings.DOC_TYPE, Strings.toString(docMapping))
                        .version(Version.CURRENT.id)
                        .build();
                templates.put(AnomalyDetectorsIndex.jobResultsIndexPrefix(), jobResultsTemplate);
            } catch (IOException e) {
                logger.error("Error loading the template for the " + AnomalyDetectorsIndex.jobResultsIndexPrefix() + " indices", e);
            }

            return templates;
        };
    }

    public static boolean allTemplatesInstalled(ClusterState clusterState) {
        boolean allPresent = true;
        List<String> templateNames = Arrays.asList(AuditorField.NOTIFICATIONS_INDEX, MlMetaIndex.INDEX_NAME,
                AnomalyDetectorsIndex.jobStateIndexName(), AnomalyDetectorsIndex.jobResultsIndexPrefix());
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
                        // mem < 0 means the value couldn't be obtained for some reason
                        || (mem < 0 && containerLimit.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) < 0)) {
                    mem = containerLimit.longValue();
                }
            }
        }
        return mem;
    }
}
