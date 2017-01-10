/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchRequestParsers;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.prelert.action.CloseJobAction;
import org.elasticsearch.xpack.prelert.action.DeleteJobAction;
import org.elasticsearch.xpack.prelert.action.DeleteListAction;
import org.elasticsearch.xpack.prelert.action.DeleteModelSnapshotAction;
import org.elasticsearch.xpack.prelert.action.DeleteSchedulerAction;
import org.elasticsearch.xpack.prelert.action.FlushJobAction;
import org.elasticsearch.xpack.prelert.action.GetBucketsAction;
import org.elasticsearch.xpack.prelert.action.GetCategoriesDefinitionAction;
import org.elasticsearch.xpack.prelert.action.GetInfluencersAction;
import org.elasticsearch.xpack.prelert.action.GetJobsAction;
import org.elasticsearch.xpack.prelert.action.GetJobsStatsAction;
import org.elasticsearch.xpack.prelert.action.GetListAction;
import org.elasticsearch.xpack.prelert.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.prelert.action.GetRecordsAction;
import org.elasticsearch.xpack.prelert.action.GetSchedulersAction;
import org.elasticsearch.xpack.prelert.action.GetSchedulersStatsAction;
import org.elasticsearch.xpack.prelert.action.OpenJobAction;
import org.elasticsearch.xpack.prelert.action.PostDataAction;
import org.elasticsearch.xpack.prelert.action.PutJobAction;
import org.elasticsearch.xpack.prelert.action.PutListAction;
import org.elasticsearch.xpack.prelert.action.PutSchedulerAction;
import org.elasticsearch.xpack.prelert.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.prelert.action.StartSchedulerAction;
import org.elasticsearch.xpack.prelert.action.StopSchedulerAction;
import org.elasticsearch.xpack.prelert.action.UpdateJobStatusAction;
import org.elasticsearch.xpack.prelert.action.UpdateModelSnapshotAction;
import org.elasticsearch.xpack.prelert.action.UpdateSchedulerStatusAction;
import org.elasticsearch.xpack.prelert.action.ValidateDetectorAction;
import org.elasticsearch.xpack.prelert.action.ValidateTransformAction;
import org.elasticsearch.xpack.prelert.action.ValidateTransformsAction;
import org.elasticsearch.xpack.prelert.job.data.DataProcessor;
import org.elasticsearch.xpack.prelert.job.manager.AutodetectProcessManager;
import org.elasticsearch.xpack.prelert.job.manager.JobManager;
import org.elasticsearch.xpack.prelert.job.metadata.JobAllocator;
import org.elasticsearch.xpack.prelert.job.metadata.JobLifeCycleService;
import org.elasticsearch.xpack.prelert.job.metadata.PrelertInitializationService;
import org.elasticsearch.xpack.prelert.job.metadata.PrelertMetadata;
import org.elasticsearch.xpack.prelert.job.persistence.JobDataCountsPersister;
import org.elasticsearch.xpack.prelert.job.persistence.JobDataDeleterFactory;
import org.elasticsearch.xpack.prelert.job.persistence.JobProvider;
import org.elasticsearch.xpack.prelert.job.persistence.JobRenormalizedResultsPersister;
import org.elasticsearch.xpack.prelert.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.prelert.job.process.NativeController;
import org.elasticsearch.xpack.prelert.job.process.ProcessCtrl;
import org.elasticsearch.xpack.prelert.job.process.autodetect.AutodetectProcessFactory;
import org.elasticsearch.xpack.prelert.job.process.autodetect.BlackHoleAutodetectProcess;
import org.elasticsearch.xpack.prelert.job.process.autodetect.NativeAutodetectProcessFactory;
import org.elasticsearch.xpack.prelert.job.process.autodetect.output.AutodetectResultsParser;
import org.elasticsearch.xpack.prelert.job.process.normalizer.MultiplyingNormalizerProcess;
import org.elasticsearch.xpack.prelert.job.process.normalizer.NativeNormalizerProcessFactory;
import org.elasticsearch.xpack.prelert.job.process.normalizer.NormalizerFactory;
import org.elasticsearch.xpack.prelert.job.process.normalizer.NormalizerProcessFactory;
import org.elasticsearch.xpack.prelert.job.status.StatusReporter;
import org.elasticsearch.xpack.prelert.job.usage.UsageReporter;
import org.elasticsearch.xpack.prelert.rest.job.RestCloseJobAction;
import org.elasticsearch.xpack.prelert.rest.job.RestDeleteJobAction;
import org.elasticsearch.xpack.prelert.rest.job.RestFlushJobAction;
import org.elasticsearch.xpack.prelert.rest.job.RestGetJobsAction;
import org.elasticsearch.xpack.prelert.rest.job.RestGetJobsStatsAction;
import org.elasticsearch.xpack.prelert.rest.job.RestOpenJobAction;
import org.elasticsearch.xpack.prelert.rest.job.RestPostDataAction;
import org.elasticsearch.xpack.prelert.rest.job.RestPutJobAction;
import org.elasticsearch.xpack.prelert.rest.list.RestDeleteListAction;
import org.elasticsearch.xpack.prelert.rest.list.RestGetListAction;
import org.elasticsearch.xpack.prelert.rest.list.RestPutListAction;
import org.elasticsearch.xpack.prelert.rest.modelsnapshots.RestDeleteModelSnapshotAction;
import org.elasticsearch.xpack.prelert.rest.modelsnapshots.RestGetModelSnapshotsAction;
import org.elasticsearch.xpack.prelert.rest.modelsnapshots.RestRevertModelSnapshotAction;
import org.elasticsearch.xpack.prelert.rest.modelsnapshots.RestUpdateModelSnapshotAction;
import org.elasticsearch.xpack.prelert.rest.results.RestGetBucketsAction;
import org.elasticsearch.xpack.prelert.rest.results.RestGetCategoriesAction;
import org.elasticsearch.xpack.prelert.rest.results.RestGetInfluencersAction;
import org.elasticsearch.xpack.prelert.rest.results.RestGetRecordsAction;
import org.elasticsearch.xpack.prelert.rest.schedulers.RestDeleteSchedulerAction;
import org.elasticsearch.xpack.prelert.rest.schedulers.RestGetSchedulersAction;
import org.elasticsearch.xpack.prelert.rest.schedulers.RestGetSchedulersStatsAction;
import org.elasticsearch.xpack.prelert.rest.schedulers.RestPutSchedulerAction;
import org.elasticsearch.xpack.prelert.rest.schedulers.RestStartSchedulerAction;
import org.elasticsearch.xpack.prelert.rest.schedulers.RestStopSchedulerAction;
import org.elasticsearch.xpack.prelert.rest.validate.RestValidateDetectorAction;
import org.elasticsearch.xpack.prelert.rest.validate.RestValidateTransformAction;
import org.elasticsearch.xpack.prelert.rest.validate.RestValidateTransformsAction;
import org.elasticsearch.xpack.prelert.scheduler.ScheduledJobRunner;
import org.elasticsearch.xpack.prelert.utils.NamedPipeHelper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class PrelertPlugin extends Plugin implements ActionPlugin {
    public static final String NAME = "prelert";
    public static final String BASE_PATH = "/_xpack/ml/";
    public static final String THREAD_POOL_NAME = NAME;
    public static final String SCHEDULED_RUNNER_THREAD_POOL_NAME = NAME + "_scheduled_runner";
    public static final String AUTODETECT_PROCESS_THREAD_POOL_NAME = NAME + "_autodetect_process";

    // NORELEASE - temporary solution
    static final Setting<Boolean> USE_NATIVE_PROCESS_OPTION = Setting.boolSetting("useNativeProcess", true, Property.NodeScope,
            Property.Deprecated);

    private final Settings settings;
    private final Environment env;

    private final ParseFieldMatcherSupplier parseFieldMatcherSupplier;

    public PrelertPlugin(Settings settings) {
        this.settings = settings;
        this.env = new Environment(settings);
        ParseFieldMatcher matcher = new ParseFieldMatcher(settings);
        parseFieldMatcherSupplier = () -> matcher;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Collections.unmodifiableList(
                Arrays.asList(USE_NATIVE_PROCESS_OPTION,
                        ProcessCtrl.DONT_PERSIST_MODEL_STATE_SETTING,
                        ProcessCtrl.MAX_ANOMALY_RECORDS_SETTING,
                        StatusReporter.ACCEPTABLE_PERCENTAGE_DATE_PARSE_ERRORS_SETTING,
                        StatusReporter.ACCEPTABLE_PERCENTAGE_OUT_OF_ORDER_ERRORS_SETTING,
                        UsageReporter.UPDATE_INTERVAL_SETTING,
                        AutodetectProcessManager.MAX_RUNNING_JOBS_PER_NODE));
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Collections.singletonList(new NamedWriteableRegistry.Entry(MetaData.Custom.class, "prelert", PrelertMetadata::new));
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        NamedXContentRegistry.Entry entry = new NamedXContentRegistry.Entry(
                MetaData.Custom.class,
                new ParseField("prelert"),
                parser -> PrelertMetadata.PRELERT_METADATA_PARSER.parse(parser, parseFieldMatcherSupplier).build()
        );
        return Collections.singletonList(entry);
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               SearchRequestParsers searchRequestParsers, NamedXContentRegistry xContentRegistry) {
        JobResultsPersister jobResultsPersister = new JobResultsPersister(settings, client);
        JobProvider jobProvider = new JobProvider(client, 0, parseFieldMatcherSupplier.getParseFieldMatcher());
        JobRenormalizedResultsPersister jobRenormalizedResultsPersister = new JobRenormalizedResultsPersister(settings,
                jobResultsPersister);
        JobDataCountsPersister jobDataCountsPersister = new JobDataCountsPersister(settings, client);

        JobManager jobManager = new JobManager(settings, jobProvider, jobResultsPersister, clusterService);
        AutodetectProcessFactory autodetectProcessFactory;
        NormalizerProcessFactory normalizerProcessFactory;
        if (USE_NATIVE_PROCESS_OPTION.get(settings)) {
            try {
                NativeController nativeController = new NativeController(env, new NamedPipeHelper());
                nativeController.tailLogsInThread();
                autodetectProcessFactory = new NativeAutodetectProcessFactory(jobProvider, env, settings, nativeController);
                normalizerProcessFactory = new NativeNormalizerProcessFactory(env, settings, nativeController);
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to create native process factories", e);
            }
        } else {
            autodetectProcessFactory = (jobDetails, ignoreDowntime, executorService) -> new BlackHoleAutodetectProcess();
            // factor of 1.0 makes renormalization a no-op
            normalizerProcessFactory = (jobId, quantilesState, bucketSpan, perPartitionNormalization,
                                        executorService) -> new MultiplyingNormalizerProcess(settings, 1.0);
        }
        NormalizerFactory normalizerFactory = new NormalizerFactory(normalizerProcessFactory,
                threadPool.executor(PrelertPlugin.THREAD_POOL_NAME));
        AutodetectResultsParser autodetectResultsParser = new AutodetectResultsParser(settings, parseFieldMatcherSupplier);
        DataProcessor dataProcessor = new AutodetectProcessManager(settings, client, threadPool, jobManager, jobProvider,
                jobResultsPersister, jobRenormalizedResultsPersister, jobDataCountsPersister, autodetectResultsParser,
                autodetectProcessFactory, normalizerFactory);
        ScheduledJobRunner scheduledJobRunner = new ScheduledJobRunner(threadPool, client, clusterService, jobProvider,
                System::currentTimeMillis);
        TaskManager taskManager = new TaskManager(settings);

        JobLifeCycleService jobLifeCycleService =
                new JobLifeCycleService(settings, client, clusterService, dataProcessor, threadPool.generic());
        // we hop on the lifecycle service of ResourceWatcherService, because
        // that one is stopped before discovery is.
        // (when discovery is stopped it will send a leave request to elected master node, which will then be removed
        // from the cluster state, which then triggers other events)
        resourceWatcherService.addLifecycleListener(new LifecycleListener() {

            @Override
            public void beforeStop() {
                jobLifeCycleService.stop();
            }
        });

        return Arrays.asList(
                jobProvider,
                jobManager,
                new JobAllocator(settings, clusterService, threadPool),
                jobLifeCycleService,
                new JobDataDeleterFactory(client), //NORELEASE: this should use Delete-by-query
                dataProcessor,
                new PrelertInitializationService(settings, threadPool, clusterService, jobProvider),
                jobDataCountsPersister,
                scheduledJobRunner,
                taskManager
                );
    }

    @Override
    public List<Class<? extends RestHandler>> getRestHandlers() {
        return Arrays.asList(
                RestGetJobsAction.class,
                RestGetJobsStatsAction.class,
                RestPutJobAction.class,
                RestDeleteJobAction.class,
                RestOpenJobAction.class,
                RestGetListAction.class,
                RestPutListAction.class,
                RestDeleteListAction.class,
                RestGetInfluencersAction.class,
                RestGetRecordsAction.class,
                RestGetBucketsAction.class,
                RestPostDataAction.class,
                RestCloseJobAction.class,
                RestFlushJobAction.class,
                RestValidateDetectorAction.class,
                RestValidateTransformAction.class,
                RestValidateTransformsAction.class,
                RestGetCategoriesAction.class,
                RestGetModelSnapshotsAction.class,
                RestRevertModelSnapshotAction.class,
                RestUpdateModelSnapshotAction.class,
                RestGetSchedulersAction.class,
                RestGetSchedulersStatsAction.class,
                RestPutSchedulerAction.class,
                RestDeleteSchedulerAction.class,
                RestStartSchedulerAction.class,
                RestStopSchedulerAction.class,
                RestDeleteModelSnapshotAction.class
                );
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
                new ActionHandler<>(GetJobsAction.INSTANCE, GetJobsAction.TransportAction.class),
                new ActionHandler<>(GetJobsStatsAction.INSTANCE, GetJobsStatsAction.TransportAction.class),
                new ActionHandler<>(PutJobAction.INSTANCE, PutJobAction.TransportAction.class),
                new ActionHandler<>(DeleteJobAction.INSTANCE, DeleteJobAction.TransportAction.class),
                new ActionHandler<>(OpenJobAction.INSTANCE, OpenJobAction.TransportAction.class),
                new ActionHandler<>(UpdateJobStatusAction.INSTANCE, UpdateJobStatusAction.TransportAction.class),
                new ActionHandler<>(UpdateSchedulerStatusAction.INSTANCE, UpdateSchedulerStatusAction.TransportAction.class),
                new ActionHandler<>(GetListAction.INSTANCE, GetListAction.TransportAction.class),
                new ActionHandler<>(PutListAction.INSTANCE, PutListAction.TransportAction.class),
                new ActionHandler<>(DeleteListAction.INSTANCE, DeleteListAction.TransportAction.class),
                new ActionHandler<>(GetBucketsAction.INSTANCE, GetBucketsAction.TransportAction.class),
                new ActionHandler<>(GetInfluencersAction.INSTANCE, GetInfluencersAction.TransportAction.class),
                new ActionHandler<>(GetRecordsAction.INSTANCE, GetRecordsAction.TransportAction.class),
                new ActionHandler<>(PostDataAction.INSTANCE, PostDataAction.TransportAction.class),
                new ActionHandler<>(CloseJobAction.INSTANCE, CloseJobAction.TransportAction.class),
                new ActionHandler<>(FlushJobAction.INSTANCE, FlushJobAction.TransportAction.class),
                new ActionHandler<>(ValidateDetectorAction.INSTANCE, ValidateDetectorAction.TransportAction.class),
                new ActionHandler<>(ValidateTransformAction.INSTANCE, ValidateTransformAction.TransportAction.class),
                new ActionHandler<>(ValidateTransformsAction.INSTANCE, ValidateTransformsAction.TransportAction.class),
                new ActionHandler<>(GetCategoriesDefinitionAction.INSTANCE, GetCategoriesDefinitionAction.TransportAction.class),
                new ActionHandler<>(GetModelSnapshotsAction.INSTANCE, GetModelSnapshotsAction.TransportAction.class),
                new ActionHandler<>(RevertModelSnapshotAction.INSTANCE, RevertModelSnapshotAction.TransportAction.class),
                new ActionHandler<>(UpdateModelSnapshotAction.INSTANCE, UpdateModelSnapshotAction.TransportAction.class),
                new ActionHandler<>(GetSchedulersAction.INSTANCE, GetSchedulersAction.TransportAction.class),
                new ActionHandler<>(GetSchedulersStatsAction.INSTANCE, GetSchedulersStatsAction.TransportAction.class),
                new ActionHandler<>(PutSchedulerAction.INSTANCE, PutSchedulerAction.TransportAction.class),
                new ActionHandler<>(DeleteSchedulerAction.INSTANCE, DeleteSchedulerAction.TransportAction.class),
                new ActionHandler<>(StartSchedulerAction.INSTANCE, StartSchedulerAction.TransportAction.class),
                new ActionHandler<>(StopSchedulerAction.INSTANCE, StopSchedulerAction.TransportAction.class),
                new ActionHandler<>(DeleteModelSnapshotAction.INSTANCE, DeleteModelSnapshotAction.TransportAction.class)
                );
    }

    public static Path resolveConfigFile(Environment env, String name) {
        return env.configFile().resolve(NAME).resolve(name);
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        int maxNumberOfJobs = AutodetectProcessManager.MAX_RUNNING_JOBS_PER_NODE.get(settings);
        FixedExecutorBuilder prelert = new FixedExecutorBuilder(settings, THREAD_POOL_NAME,
                maxNumberOfJobs * 2, 1000, "xpack.prelert.thread_pool");

        // fail quick to run autodetect process / scheduler, so no queues
        // 4 threads: for c++ logging, result processing, state processing and restore state
        FixedExecutorBuilder autoDetect = new FixedExecutorBuilder(settings, AUTODETECT_PROCESS_THREAD_POOL_NAME,
                maxNumberOfJobs * 4, 4, "xpack.prelert.autodetect_process_thread_pool");

        // TODO: if scheduled and non scheduled jobs are considered more equal and the scheduler and
        // autodetect process are created at the same time then these two different TPs can merge.
        FixedExecutorBuilder scheduler = new FixedExecutorBuilder(settings, SCHEDULED_RUNNER_THREAD_POOL_NAME,
                maxNumberOfJobs, 1, "xpack.prelert.scheduler_thread_pool");
        return Arrays.asList(prelert, autoDetect, scheduler);
    }
}
