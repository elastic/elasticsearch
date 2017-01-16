/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
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
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.ml.action.CloseJobAction;
import org.elasticsearch.xpack.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.ml.action.DeleteListAction;
import org.elasticsearch.xpack.ml.action.DeleteModelSnapshotAction;
import org.elasticsearch.xpack.ml.action.DeleteSchedulerAction;
import org.elasticsearch.xpack.ml.action.FlushJobAction;
import org.elasticsearch.xpack.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.ml.action.GetCategoriesDefinitionAction;
import org.elasticsearch.xpack.ml.action.GetInfluencersAction;
import org.elasticsearch.xpack.ml.action.GetJobsAction;
import org.elasticsearch.xpack.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.ml.action.GetListAction;
import org.elasticsearch.xpack.ml.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.ml.action.GetRecordsAction;
import org.elasticsearch.xpack.ml.action.GetSchedulersAction;
import org.elasticsearch.xpack.ml.action.GetSchedulersStatsAction;
import org.elasticsearch.xpack.ml.action.OpenJobAction;
import org.elasticsearch.xpack.ml.action.PostDataAction;
import org.elasticsearch.xpack.ml.action.PutJobAction;
import org.elasticsearch.xpack.ml.action.PutListAction;
import org.elasticsearch.xpack.ml.action.PutSchedulerAction;
import org.elasticsearch.xpack.ml.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.ml.action.StartSchedulerAction;
import org.elasticsearch.xpack.ml.action.StopSchedulerAction;
import org.elasticsearch.xpack.ml.action.UpdateJobStatusAction;
import org.elasticsearch.xpack.ml.action.UpdateModelSnapshotAction;
import org.elasticsearch.xpack.ml.action.UpdateSchedulerStatusAction;
import org.elasticsearch.xpack.ml.action.ValidateDetectorAction;
import org.elasticsearch.xpack.ml.action.ValidateTransformAction;
import org.elasticsearch.xpack.ml.action.ValidateTransformsAction;
import org.elasticsearch.xpack.ml.job.data.DataProcessor;
import org.elasticsearch.xpack.ml.job.manager.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.job.manager.JobManager;
import org.elasticsearch.xpack.ml.job.metadata.JobAllocator;
import org.elasticsearch.xpack.ml.job.metadata.JobLifeCycleService;
import org.elasticsearch.xpack.ml.job.metadata.MlInitializationService;
import org.elasticsearch.xpack.ml.job.metadata.MlMetadata;
import org.elasticsearch.xpack.ml.job.persistence.JobDataCountsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobDataDeleterFactory;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobRenormalizedResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.process.NativeController;
import org.elasticsearch.xpack.ml.job.process.ProcessCtrl;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessFactory;
import org.elasticsearch.xpack.ml.job.process.autodetect.BlackHoleAutodetectProcess;
import org.elasticsearch.xpack.ml.job.process.autodetect.NativeAutodetectProcessFactory;
import org.elasticsearch.xpack.ml.job.process.autodetect.output.AutodetectResultsParser;
import org.elasticsearch.xpack.ml.job.process.normalizer.MultiplyingNormalizerProcess;
import org.elasticsearch.xpack.ml.job.process.normalizer.NativeNormalizerProcessFactory;
import org.elasticsearch.xpack.ml.job.process.normalizer.NormalizerFactory;
import org.elasticsearch.xpack.ml.job.process.normalizer.NormalizerProcessFactory;
import org.elasticsearch.xpack.ml.job.status.StatusReporter;
import org.elasticsearch.xpack.ml.job.usage.UsageReporter;
import org.elasticsearch.xpack.ml.rest.job.RestCloseJobAction;
import org.elasticsearch.xpack.ml.rest.job.RestDeleteJobAction;
import org.elasticsearch.xpack.ml.rest.job.RestFlushJobAction;
import org.elasticsearch.xpack.ml.rest.job.RestGetJobsAction;
import org.elasticsearch.xpack.ml.rest.job.RestGetJobsStatsAction;
import org.elasticsearch.xpack.ml.rest.job.RestOpenJobAction;
import org.elasticsearch.xpack.ml.rest.job.RestPostDataAction;
import org.elasticsearch.xpack.ml.rest.job.RestPutJobAction;
import org.elasticsearch.xpack.ml.rest.list.RestDeleteListAction;
import org.elasticsearch.xpack.ml.rest.list.RestGetListAction;
import org.elasticsearch.xpack.ml.rest.list.RestPutListAction;
import org.elasticsearch.xpack.ml.rest.modelsnapshots.RestDeleteModelSnapshotAction;
import org.elasticsearch.xpack.ml.rest.modelsnapshots.RestGetModelSnapshotsAction;
import org.elasticsearch.xpack.ml.rest.modelsnapshots.RestRevertModelSnapshotAction;
import org.elasticsearch.xpack.ml.rest.modelsnapshots.RestUpdateModelSnapshotAction;
import org.elasticsearch.xpack.ml.rest.results.RestGetBucketsAction;
import org.elasticsearch.xpack.ml.rest.results.RestGetCategoriesAction;
import org.elasticsearch.xpack.ml.rest.results.RestGetInfluencersAction;
import org.elasticsearch.xpack.ml.rest.results.RestGetRecordsAction;
import org.elasticsearch.xpack.ml.rest.schedulers.RestDeleteSchedulerAction;
import org.elasticsearch.xpack.ml.rest.schedulers.RestGetSchedulersAction;
import org.elasticsearch.xpack.ml.rest.schedulers.RestGetSchedulersStatsAction;
import org.elasticsearch.xpack.ml.rest.schedulers.RestPutSchedulerAction;
import org.elasticsearch.xpack.ml.rest.schedulers.RestStartSchedulerAction;
import org.elasticsearch.xpack.ml.rest.schedulers.RestStopSchedulerAction;
import org.elasticsearch.xpack.ml.rest.validate.RestValidateDetectorAction;
import org.elasticsearch.xpack.ml.rest.validate.RestValidateTransformAction;
import org.elasticsearch.xpack.ml.rest.validate.RestValidateTransformsAction;
import org.elasticsearch.xpack.ml.scheduler.ScheduledJobRunner;
import org.elasticsearch.xpack.ml.utils.NamedPipeHelper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class MlPlugin extends Plugin implements ActionPlugin {
    public static final String NAME = "ml";
    public static final String BASE_PATH = "/_xpack/ml/";
    public static final String THREAD_POOL_NAME = NAME;
    public static final String SCHEDULED_RUNNER_THREAD_POOL_NAME = NAME + "_scheduled_runner";
    public static final String AUTODETECT_PROCESS_THREAD_POOL_NAME = NAME + "_autodetect_process";

    // NORELEASE - temporary solution
    static final Setting<Boolean> USE_NATIVE_PROCESS_OPTION = Setting.boolSetting("useNativeProcess", true, Property.NodeScope,
            Property.Deprecated);

    private final Settings settings;
    private final Environment env;

    public MlPlugin(Settings settings) {
        this.settings = settings;
        this.env = new Environment(settings);
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
        return Collections.singletonList(new NamedWriteableRegistry.Entry(MetaData.Custom.class, "ml", MlMetadata::new));
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        NamedXContentRegistry.Entry entry = new NamedXContentRegistry.Entry(
                MetaData.Custom.class,
                new ParseField("ml"),
                parser -> MlMetadata.ML_METADATA_PARSER.parse(parser, null).build()
        );
        return Collections.singletonList(entry);
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry) {
        JobResultsPersister jobResultsPersister = new JobResultsPersister(settings, client);
        JobProvider jobProvider = new JobProvider(client, 0);
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
                threadPool.executor(MlPlugin.THREAD_POOL_NAME));
        AutodetectResultsParser autodetectResultsParser = new AutodetectResultsParser(settings);
        DataProcessor dataProcessor = new AutodetectProcessManager(settings, client, threadPool, jobManager, jobProvider,
                jobResultsPersister, jobRenormalizedResultsPersister, jobDataCountsPersister, autodetectResultsParser,
                autodetectProcessFactory, normalizerFactory);
        ScheduledJobRunner scheduledJobRunner = new ScheduledJobRunner(threadPool, client, clusterService, jobProvider,
                System::currentTimeMillis);

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
                new MlInitializationService(settings, threadPool, clusterService, jobProvider),
                jobDataCountsPersister,
                scheduledJobRunner
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
        FixedExecutorBuilder ml = new FixedExecutorBuilder(settings, THREAD_POOL_NAME,
                maxNumberOfJobs * 2, 1000, "xpack.ml.thread_pool");

        // fail quick to run autodetect process / scheduler, so no queues
        // 4 threads: for c++ logging, result processing, state processing and restore state
        FixedExecutorBuilder autoDetect = new FixedExecutorBuilder(settings, AUTODETECT_PROCESS_THREAD_POOL_NAME,
                maxNumberOfJobs * 4, 4, "xpack.ml.autodetect_process_thread_pool");

        // TODO: if scheduled and non scheduled jobs are considered more equal and the scheduler and
        // autodetect process are created at the same time then these two different TPs can merge.
        FixedExecutorBuilder scheduler = new FixedExecutorBuilder(settings, SCHEDULED_RUNNER_THREAD_POOL_NAME,
                maxNumberOfJobs, 1, "xpack.ml.scheduler_thread_pool");
        return Arrays.asList(ml, autoDetect, scheduler);
    }
}
