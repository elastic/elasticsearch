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
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchRequestParsers;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.prelert.action.DeleteJobAction;
import org.elasticsearch.xpack.prelert.action.DeleteListAction;
import org.elasticsearch.xpack.prelert.action.DeleteModelSnapshotAction;
import org.elasticsearch.xpack.prelert.action.GetBucketsAction;
import org.elasticsearch.xpack.prelert.action.GetCategoriesDefinitionAction;
import org.elasticsearch.xpack.prelert.action.GetInfluencersAction;
import org.elasticsearch.xpack.prelert.action.GetJobsAction;
import org.elasticsearch.xpack.prelert.action.GetListAction;
import org.elasticsearch.xpack.prelert.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.prelert.action.GetRecordsAction;
import org.elasticsearch.xpack.prelert.action.JobDataAction;
import org.elasticsearch.xpack.prelert.action.CloseJobAction;
import org.elasticsearch.xpack.prelert.action.FlushJobAction;
import org.elasticsearch.xpack.prelert.action.PutJobAction;
import org.elasticsearch.xpack.prelert.action.PutListAction;
import org.elasticsearch.xpack.prelert.action.PutModelSnapshotDescriptionAction;
import org.elasticsearch.xpack.prelert.action.OpenJobAction;
import org.elasticsearch.xpack.prelert.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.prelert.action.StartJobSchedulerAction;
import org.elasticsearch.xpack.prelert.action.StopJobSchedulerAction;
import org.elasticsearch.xpack.prelert.action.UpdateJobSchedulerStatusAction;
import org.elasticsearch.xpack.prelert.action.UpdateJobStatusAction;
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
import org.elasticsearch.xpack.prelert.job.persistence.JobDataDeleterFactory;
import org.elasticsearch.xpack.prelert.job.persistence.JobDataCountsPersister;
import org.elasticsearch.xpack.prelert.job.persistence.JobProvider;
import org.elasticsearch.xpack.prelert.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.prelert.job.process.NativeController;
import org.elasticsearch.xpack.prelert.job.process.ProcessCtrl;
import org.elasticsearch.xpack.prelert.job.process.autodetect.AutodetectProcessFactory;
import org.elasticsearch.xpack.prelert.job.process.autodetect.BlackHoleAutodetectProcess;
import org.elasticsearch.xpack.prelert.job.process.autodetect.NativeAutodetectProcessFactory;
import org.elasticsearch.xpack.prelert.job.process.autodetect.output.AutodetectResultsParser;
import org.elasticsearch.xpack.prelert.job.scheduler.ScheduledJobService;
import org.elasticsearch.xpack.prelert.job.scheduler.http.HttpDataExtractorFactory;
import org.elasticsearch.xpack.prelert.job.status.StatusReporter;
import org.elasticsearch.xpack.prelert.job.usage.UsageReporter;
import org.elasticsearch.xpack.prelert.rest.job.RestJobDataAction;
import org.elasticsearch.xpack.prelert.rest.job.RestCloseJobAction;
import org.elasticsearch.xpack.prelert.rest.job.RestFlushJobAction;
import org.elasticsearch.xpack.prelert.rest.list.RestDeleteListAction;
import org.elasticsearch.xpack.prelert.rest.results.RestGetInfluencersAction;
import org.elasticsearch.xpack.prelert.rest.job.RestDeleteJobAction;
import org.elasticsearch.xpack.prelert.rest.job.RestGetJobsAction;
import org.elasticsearch.xpack.prelert.rest.job.RestPutJobAction;
import org.elasticsearch.xpack.prelert.rest.job.RestOpenJobAction;
import org.elasticsearch.xpack.prelert.rest.list.RestGetListAction;
import org.elasticsearch.xpack.prelert.rest.list.RestPutListAction;
import org.elasticsearch.xpack.prelert.rest.modelsnapshots.RestDeleteModelSnapshotAction;
import org.elasticsearch.xpack.prelert.rest.modelsnapshots.RestGetModelSnapshotsAction;
import org.elasticsearch.xpack.prelert.rest.modelsnapshots.RestPutModelSnapshotDescriptionAction;
import org.elasticsearch.xpack.prelert.rest.modelsnapshots.RestRevertModelSnapshotAction;
import org.elasticsearch.xpack.prelert.rest.results.RestGetBucketsAction;
import org.elasticsearch.xpack.prelert.rest.results.RestGetCategoriesAction;
import org.elasticsearch.xpack.prelert.rest.results.RestGetRecordsAction;
import org.elasticsearch.xpack.prelert.rest.schedulers.RestStartJobSchedulerAction;
import org.elasticsearch.xpack.prelert.rest.schedulers.RestStopJobSchedulerAction;
import org.elasticsearch.xpack.prelert.rest.validate.RestValidateDetectorAction;
import org.elasticsearch.xpack.prelert.rest.validate.RestValidateTransformAction;
import org.elasticsearch.xpack.prelert.rest.validate.RestValidateTransformsAction;
import org.elasticsearch.xpack.prelert.utils.NamedPipeHelper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class PrelertPlugin extends Plugin implements ActionPlugin {
    public static final String NAME = "prelert";
    public static final String BASE_PATH = "/_xpack/prelert/";
    public static final String THREAD_POOL_NAME = NAME;
    public static final String SCHEDULER_THREAD_POOL_NAME = NAME + "_scheduler";
    public static final String AUTODETECT_PROCESS_THREAD_POOL_NAME = NAME + "_autodetect_process";

    // NORELEASE - temporary solution
    static final Setting<Boolean> USE_NATIVE_PROCESS_OPTION = Setting.boolSetting("useNativeProcess", false, Property.NodeScope,
            Property.Deprecated);

    private final Settings settings;
    private final Environment env;

    private final ParseFieldMatcherSupplier parseFieldMatcherSupplier;

    static {
        MetaData.registerPrototype(PrelertMetadata.TYPE, PrelertMetadata.PROTO);
    }

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
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService, ScriptService scriptService,
            SearchRequestParsers searchRequestParsers) {

        JobResultsPersister jobResultsPersister = new JobResultsPersister(settings, client);
        JobProvider jobProvider = new JobProvider(client, 0, parseFieldMatcherSupplier.getParseFieldMatcher());
        JobDataCountsPersister jobDataCountsPersister = new JobDataCountsPersister(settings, client);

        JobManager jobManager = new JobManager(settings, jobProvider, jobResultsPersister, clusterService);
        AutodetectProcessFactory processFactory;
        if (USE_NATIVE_PROCESS_OPTION.get(settings)) {
            try {
                NativeController nativeController = new NativeController(env, new NamedPipeHelper());
                nativeController.tailLogsInThread();
                processFactory = new NativeAutodetectProcessFactory(jobProvider, env, settings, nativeController);
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to create native process factory", e);
            }
        } else {
            processFactory = (JobDetails, ignoreDowntime, executorService) -> new BlackHoleAutodetectProcess();
        }
        AutodetectResultsParser autodetectResultsParser = new AutodetectResultsParser(settings, parseFieldMatcherSupplier);
        DataProcessor dataProcessor = new AutodetectProcessManager(settings, client, threadPool, jobManager, jobProvider,
                jobResultsPersister, jobDataCountsPersister, autodetectResultsParser, processFactory, clusterService.getClusterSettings());
        ScheduledJobService scheduledJobService = new ScheduledJobService(threadPool, client, jobProvider, dataProcessor,
                // norelease: we will no longer need to pass the client here after we switch to a client based data extractor
                new HttpDataExtractorFactory(client),
                System::currentTimeMillis);
        return Arrays.asList(
                jobProvider,
                jobManager,
                new JobAllocator(settings, clusterService, threadPool),
                new JobLifeCycleService(settings, client, clusterService, scheduledJobService, dataProcessor, threadPool.generic()),
                new JobDataDeleterFactory(client), //NORELEASE: this should use Delete-by-query
                dataProcessor,
                new PrelertInitializationService(settings, threadPool, clusterService, jobProvider),
                jobDataCountsPersister
                );
    }

    @Override
    public List<Class<? extends RestHandler>> getRestHandlers() {
        return Arrays.asList(
                RestGetJobsAction.class,
                RestPutJobAction.class,
                RestDeleteJobAction.class,
                RestOpenJobAction.class,
                RestGetListAction.class,
                RestPutListAction.class,
                RestDeleteListAction.class,
                RestGetInfluencersAction.class,
                RestGetRecordsAction.class,
                RestGetBucketsAction.class,
                RestJobDataAction.class,
                RestCloseJobAction.class,
                RestFlushJobAction.class,
                RestValidateDetectorAction.class,
                RestValidateTransformAction.class,
                RestValidateTransformsAction.class,
                RestGetCategoriesAction.class,
                RestGetModelSnapshotsAction.class,
                RestRevertModelSnapshotAction.class,
                RestPutModelSnapshotDescriptionAction.class,
                RestStartJobSchedulerAction.class,
                RestStopJobSchedulerAction.class,
                RestDeleteModelSnapshotAction.class
                );
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
                new ActionHandler<>(GetJobsAction.INSTANCE, GetJobsAction.TransportAction.class),
                new ActionHandler<>(PutJobAction.INSTANCE, PutJobAction.TransportAction.class),
                new ActionHandler<>(DeleteJobAction.INSTANCE, DeleteJobAction.TransportAction.class),
                new ActionHandler<>(OpenJobAction.INSTANCE, OpenJobAction.TransportAction.class),
                new ActionHandler<>(UpdateJobStatusAction.INSTANCE, UpdateJobStatusAction.TransportAction.class),
                new ActionHandler<>(UpdateJobSchedulerStatusAction.INSTANCE, UpdateJobSchedulerStatusAction.TransportAction.class),
                new ActionHandler<>(GetListAction.INSTANCE, GetListAction.TransportAction.class),
                new ActionHandler<>(PutListAction.INSTANCE, PutListAction.TransportAction.class),
                new ActionHandler<>(DeleteListAction.INSTANCE, DeleteListAction.TransportAction.class),
                new ActionHandler<>(GetBucketsAction.INSTANCE, GetBucketsAction.TransportAction.class),
                new ActionHandler<>(GetInfluencersAction.INSTANCE, GetInfluencersAction.TransportAction.class),
                new ActionHandler<>(GetRecordsAction.INSTANCE, GetRecordsAction.TransportAction.class),
                new ActionHandler<>(JobDataAction.INSTANCE, JobDataAction.TransportAction.class),
                new ActionHandler<>(CloseJobAction.INSTANCE, CloseJobAction.TransportAction.class),
                new ActionHandler<>(FlushJobAction.INSTANCE, FlushJobAction.TransportAction.class),
                new ActionHandler<>(ValidateDetectorAction.INSTANCE, ValidateDetectorAction.TransportAction.class),
                new ActionHandler<>(ValidateTransformAction.INSTANCE, ValidateTransformAction.TransportAction.class),
                new ActionHandler<>(ValidateTransformsAction.INSTANCE, ValidateTransformsAction.TransportAction.class),
                new ActionHandler<>(GetCategoriesDefinitionAction.INSTANCE, GetCategoriesDefinitionAction.TransportAction.class),
                new ActionHandler<>(GetModelSnapshotsAction.INSTANCE, GetModelSnapshotsAction.TransportAction.class),
                new ActionHandler<>(RevertModelSnapshotAction.INSTANCE, RevertModelSnapshotAction.TransportAction.class),
                new ActionHandler<>(PutModelSnapshotDescriptionAction.INSTANCE, PutModelSnapshotDescriptionAction.TransportAction.class),
                new ActionHandler<>(StartJobSchedulerAction.INSTANCE, StartJobSchedulerAction.TransportAction.class),
                new ActionHandler<>(StopJobSchedulerAction.INSTANCE, StopJobSchedulerAction.TransportAction.class),
                new ActionHandler<>(DeleteModelSnapshotAction.INSTANCE, DeleteModelSnapshotAction.TransportAction.class)
                );
    }

    public static Path resolveConfigFile(Environment env, String name) {
        return env.configFile().resolve(NAME).resolve(name);
    }

    public static Path resolveLogFile(Environment env, String name) {
        return env.logsFile().resolve(NAME).resolve(name);
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        int maxNumberOfJobs = AutodetectProcessManager.MAX_RUNNING_JOBS_PER_NODE.get(settings);
        FixedExecutorBuilder prelert = new FixedExecutorBuilder(settings, THREAD_POOL_NAME,
                maxNumberOfJobs, 1000, "xpack.prelert.thread_pool");

        // fail quick to start autodetect process / scheduler, so no queues
        // 4 threads: for c++ logging, result processing, state processing and restore state
        FixedExecutorBuilder autoDetect = new FixedExecutorBuilder(settings, AUTODETECT_PROCESS_THREAD_POOL_NAME,
                maxNumberOfJobs * 4, 4, "xpack.prelert.autodetect_process_thread_pool");

        // TODO: if scheduled and non scheduled jobs are considered more equal and the scheduler and
        // autodetect process are created at the same time then these two different TPs can merge.
        FixedExecutorBuilder scheduler = new FixedExecutorBuilder(settings, SCHEDULER_THREAD_POOL_NAME,
                maxNumberOfJobs, 1, "xpack.prelert.scheduler_thread_pool");
        return Arrays.asList(prelert, autoDetect, scheduler);
    }
}
