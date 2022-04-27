/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.collector.ml;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.collector.Collector;

import java.util.List;

import static org.elasticsearch.xpack.core.ClientHelper.MONITORING_ORIGIN;
import static org.elasticsearch.xpack.core.ml.MachineLearningField.ML_API_FEATURE;
import static org.elasticsearch.xpack.monitoring.collector.TimeoutUtils.ensureNoTimeouts;

/**
 * Collector for Machine Learning Job Stats.
 * <p>
 * This collector runs on the master node because it's the only dependable place that requires it when X-Pack ML is enabled.
 * If ML is not enabled or if it is not allowed to run because of the license, then this will not collect results.
 * <p>
 * Each Job Stats returned is used to create a separate {@link JobStatsMonitoringDoc}.
 */
public class JobStatsCollector extends Collector {

    /**
     * Timeout value when collecting ML job statistics (default to 10s)
     */
    public static final Setting<TimeValue> JOB_STATS_TIMEOUT = collectionTimeoutSetting("ml.job.stats.timeout");

    private final Settings settings;
    private final ThreadContext threadContext;
    private final Client client;

    public JobStatsCollector(
        final Settings settings,
        final ClusterService clusterService,
        final XPackLicenseState licenseState,
        final Client client
    ) {
        this(settings, clusterService, licenseState, client, client.threadPool().getThreadContext());
    }

    JobStatsCollector(
        final Settings settings,
        final ClusterService clusterService,
        final XPackLicenseState licenseState,
        final Client client,
        final ThreadContext threadContext
    ) {
        super(JobStatsMonitoringDoc.TYPE, clusterService, JOB_STATS_TIMEOUT, licenseState);
        this.settings = settings;
        this.client = client;
        this.threadContext = threadContext;
    }

    @Override
    protected boolean shouldCollect(final boolean isElectedMaster) {
        // This can only run when monitoring is allowed + ML is enabled/allowed, but also only on the elected master node
        return isElectedMaster
            && super.shouldCollect(isElectedMaster)
            && XPackSettings.MACHINE_LEARNING_ENABLED.get(settings)
            && ML_API_FEATURE.checkWithoutTracking(licenseState);
    }

    @Override
    protected List<MonitoringDoc> doCollect(final MonitoringDoc.Node node, final long interval, final ClusterState clusterState)
        throws Exception {
        // fetch details about all jobs
        try (ThreadContext.StoredContext ignore = threadContext.stashWithOrigin(MONITORING_ORIGIN)) {
            final GetJobsStatsAction.Request request = new GetJobsStatsAction.Request(Metadata.ALL).setTimeout(getCollectionTimeout());
            final GetJobsStatsAction.Response jobs = client.execute(GetJobsStatsAction.INSTANCE, request).actionGet();

            ensureNoTimeouts(getCollectionTimeout(), jobs);

            final long timestamp = timestamp();
            final String clusterUuid = clusterUuid(clusterState);

            return jobs.getResponse()
                .results()
                .stream()
                .<MonitoringDoc>map(jobStats -> new JobStatsMonitoringDoc(clusterUuid, timestamp, interval, node, jobStats))
                .toList();
        }
    }

}
