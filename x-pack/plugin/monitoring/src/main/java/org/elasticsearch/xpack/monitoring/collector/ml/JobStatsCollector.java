/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.ml;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.collector.Collector;

import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.MONITORING_ORIGIN;

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

    public JobStatsCollector(final Settings settings, final ClusterService clusterService,
                             final XPackLicenseState licenseState, final Client client) {
        this(settings, clusterService, licenseState, client, client.threadPool().getThreadContext());
    }

    JobStatsCollector(final Settings settings, final ClusterService clusterService,
                      final XPackLicenseState licenseState, final Client client, final ThreadContext threadContext) {
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
                && licenseState.isMachineLearningAllowed();
    }

    @Override
    protected List<MonitoringDoc> doCollect(final MonitoringDoc.Node node,
                                            final long interval,
                                            final ClusterState clusterState) throws Exception {
        // fetch details about all jobs
        try (ThreadContext.StoredContext ignore = threadContext.stashWithOrigin(MONITORING_ORIGIN)) {
            final GetJobsStatsAction.Response jobs =
                    client.execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(MetaData.ALL))
                            .actionGet(getCollectionTimeout());

            final long timestamp = timestamp();
            final String clusterUuid = clusterUuid(clusterState);

            return jobs.getResponse().results().stream()
                    .map(jobStats -> new JobStatsMonitoringDoc(clusterUuid, timestamp, interval, node, jobStats))
                    .collect(Collectors.toList());
        }
    }

}
