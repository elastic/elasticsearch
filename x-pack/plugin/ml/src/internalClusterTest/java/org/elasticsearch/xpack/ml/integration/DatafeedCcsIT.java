/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction.Response.JobStats;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.ChunkingConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.elasticsearch.xpack.ml.LocalStateMachineLearning;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;
import org.elasticsearch.xpack.wildcard.Wildcard;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;

public class DatafeedCcsIT extends AbstractMultiClustersTestCase {

    private static final String REMOTE_CLUSTER = "remote_cluster";
    private static final String DATA_INDEX = "data";

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
            // For an internal cluster test we have to use the "black hole" autodetect process - we cannot use the native one
            .put(MachineLearningField.AUTODETECT_PROCESS.getKey(), false)
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .put(XPackSettings.WATCHER_ENABLED.getKey(), false)
            .put(XPackSettings.GRAPH_ENABLED.getKey(), false)
            .put(LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED_SETTING.getKey(), false)
            // Default the watermarks to absurdly low to prevent the tests from failing on nodes without enough disk space
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "1b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "1b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "1b")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return List.of(
            LocalStateMachineLearning.class,
            CommonAnalysisPlugin.class,
            IngestCommonPlugin.class,
            ReindexPlugin.class,
            ShutdownPlugin.class,
            // To remove warnings about painless not being supported
            BaseMlIntegTestCase.MockPainlessScriptEngine.TestPlugin.class,
            // ILM is required for .ml-state template index settings
            IndexLifecycle.class,
            // Deprecation warnings go to a data stream, if we ever cause a deprecation warning the data streams plugin is required
            DataStreamsPlugin.class,
            // To remove errors from parsing built in templates that contain scaled_float or wildcard
            MapperExtrasPlugin.class,
            Wildcard.class
        );
    }

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    public void testDatafeedWithCcsRemoteHealthy() throws Exception {
        setSkipUnavailable(randomBoolean());
        String jobId = "ccs-healthy-job";
        String datafeedId = jobId;
        long numDocs = randomIntBetween(32, 2048);
        long endTimeMs = indexRemoteDocs(numDocs);
        setupJobAndDatafeed(jobId, datafeedId, endTimeMs);
        try {
            // Datafeed should complete and auto-close the job.
            // Use a 3 minute timeout because multiple suites run in parallel in CI which slows things down a lot.
            // (Usually the test completes within 1 minute and much faster than that if run locally with nothing major running in parallel.)
            assertBusy(() -> {
                JobStats jobStats = getJobStats(jobId);
                assertThat(jobStats.getState(), is(JobState.CLOSED));
                assertThat(jobStats.getDataCounts().getProcessedRecordCount(), is(numDocs));
            }, 3, TimeUnit.MINUTES);
        } catch (AssertionError ae) {
            // On failure close the job, because otherwise there will be masses of noise in the logs from the job fighting with the
            // post-test cleanup which obscures the original failure. Force closing the job also stops the datafeed if necessary.
            try {
                client(LOCAL_CLUSTER).execute(CloseJobAction.INSTANCE, new CloseJobAction.Request(jobId).setForce(true)).actionGet();
            } catch (Exception e) {
                ae.addSuppressed(e);
            }
            throw ae;
        } finally {
            clearSkipUnavailable();
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/84268")
    public void testDatafeedWithCcsRemoteUnavailable() throws Exception {
        setSkipUnavailable(randomBoolean());
        String jobId = "ccs-unavailable-job";
        String datafeedId = jobId;
        long numDocs = randomIntBetween(32, 2048);
        indexRemoteDocs(numDocs);
        setupJobAndDatafeed(jobId, datafeedId, null);
        try {
            NetworkDisruption networkDisruption = new NetworkDisruption(
                new NetworkDisruption.IsolateAllNodes(Set.of(cluster(REMOTE_CLUSTER).getNodeNames())),
                NetworkDisruption.DISCONNECT
            );
            cluster(REMOTE_CLUSTER).setDisruptionScheme(networkDisruption);
            networkDisruption.startDisrupting();
            // Wait until the datafeed suffers from the disruption OR processes all the documents.
            // (Sometimes this test won't actually test the desired functionality, as it's possible
            // that the datafeed processes all data before the disruption starts.)
            assertBusy(() -> {
                if (doesLocalAuditMessageExist("Datafeed is encountering errors extracting data") == false) {
                    JobStats jobStats = getJobStats(jobId);
                    assertThat(jobStats.getDataCounts().getProcessedRecordCount(), is(numDocs));
                }
            });
            networkDisruption.removeAndEnsureHealthy(cluster(REMOTE_CLUSTER));
            // Datafeed should eventually read all the docs.
            // Use a 3 minute timeout because multiple suites run in parallel in CI which slows things down a lot.
            // (Usually the test completes within 1 minute and much faster than that if run locally with nothing major running in parallel.)
            assertBusy(() -> {
                JobStats jobStats = getJobStats(jobId);
                assertThat(jobStats.getState(), is(JobState.OPENED));
                assertThat(jobStats.getDataCounts().getProcessedRecordCount(), is(numDocs));
            }, 3, TimeUnit.MINUTES);
        } finally {
            client(LOCAL_CLUSTER).execute(StopDatafeedAction.INSTANCE, new StopDatafeedAction.Request(datafeedId)).actionGet();
            client(LOCAL_CLUSTER).execute(CloseJobAction.INSTANCE, new CloseJobAction.Request(jobId)).actionGet();
            clearSkipUnavailable();
        }
    }

    /**
     * Index some datafeed data into the remote cluster.
     * @return The epoch millisecond timestamp of the most recent document.
     */
    private long indexRemoteDocs(long numDocs) {
        client(REMOTE_CLUSTER).admin().indices().prepareCreate(DATA_INDEX).setMapping("time", "type=date").get();
        long now = System.currentTimeMillis();
        long weekAgo = now - 604800000;
        long twoWeeksAgo = weekAgo - 604800000;
        BaseMlIntegTestCase.indexDocs(client(REMOTE_CLUSTER), logger, DATA_INDEX, numDocs, twoWeeksAgo, weekAgo);
        return weekAgo;
    }

    private boolean doesLocalAuditMessageExist(String message) {
        try {
            SearchResponse response = client(LOCAL_CLUSTER).prepareSearch(".ml-notifications*")
                .setQuery(new MatchPhraseQueryBuilder("message", message))
                .execute()
                .actionGet();
            return response.getHits().getTotalHits().value > 0;
        } catch (ElasticsearchException e) {
            return false;
        }
    }

    private JobStats getJobStats(String jobId) {
        return client(LOCAL_CLUSTER).execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(jobId))
            .actionGet()
            .getResponse()
            .results()
            .get(0);
    }

    /**
     * Create and start a job and datafeed on the local cluster but searching for data in the remote cluster.
     */
    private void setupJobAndDatafeed(String jobId, String datafeedId, Long endTimeMs) throws Exception {
        Job.Builder job = BaseMlIntegTestCase.createScheduledJob(jobId, ByteSizeValue.ofMb(20));
        client(LOCAL_CLUSTER).execute(PutJobAction.INSTANCE, new PutJobAction.Request(job)).actionGet();

        // Default frequency is 1 second, which avoids the test sleeping excessively
        DatafeedConfig.Builder config = BaseMlIntegTestCase.createDatafeedBuilder(
            datafeedId,
            job.getId(),
            List.of(REMOTE_CLUSTER + ":" + DATA_INDEX)
        );
        // Setting a small chunk size increases the number of separate searches the datafeed
        // must make, which maximises the chance of a problem being exposed by the test
        config.setChunkingConfig(ChunkingConfig.newManual(TimeValue.timeValueMinutes(10)));
        client(LOCAL_CLUSTER).execute(PutDatafeedAction.INSTANCE, new PutDatafeedAction.Request(config.build())).actionGet();

        client(LOCAL_CLUSTER).execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(job.getId()));
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse = client(LOCAL_CLUSTER).execute(
                GetJobsStatsAction.INSTANCE,
                new GetJobsStatsAction.Request(job.getId())
            ).actionGet();
            assertThat(statsResponse.getResponse().results().get(0).getState(), is(JobState.OPENED));
        }, 30, TimeUnit.SECONDS);

        StartDatafeedAction.DatafeedParams datafeedParams = new StartDatafeedAction.DatafeedParams(config.getId(), 0L);
        datafeedParams.setEndTime(endTimeMs);
        client(LOCAL_CLUSTER).execute(StartDatafeedAction.INSTANCE, new StartDatafeedAction.Request(datafeedParams)).actionGet();
    }

    private void setSkipUnavailable(boolean skip) {
        client(LOCAL_CLUSTER).admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put("cluster.remote." + REMOTE_CLUSTER + ".skip_unavailable", skip).build())
            .get();
    }

    private void clearSkipUnavailable() {
        client(LOCAL_CLUSTER).admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().putNull("cluster.remote." + REMOTE_CLUSTER + ".skip_unavailable").build())
            .get();
    }
}
