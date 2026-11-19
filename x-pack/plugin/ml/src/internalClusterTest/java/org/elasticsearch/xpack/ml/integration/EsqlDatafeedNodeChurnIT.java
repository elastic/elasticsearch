/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.datafeed.DelayedDataCheckConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.encryption.EncryptionPlugin;
import org.elasticsearch.xpack.esql.core.plugin.EsqlCorePlugin;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.NodeRoles.onlyRoles;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Proves that the ES|QL datafeed node-failure retry (see {@code EsqlQueryRunner#isRetryableNodeFailure})
 * engages on an actual cluster disruption rather than a mocked exception, and that a single churned
 * cycle leaves the datafeed started instead of wedging or crashing it.
 */
@TestLogging(
    value = "org.elasticsearch.xpack.ml.datafeed.extractor.esql:DEBUG",
    reason = "observe the node-failure classification and single-retry log line in EsqlQueryRunner"
)
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class EsqlDatafeedNodeChurnIT extends BaseMlIntegTestCase {

    /**
     * Per-data-node fan-out action for an ES|QL {@code _query} request. Mirrors
     * {@code org.elasticsearch.xpack.esql.plugin.ComputeService#DATA_ACTION_NAME} (not on the ml
     * plugin's compile classpath); the literal is derived from
     * {@code EsqlQueryAction.NAME = "indices:data/read/esql"} plus the data-node suffix.
     */
    private static final String ESQL_DATA_ACTION_NAME = "indices:data/read/esql/data";

    private static final String DATAFEED_JOB_LOGGER = "org.elasticsearch.xpack.ml.datafeed.DatafeedJob";

    private static final String ESQL_QUERY_RUNNER_LOGGER = "org.elasticsearch.xpack.ml.datafeed.extractor.esql.EsqlQueryRunner";

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // This test never exercises ES|QL's external-data-source federation feature, but
            // EsqlPlugin#createComponents unconditionally resolves the encryption service at
            // startup regardless of this setting -- so EncryptionPlugin is still installed below
            // in nodePlugins(); this setting only disables federation at runtime, it does not
            // remove the need for that plugin to be present.
            .put("esql.federation.enabled", false)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(
            CollectionUtils.appendToCopy(
                CollectionUtils.appendToCopy(
                    CollectionUtils.appendToCopy(super.nodePlugins(), EsqlCorePlugin.class),
                    EncryptionPlugin.class
                ),
                EsqlPlugin.class
            ),
            MockTransportService.TestPlugin.class
        );
    }

    /**
     * The node holding the shard queried by an in-flight ES|QL {@code _query} is disconnected
     * mid-cycle. The resulting transport-level exception must be classified as a node failure and retried
     * once (visible as a DEBUG log line from the query runner); since the source index has no replica, the
     * retry itself cannot find another shard copy and the cycle fails cleanly (WARN, not ERROR) rather
     * than looping or crashing. A 1s bucket span, grouping interval and frequency keep the real-time schedule short enough to
     * observe the next cycle after the node heals. That cycle must succeed: the job processes records and logs no further
     * extraction failure, and the datafeed stays STARTED.
     */
    public void testMidQueryNodeChurnShouldRecoverAndKeepDatafeedStarted() throws Exception {
        ChurnScenario scenario = setUpChurnScenario();
        String datafeedId = scenario.datafeedId();
        String victimNode = scenario.victimNode();
        long windowStart = scenario.windowStart();
        String jobId = scenario.jobId();

        MockTransportService victimTransport = MockTransportService.getInstance(victimNode);
        CountDownLatch requestReceived = new CountDownLatch(1);
        CountDownLatch releaseRequest = new CountDownLatch(1);
        victimTransport.addRequestHandlingBehavior(ESQL_DATA_ACTION_NAME, (handler, request, channel, task) -> {
            requestReceived.countDown();
            // Times out as a test failure without forwarding, so the request can never complete before the disruption starts.
            safeAwait(releaseRequest, TimeValue.timeValueSeconds(60));
            handler.messageReceived(request, channel, task);
        });

        NetworkDisruption networkDisruption = null;
        try (MockLog mockLog = MockLog.capture(DATAFEED_JOB_LOGGER, ESQL_QUERY_RUNNER_LOGGER)) {
            // The disconnect is classified as a node failure and triggers exactly one retry of the same
            // ES|QL query range; this is the proof that the retry engaged on a real transport
            // disconnection, not a fabricated exception.
            MockLog.SeenEventExpectation churnLoggedAtDebug = new MockLog.SeenEventExpectation(
                "node failure classified and retried at DEBUG",
                ESQL_QUERY_RUNNER_LOGGER,
                Level.DEBUG,
                "*ES|QL query failed due to a node or shard availability failure; retrying the same range*"
            );
            MockLog.UnseenEventExpectation noErrorLogged = new MockLog.UnseenEventExpectation(
                "no ERROR logged for the node-churn extraction failure",
                DATAFEED_JOB_LOGGER,
                Level.ERROR,
                "*"
            );
            // The retry cannot find another shard copy while the partition is up, so the cycle ends in this WARN.
            // Its arrival is the completion barrier for the disrupted cycle.
            MockLog.SeenEventExpectation failedCycleLoggedAtWarn = new MockLog.SeenEventExpectation(
                "failed extraction cycle logged at WARN",
                DATAFEED_JOB_LOGGER,
                Level.WARN,
                "*error while extracting data*"
            );
            // Register all expectations before triggering anything: the "no ERROR" check must span
            // the whole window from before the disruption starts through the retry, its resolution,
            // and the datafeed settling back to STARTED -- not just an instant sampled after the fact.
            mockLog.addExpectation(churnLoggedAtDebug);
            mockLog.addExpectation(failedCycleLoggedAtWarn);
            mockLog.addExpectation(noErrorLogged);

            MockLog.UnseenEventExpectation noFurtherExtractionFailure = new MockLog.UnseenEventExpectation(
                "no extraction failure after the node healed",
                DATAFEED_JOB_LOGGER,
                Level.WARN,
                "*error while extracting data*"
            );

            try {
                client().execute(
                    StartDatafeedAction.INSTANCE,
                    new StartDatafeedAction.Request(datafeedId, windowStart - TimeValue.timeValueMinutes(5).millis())
                ).actionGet();

                assertTrue(
                    "victim node should have received the in-flight ES|QL data-node request",
                    requestReceived.await(30, TimeUnit.SECONDS)
                );

                Set<String> isolatedSide = Collections.singleton(victimNode);
                Set<String> restOfClusterSide = new HashSet<>(Arrays.asList(internalCluster().getNodeNames()));
                restOfClusterSide.remove(victimNode);
                networkDisruption = new NetworkDisruption(
                    new NetworkDisruption.TwoPartitions(isolatedSide, restOfClusterSide),
                    NetworkDisruption.DISCONNECT
                );
                internalCluster().setDisruptionScheme(networkDisruption);
                networkDisruption.startDisrupting();

                // Confirms the disconnection was classified as a node failure and retried once.
                churnLoggedAtDebug.awaitMatched(TimeValue.timeValueSeconds(30).millis());
                // Heal only after the failed cycle has completed; healing right after the DEBUG line would let the retry succeed.
                failedCycleLoggedAtWarn.awaitMatched(TimeValue.timeValueSeconds(30).millis());
                // Nothing may have been processed while the only shard copy was unreachable.
                assertThat(getDataCounts(jobId).getProcessedRecordCount(), equalTo(0L));
            } finally {
                releaseRequest.countDown();
                if (networkDisruption != null) {
                    networkDisruption.removeAndEnsureHealthy(internalCluster());
                }
                victimTransport.clearAllRules();
            }

            // A single churned cycle must not wedge the datafeed: a later scheduled cycle has to run its query again,
            // which is visible as a new data-node request reaching the healed node, and has to succeed.
            // Only failures logged from here on count against the resumed cycle.
            mockLog.addExpectation(noFurtherExtractionFailure);
            CountDownLatch resumedQueryReachedVictim = new CountDownLatch(1);
            victimTransport.addRequestHandlingBehavior(ESQL_DATA_ACTION_NAME, (handler, request, channel, task) -> {
                resumedQueryReachedVictim.countDown();
                handler.messageReceived(request, channel, task);
            });
            safeAwait(resumedQueryReachedVictim, TimeValue.timeValueSeconds(60));
            victimTransport.clearAllRules();

            // The failed cycle did not advance time, so records can only be processed by a successful post-heal cycle.
            assertBusy(() -> assertThat(getDataCounts(jobId).getProcessedRecordCount(), greaterThan(0L)), 30, TimeUnit.SECONDS);

            assertBusy(() -> assertThat(getDatafeedState(datafeedId), equalTo(DatafeedState.STARTED)), 30, TimeUnit.SECONDS);

            // Only now -- after the retry, its resolution, and the datafeed settling back to STARTED --
            // assert that no ERROR was logged for this churn event anywhere across that whole window.
            mockLog.assertAllExpectationsMatched();
        }
    }

    /**
     * Result of {@link #setUpChurnScenario}: a 3-node cluster with an open job/datafeed pair and a
     * single-shard, zero-replica source index pinned to the "victim" node, distinct from the node
     * running the job's persistent task.
     */
    private record ChurnScenario(String jobId, String datafeedId, String victimNode, long windowStart) {}

    /**
     * Starts a 3-node cluster (1 master-only, 2 data+ml), opens a job, pins a zero-replica source
     * index to the node that did not get the job (the "victim"), indexes data into it, and puts
     * (but does not start) an ES|QL datafeed for the job.
     */
    private ChurnScenario setUpChurnScenario() throws Exception {
        assumeTrue("ES|QL datafeeds feature flag must be enabled", MachineLearning.ESQL_DATAFEEDS_FEATURE_FLAG.isEnabled());
        internalCluster().ensureAtMostNumDataNodes(0);
        internalCluster().startMasterOnlyNode();
        String nodeA = internalCluster().startNode(onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.ML_ROLE)));
        String nodeB = internalCluster().startNode(onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.ML_ROLE)));
        ensureStableCluster(3);

        String jobId = "esql-churn-job";
        String datafeedId = jobId + "-datafeed";
        String indexName = "esql-churn-data";

        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(buildEsqlJobBuilder(jobId))).actionGet();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId)).actionGet();

        String holderNode = awaitJobOpenedAndAssigned(jobId, null);
        String victimNode = holderNode.equals(nodeA) ? nodeB : nodeA;

        createIndexPinnedToNode(indexName, victimNode);

        long now = System.currentTimeMillis();
        long windowStart = now - TimeValue.timeValueMinutes(12).millis();
        long windowEnd = now - TimeValue.timeValueMinutes(1).millis();
        indexDocs(logger, indexName, 500, windowStart, windowEnd);

        DatafeedConfig datafeed = buildEsqlDatafeed(datafeedId, jobId, indexName, TimeValue.timeValueSeconds(1));
        client().execute(PutDatafeedAction.INSTANCE, new PutDatafeedAction.Request(datafeed)).actionGet();

        setMlIndicesDelayedNodeLeftTimeoutToZero();

        return new ChurnScenario(jobId, datafeedId, victimNode, windowStart);
    }

    private static Job.Builder buildEsqlJobBuilder(String jobId) {
        Job.Builder builder = new Job.Builder(jobId);
        Detector.Builder detector = new Detector.Builder("count", null);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueSeconds(1));
        builder.setAnalysisConfig(analysisConfig);
        builder.setDataDescription(new DataDescription.Builder().setTimeField("time").setTimeFormat(DataDescription.EPOCH_MS));
        return builder;
    }

    private static DatafeedConfig buildEsqlDatafeed(String datafeedId, String jobId, String indexName, TimeValue groupingInterval) {
        return new DatafeedConfig.Builder(datafeedId, jobId).setEsqlQuery("FROM " + indexName + " | KEEP time")
            .setSourceTimeField("@timestamp")
            .setGroupingInterval(groupingInterval)
            .setFrequency(groupingInterval)
            .setDelayedDataCheckConfig(DelayedDataCheckConfig.disabledDelayedDataCheckConfig())
            .build();
    }

    private void createIndexPinnedToNode(String indexName, String nodeName) {
        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(indexSettings(1, 0).put("index.routing.allocation.require._name", nodeName))
            .setMapping("time", "type=long", "@timestamp", "type=date")
            .get();
        ensureGreen(indexName);
    }
}
