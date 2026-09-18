/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
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
import org.elasticsearch.xpack.core.ml.datafeed.EsqlDatafeedSourceCheckpoint;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
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

/**
 * Proves that ES|QL datafeed node-churn handling (see {@code EsqlDataExtractor#isNodeChurnFailure})
 * survives an actual cluster disruption rather than a mocked exception, and that the durable source
 * checkpoint is never persisted for an extraction cycle that was interrupted mid-flight.
 */
@TestLogging(
    value = "org.elasticsearch.xpack.ml.datafeed.extractor.esql:DEBUG",
    reason = "observe the node-churn classification and single-retry log line in EsqlDataExtractor"
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

    private static final String ESQL_DATA_EXTRACTOR_LOGGER = "org.elasticsearch.xpack.ml.datafeed.extractor.esql.EsqlDataExtractor";

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
     * Case A: the node holding the shard queried by an in-flight ES|QL {@code _query} is disconnected
     * mid-cycle. The resulting transport-level exception must be classified as node churn and retried
     * once (visible as a DEBUG log line from the extractor); since the source index has no replica, the
     * retry itself cannot find another shard copy and the cycle fails cleanly (WARN, not ERROR) rather
     * than looping or crashing. Either way the datafeed returns to a stable, still-scheduled state.
     */
    public void testMidQueryNodeChurnShouldLogAtDebugAndKeepDatafeedStarted() throws Exception {
        ChurnScenario scenario = setUpChurnScenario("a");
        String datafeedId = scenario.datafeedId();
        String victimNode = scenario.victimNode();
        long windowStart = scenario.windowStart();

        MockTransportService victimTransport = MockTransportService.getInstance(victimNode);
        CountDownLatch requestReceived = new CountDownLatch(1);
        CountDownLatch releaseRequest = new CountDownLatch(1);
        victimTransport.addRequestHandlingBehavior(ESQL_DATA_ACTION_NAME, (handler, request, channel, task) -> {
            requestReceived.countDown();
            releaseRequest.await(10, TimeUnit.SECONDS);
            handler.messageReceived(request, channel, task);
        });

        NetworkDisruption networkDisruption = null;
        try (MockLog mockLog = MockLog.capture(DATAFEED_JOB_LOGGER, ESQL_DATA_EXTRACTOR_LOGGER)) {
            // The disconnect is classified as node churn and triggers exactly one retry of the same
            // ES|QL query range; this is the proof that isNodeChurnFailure engaged on a real transport
            // disconnection, not a fabricated exception.
            MockLog.SeenEventExpectation churnLoggedAtDebug = new MockLog.SeenEventExpectation(
                "node churn classified and retried at DEBUG",
                ESQL_DATA_EXTRACTOR_LOGGER,
                Level.DEBUG,
                "*ES|QL query failed due to node churn; retrying the same range*"
            );
            MockLog.UnseenEventExpectation noErrorLogged = new MockLog.UnseenEventExpectation(
                "no ERROR logged for the node-churn extraction failure",
                DATAFEED_JOB_LOGGER,
                Level.ERROR,
                "*"
            );
            // Register both expectations before triggering anything: the "no ERROR" check must span
            // the whole window from before the disruption starts through the retry, its resolution,
            // and the datafeed settling back to STARTED -- not just an instant sampled after the fact.
            mockLog.addExpectation(churnLoggedAtDebug);
            mockLog.addExpectation(noErrorLogged);

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

                // Confirms the disconnection was classified as churn and retried once.
                churnLoggedAtDebug.awaitMatched(TimeValue.timeValueSeconds(30).millis());
            } finally {
                releaseRequest.countDown();
                if (networkDisruption != null) {
                    networkDisruption.removeAndEnsureHealthy(internalCluster());
                }
                victimTransport.clearAllRules();
            }

            // A single churned cycle must not wedge the datafeed: it stays STARTED and keeps its
            // normal real-time schedule rather than retrying without bound or crashing.
            assertBusy(() -> assertThat(getDatafeedState(datafeedId), equalTo(DatafeedState.STARTED)), 30, TimeUnit.SECONDS);

            // Only now -- after the retry, its resolution, and the datafeed settling back to STARTED --
            // assert that no ERROR was logged for this churn event anywhere across that whole window.
            mockLog.assertAllExpectationsMatched();
        }
    }

    /**
     * Case B: the node holding the datafeed's persistent task ("the holder") is stopped while its
     * first extraction cycle is still in flight, before postData/flush/checkpoint-commit can run. No
     * {@link EsqlDatafeedSourceCheckpoint} document must be persisted for that interrupted cycle.
     */
    public void testHolderNodeLossMidCycleShouldNotPersistCheckpoint() throws Exception {
        ChurnScenario scenario = setUpChurnScenario("b");
        String jobId = scenario.jobId();
        String datafeedId = scenario.datafeedId();
        String holderNode = scenario.holderNode();
        String victimNode = scenario.victimNode();
        long windowStart = scenario.windowStart();

        MockTransportService victimTransport = MockTransportService.getInstance(victimNode);
        CountDownLatch requestReceived = new CountDownLatch(1);
        CountDownLatch releaseRequest = new CountDownLatch(1);
        victimTransport.addRequestHandlingBehavior(ESQL_DATA_ACTION_NAME, (handler, request, channel, task) -> {
            requestReceived.countDown();
            releaseRequest.await(30, TimeUnit.SECONDS);
            handler.messageReceived(request, channel, task);
        });

        try {
            client().execute(
                StartDatafeedAction.INSTANCE,
                new StartDatafeedAction.Request(datafeedId, windowStart - TimeValue.timeValueMinutes(5).millis())
            ).actionGet();

            assertTrue(
                "victim node should have received the in-flight ES|QL data-node request",
                requestReceived.await(30, TimeUnit.SECONDS)
            );

            // The holder is torn down while its query to the victim node is still blocked: the extraction,
            // postData and flush for this cycle can never complete, so the checkpoint commit can never run.
            internalCluster().stopNode(holderNode);
            ensureStableCluster(2, victimNode);
        } finally {
            releaseRequest.countDown();
            victimTransport.clearAllRules();
        }

        assertBusy(() -> {
            refresh(AnomalyDetectorsIndex.jobResultsAliasedName(jobId));
            GetResponse getResponse = client().get(
                new GetRequest(AnomalyDetectorsIndex.jobResultsAliasedName(jobId), EsqlDatafeedSourceCheckpoint.documentId(jobId))
            ).actionGet();
            assertThat(getResponse.isExists(), equalTo(false));
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Result of {@link #setUpChurnScenario}: a 3-node cluster with an open job/datafeed pair and a
     * single-shard, zero-replica source index pinned to the "victim" node, distinct from the "holder"
     * node running the job's persistent task.
     */
    private record ChurnScenario(
        String jobId,
        String datafeedId,
        String indexName,
        String holderNode,
        String victimNode,
        long windowStart
    ) {}

    /**
     * Shared setup for both churn cases: starts a 3-node cluster (1 master-only, 2 data+ml),
     * enables ES|QL datafeeds, opens a job, pins a zero-replica source index to the node that did
     * not get the job (the "victim"), indexes data into it, and puts (but does not start) an ES|QL
     * datafeed for the job.
     */
    private ChurnScenario setUpChurnScenario(String scenario) throws Exception {
        internalCluster().ensureAtMostNumDataNodes(0);
        internalCluster().startMasterOnlyNode();
        String nodeA = internalCluster().startNode(onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.ML_ROLE)));
        String nodeB = internalCluster().startNode(onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.ML_ROLE)));
        ensureStableCluster(3);
        updateClusterSettings(Settings.builder().put(MachineLearning.ESQL_DATAFEEDS_ENABLED.getKey(), true));

        String jobId = "esql-churn-" + scenario + "-job";
        String datafeedId = jobId + "-datafeed";
        String indexName = "esql-churn-" + scenario + "-data";

        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(buildEsqlJobBuilder(jobId))).actionGet();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId)).actionGet();

        String holderNode = awaitJobOpenedAndAssigned(jobId, null);
        String victimNode = holderNode.equals(nodeA) ? nodeB : nodeA;

        createIndexPinnedToNode(indexName, victimNode);

        long now = System.currentTimeMillis();
        long windowStart = now - TimeValue.timeValueMinutes(12).millis();
        long windowEnd = now - TimeValue.timeValueMinutes(1).millis();
        indexDocs(logger, indexName, 500, windowStart, windowEnd);

        DatafeedConfig datafeed = buildEsqlDatafeed(datafeedId, jobId, indexName, TimeValue.timeValueMinutes(5));
        client().execute(PutDatafeedAction.INSTANCE, new PutDatafeedAction.Request(datafeed)).actionGet();

        setMlIndicesDelayedNodeLeftTimeoutToZero();

        return new ChurnScenario(jobId, datafeedId, indexName, holderNode, victimNode, windowStart);
    }

    private static Job.Builder buildEsqlJobBuilder(String jobId) {
        Job.Builder builder = new Job.Builder(jobId);
        Detector.Builder detector = new Detector.Builder("count", null);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueMinutes(5));
        builder.setAnalysisConfig(analysisConfig);
        builder.setDataDescription(new DataDescription.Builder().setTimeField("time").setTimeFormat(DataDescription.EPOCH_MS));
        return builder;
    }

    private static DatafeedConfig buildEsqlDatafeed(String datafeedId, String jobId, String indexName, TimeValue groupingInterval) {
        return new DatafeedConfig.Builder(datafeedId, jobId).setEsqlQuery("FROM " + indexName + " | KEEP time")
            .setSourceTimeField("@timestamp")
            .setGroupingInterval(groupingInterval)
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
