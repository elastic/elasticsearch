/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.support.WriteRequest;
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
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManager;
import org.elasticsearch.xpack.ml.datafeed.DatafeedRunner;
import org.elasticsearch.xpack.ml.datafeed.extractor.esql.EsqlDataExtractor;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.NodeRoles.onlyRoles;
import static org.hamcrest.Matchers.equalTo;

/**
 * Proves that the ES|QL datafeed node-failure retry (see {@code EsqlQueryRunner#isRetryableNodeFailure})
 * engages on an actual cluster disruption rather than a mocked exception, and that a single churned
 * cycle, in lookback or in real-time, leaves the datafeed started and able to process data afterwards.
 */
@TestLogging(
    value = "org.elasticsearch.xpack.ml.datafeed.extractor.esql:DEBUG,org.elasticsearch.xpack.ml.datafeed.DatafeedJob:INFO",
    reason = "observe the node-failure classification and single-retry log line in EsqlQueryRunner and the lookback completion"
)
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class EsqlDatafeedNodeChurnIT extends BaseMlIntegTestCase {

    private static final String DATAFEED_JOB_LOGGER = "org.elasticsearch.xpack.ml.datafeed.DatafeedJob";

    private static final String ESQL_QUERY_RUNNER_LOGGER = "org.elasticsearch.xpack.ml.datafeed.extractor.esql.EsqlQueryRunner";

    // DatafeedJob and EsqlQueryRunner are package-private, so they are named by string.
    /** Loggers on the datafeed path; none of them may log an ERROR for a node-churn failure. */
    private static final List<String> DATAFEED_LOGGERS = List.of(
        DATAFEED_JOB_LOGGER,
        ESQL_QUERY_RUNNER_LOGGER,
        DatafeedRunner.class.getCanonicalName(),
        DatafeedManager.class.getCanonicalName(),
        EsqlDataExtractor.class.getCanonicalName()
    );

    private static final long INITIAL_DOCS = 500;

    private static final long DOCS_AFTER_HEAL = 50;

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
    @SuppressWarnings("unchecked")
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(
            super.nodePlugins(),
            EsqlCorePlugin.class,
            EncryptionPlugin.class,
            EsqlPlugin.class,
            MockTransportService.TestPlugin.class
        );
    }

    /**
     * The node holding the only shard copy is disconnected while the lookback query is in flight. The datafeed
     * must classify the transport failure as a node failure, retry once, and fail the cycle cleanly (WARN, not ERROR)
     * because the retry finds no other shard copy. After the node heals, a later cycle must process all the data.
     */
    public void testNodeChurnDuringLookbackShouldRecoverAndKeepDatafeedStarted() throws Exception {
        runNodeChurnScenario(false);
    }

    /** Same as the lookback case, but the disrupted query belongs to a real-time cycle after the lookback has finished. */
    public void testNodeChurnDuringRealtimeShouldRecoverAndKeepDatafeedStarted() throws Exception {
        runNodeChurnScenario(true);
    }

    private void runNodeChurnScenario(boolean disruptRealtime) throws Exception {
        ChurnScenario scenario = setUpChurnScenario();
        String datafeedId = scenario.datafeedId();
        String victimNode = scenario.victimNode();
        String jobId = scenario.jobId();

        MockTransportService victimTransport = MockTransportService.getInstance(victimNode);
        // Lookback is disrupted from the first request; for real-time the lookback is left alone until it has finished.
        AtomicBoolean armed = new AtomicBoolean(disruptRealtime == false);
        AtomicBoolean firstArmedRequest = new AtomicBoolean(true);
        CountDownLatch requestReceived = new CountDownLatch(1);
        CountDownLatch releaseRequest = new CountDownLatch(1);
        CountDownLatch resumedQueryReachedVictim = new CountDownLatch(1);
        CountDownLatch resumeGate = new CountDownLatch(1);
        // One behavior for the whole test: a second addRequestHandlingBehavior would wrap this one instead of replacing it.
        victimTransport.addRequestHandlingBehavior(ComputeService.DATA_ACTION_NAME, (handler, request, channel, task) -> {
            if (armed.get() == false) {
                handler.messageReceived(request, channel, task);
                return;
            }
            if (firstArmedRequest.getAndSet(false)) {
                requestReceived.countDown();
                // Times out as a test failure without forwarding, so the request can never complete before the disruption starts.
                safeAwait(releaseRequest, TimeValue.timeValueSeconds(60));
            } else {
                // A request after the heal is a later scheduled cycle. It is held until the failure expectation is installed,
                // so no post-heal cycle can fail or finish before the expectation is watching.
                resumedQueryReachedVictim.countDown();
                safeAwait(resumeGate, TimeValue.timeValueSeconds(60));
            }
            handler.messageReceived(request, channel, task);
        });

        NetworkDisruption networkDisruption = null;
        try (MockLog mockLog = MockLog.capture(DATAFEED_LOGGERS.toArray(String[]::new))) {
            // The disconnect is classified as a node failure and triggers exactly one retry of the same
            // ES|QL query range; this is the proof that the retry engaged on a real transport
            // disconnection, not a fabricated exception.
            MockLog.SeenEventExpectation churnLoggedAtDebug = new MockLog.SeenEventExpectation(
                "node failure classified and retried at DEBUG",
                ESQL_QUERY_RUNNER_LOGGER,
                Level.DEBUG,
                "*ES|QL query failed due to a node or shard availability failure; retrying the same range*"
            );
            // The retry cannot find another shard copy while the partition is up, so the cycle ends in this WARN.
            // Its arrival is the completion barrier for the disrupted cycle.
            MockLog.SeenEventExpectation failedCycleLoggedAtWarn = new MockLog.SeenEventExpectation(
                "failed extraction cycle logged at WARN",
                DATAFEED_JOB_LOGGER,
                Level.WARN,
                "*error while extracting data*"
            );
            MockLog.SeenEventExpectation lookbackFinished = new MockLog.SeenEventExpectation(
                "lookback finished",
                DATAFEED_JOB_LOGGER,
                Level.INFO,
                "*Lookback has finished*"
            );
            // Register all expectations before triggering anything: the "no ERROR" checks must span
            // the whole window from before the disruption starts through the retry, its resolution,
            // and the datafeed settling back to STARTED -- not just an instant sampled after the fact.
            mockLog.addExpectation(churnLoggedAtDebug);
            mockLog.addExpectation(failedCycleLoggedAtWarn);
            if (disruptRealtime) {
                mockLog.addExpectation(lookbackFinished);
            }
            for (String logger : DATAFEED_LOGGERS) {
                mockLog.addExpectation(
                    new MockLog.UnseenEventExpectation(
                        "no ERROR logged by " + logger + " for the node-churn failure",
                        logger,
                        Level.ERROR,
                        "*"
                    )
                );
            }

            MockLog.UnseenEventExpectation noFurtherExtractionFailure = new MockLog.UnseenEventExpectation(
                "no extraction failure after the node healed",
                DATAFEED_JOB_LOGGER,
                Level.WARN,
                "*error while extracting data*"
            );

            long processedBeforeChurn = disruptRealtime ? INITIAL_DOCS : 0L;
            try {
                client().execute(
                    StartDatafeedAction.INSTANCE,
                    new StartDatafeedAction.Request(datafeedId, scenario.windowStart() - TimeValue.timeValueMinutes(5).millis())
                ).actionGet();

                if (disruptRealtime) {
                    lookbackFinished.awaitMatched(TimeValue.timeValueSeconds(30).millis());
                    assertBusy(
                        () -> assertThat(getDataCounts(jobId).getProcessedRecordCount(), equalTo(INITIAL_DOCS)),
                        30,
                        TimeUnit.SECONDS
                    );
                    armed.set(true);
                }

                assertTrue(
                    "victim node should have received the in-flight ES|QL data-node request",
                    requestReceived.await(30, TimeUnit.SECONDS)
                );
                // Every record indexed after this point lies beyond the window of the in-flight real-time query.
                long churnTime = System.currentTimeMillis();

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
                assertThat(getDataCounts(jobId).getProcessedRecordCount(), equalTo(processedBeforeChurn));

                releaseRequest.countDown();
                networkDisruption.removeAndEnsureHealthy(internalCluster());

                // A single churned cycle must not wedge the datafeed: a later scheduled cycle has to run its query again,
                // which is visible as a new data-node request reaching the healed node, and has to succeed.
                // That request is held at the victim, so installing the expectation before opening the gate covers every post-heal cycle.
                mockLog.addExpectation(noFurtherExtractionFailure);
                long expectedProcessed = processedBeforeChurn;
                if (disruptRealtime) {
                    // The failed real-time cycle did not advance the window, so the next successful cycle must pick these up.
                    indexDocs(logger, scenario.indexName(), DOCS_AFTER_HEAL, churnTime, churnTime + 1000);
                    expectedProcessed += DOCS_AFTER_HEAL;
                } else {
                    expectedProcessed += INITIAL_DOCS;
                }
                resumeGate.countDown();
                safeAwait(resumedQueryReachedVictim, TimeValue.timeValueSeconds(60));

                long expected = expectedProcessed;
                assertBusy(() -> assertThat(getDataCounts(jobId).getProcessedRecordCount(), equalTo(expected)), 30, TimeUnit.SECONDS);
                assertBusy(() -> assertThat(getDatafeedState(datafeedId), equalTo(DatafeedState.STARTED)), 30, TimeUnit.SECONDS);

                // Only now -- after the retry, its resolution, and the datafeed settling back to STARTED --
                // assert that no ERROR was logged for this churn event anywhere across that whole window.
                mockLog.assertAllExpectationsMatched();
            } finally {
                releaseRequest.countDown();
                resumeGate.countDown();
                if (networkDisruption != null) {
                    networkDisruption.removeAndEnsureHealthy(internalCluster());
                }
                victimTransport.clearAllRules();
            }
        }
    }

    /**
     * Result of {@link #setUpChurnScenario}: a 3-node cluster with an open job/datafeed pair and a
     * single-shard, zero-replica source index pinned to the "victim" node, distinct from the node
     * running the job's persistent task.
     */
    private record ChurnScenario(String jobId, String datafeedId, String indexName, String victimNode, long windowStart) {}

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
        // Old enough that the whole data set belongs to the lookback and no real-time cycle finds new records on its own.
        long windowStart = now - TimeValue.timeValueMinutes(20).millis();
        long windowEnd = now - TimeValue.timeValueMinutes(10).millis();
        indexDocs(logger, indexName, INITIAL_DOCS, windowStart, windowEnd);
        // A future record keeps the shard's @timestamp range overlapping every real-time window; without it the data-node
        // request is skipped for the shard and a real-time cycle never reaches the victim node.
        long farFuture = now + TimeValue.timeValueHours(1).millis();
        client().prepareIndex(indexName)
            .setSource("time", farFuture, "@timestamp", farFuture)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        DatafeedConfig datafeed = buildEsqlDatafeed(datafeedId, jobId, indexName, TimeValue.timeValueSeconds(1));
        client().execute(PutDatafeedAction.INSTANCE, new PutDatafeedAction.Request(datafeed)).actionGet();

        setMlIndicesDelayedNodeLeftTimeoutToZero();

        return new ChurnScenario(jobId, datafeedId, indexName, victimNode, windowStart);
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
            .setQueryDelay(groupingInterval)
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
