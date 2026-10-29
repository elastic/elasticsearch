/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkOperator;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceOperator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Checks aggregation correctness and failure propagation through hash-shuffle stages.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 3, numClientNodes = 1)
public class ShuffleIT extends AbstractEsqlIntegTestCase {
    private static final int DOC_COUNT = 20_000;
    private static final int BULK_SIZE = 1_000;
    private static final int BREAKER_DOC_COUNT = 200_000;
    private static final int BREAKER_BULK_SIZE = 5_000;
    private static final int LOOKUP_DOC_COUNT = 2_000;
    private static final int LOOKUP_USER_COUNT = 1_500;
    private static final int SALARY_DOC_COUNT = 3_000;
    private static final int SALARY_USER_COUNT = 1_000;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(InternalExchangePlugin.class);
        plugins.add(FailingPauseFieldPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING, TimeValue.timeValueMinutes(1))
            .put(
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(),
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.getDefault(Settings.EMPTY)
            )
            .put(
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(),
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getDefault(Settings.EMPTY)
            );
        if (DiscoveryNode.canContainData(otherSettings) == false) {
            settings.put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "12mb");
        }
        return settings.build();
    }

    @Before
    public void setupIndices() {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("events")
                .setSettings(
                    Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 5).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
                .setMapping("user_id", "type=long", "host", "type=keyword", "bytes", "type=long", "dept", "type=keyword")
        );
        for (int start = 0; start < DOC_COUNT; start += BULK_SIZE) {
            BulkRequestBuilder bulk = client().prepareBulk("events");
            int end = Math.min(start + BULK_SIZE, DOC_COUNT);
            for (int doc = start; doc < end; doc++) {
                Map<String, Object> source = new HashMap<>();
                int userIdKind = randomInt(99);
                if (userIdKind == 0) {
                    source.put("user_id", null);
                } else {
                    long userId = randomLongBetween(0, 2_999);
                    if (userIdKind == 1) {
                        source.put("user_id", List.of(userId, (userId + 1) % 3_000));
                    } else {
                        source.put("user_id", userId);
                    }
                }
                source.put("host", "host-" + randomIntBetween(0, 99));
                source.put("bytes", randomIntBetween(0, 10_000));
                source.put("dept", randomFrom("engineering", "finance", "sales"));
                bulk.add(client().prepareIndex().setSource(source));
            }
            bulk.get();
        }
        client().admin().indices().prepareRefresh("events").get();
    }

    private void setupBreakerIndex() {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("breaker_events")
                .setSettings(
                    Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
                .setMapping("breaker_key", "type=keyword", "breaker_bucket", "type=long")
        );
        for (int start = 0; start < BREAKER_DOC_COUNT; start += BREAKER_BULK_SIZE) {
            BulkRequestBuilder bulk = client().prepareBulk("breaker_events");
            int end = Math.min(start + BREAKER_BULK_SIZE, BREAKER_DOC_COUNT);
            for (int doc = start; doc < end; doc++) {
                bulk.add(client().prepareIndex().setSource(Map.of("breaker_key", "key-" + doc, "breaker_bucket", doc % 10)));
            }
            bulk.get();
        }
        client().admin().indices().prepareRefresh("breaker_events").get();
    }

    private void setupJoinIndices() {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("users_lookup")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.mode", "lookup")
                )
                .setMapping("user_id", "type=long", "segment", "type=keyword", "country", "type=keyword")
        );
        for (int start = 0; start < LOOKUP_DOC_COUNT; start += BULK_SIZE) {
            BulkRequestBuilder bulk = client().prepareBulk("users_lookup");
            int end = Math.min(start + BULK_SIZE, LOOKUP_DOC_COUNT);
            for (int doc = start; doc < end; doc++) {
                Map<String, Object> source = new HashMap<>();
                source.put("user_id", doc % LOOKUP_USER_COUNT);
                source.put("segment", "segment-" + doc % 8);
                source.put("country", "country-" + doc % 50);
                bulk.add(client().prepareIndex().setSource(source));
            }
            bulk.get();
        }

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("salaries")
                .setSettings(
                    Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
                .setMapping("user_id", "type=long", "amount", "type=long")
        );
        for (int start = 0; start < SALARY_DOC_COUNT; start += BULK_SIZE) {
            BulkRequestBuilder bulk = client().prepareBulk("salaries");
            int end = Math.min(start + BULK_SIZE, SALARY_DOC_COUNT);
            for (int doc = start; doc < end; doc++) {
                Map<String, Object> source = new HashMap<>();
                source.put("user_id", doc % SALARY_USER_COUNT);
                source.put("amount", doc % 997 + 1);
                bulk.add(client().prepareIndex().setSource(source));
            }
            bulk.get();
        }
        client().admin().indices().prepareRefresh("users_lookup", "salaries").get();
    }

    public void testShuffleAggCountByUserId() {
        String query = "FROM events | STATS c = COUNT(*) BY user_id | SORT user_id";
        assertShuffleMatches(query, 1);
    }

    public void testShuffleAgg() {
        assertShuffleMatches(
            "FROM events | STATS c = COUNT(*), b = SUM(bytes) BY user_id, host | SORT c DESC, user_id, host | LIMIT 50",
            randomIntBetween(1, 4)
        );
        assertShuffleMatches(
            "FROM events | STATS c = COUNT(*), b = SUM(bytes) BY user_id, host | SORT c DESC, user_id, host",
            randomIntBetween(1, 4)
        );
    }

    public void testShuffleAggTwoLevel() {
        assertShuffleMatches("FROM events | STATS BY user_id | STATS n = COUNT(*)", randomIntBetween(1, 4));
    }

    public void testDistributedAggAvoidsSingleNodeCircuitBreaker() throws Exception {
        setupBreakerIndex();
        String query = """
            FROM breaker_events
            | STATS c = COUNT(*) BY breaker_key, breaker_bucket
            | STATS total = SUM(c) BY breaker_bucket
            | SORT breaker_bucket
            """;
        DiscoveryNode coordinator = clusterService().state()
            .nodes()
            .getNodes()
            .values()
            .stream()
            .filter(node -> node.canContainData() == false && node.isMasterNode() == false)
            .findFirst()
            .orElseThrow();

        Settings singleNodePragmas = Settings.builder()
            .put(QueryPragmas.EXCHANGE_BUFFER_SIZE.getKey(), 1)
            .put(QueryPragmas.TASK_CONCURRENCY.getKey(), 1)
            .build();
        EsqlQueryRequest singleNodeRequest = syncEsqlQueryRequest(query).acceptedPragmaRisks(true)
            .allowPartialResults(false)
            .pragmas(new QueryPragmas(singleNodePragmas));
        Exception failure = expectThrows(Exception.class, () -> {
            try (
                EsqlQueryResponse ignored = client(coordinator.getName()).execute(EsqlQueryAction.INSTANCE, singleNodeRequest)
                    .actionGet(2, TimeUnit.MINUTES)
            ) {}
        });
        Throwable cause = ExceptionsHelper.unwrapCause(EsqlTestUtils.unwrapIfWrappedInRemoteException(failure));
        assertTrue("expected a circuit breaker failure, got [" + cause + "]", cause instanceof CircuitBreakingException);
        assertTrue(
            "expected aggregation state to trip the breaker, got [" + cause.getMessage() + "]",
            cause.getMessage().contains("SwissHash") || cause.getMessage().contains("reused_arrays")
        );
        assertFalse("page fetching, not aggregation state, tripped the breaker", cause.getMessage().contains("fetch page"));

        EsqlQueryRequest distributedRequest = syncEsqlQueryRequest(query).acceptedPragmaRisks(true)
            .allowPartialResults(false)
            .pragmas(new QueryPragmas(breakerShufflePragmas()))
            .profile(true);
        try (
            EsqlQueryResponse response = client(coordinator.getName()).execute(EsqlQueryAction.INSTANCE, distributedRequest)
                .actionGet(2, TimeUnit.MINUTES)
        ) {
            List<List<Object>> expectedRows = new ArrayList<>();
            for (long bucket = 0; bucket < 10; bucket++) {
                expectedRows.add(List.of(BREAKER_DOC_COUNT / 10L, bucket));
            }
            assertThat("distributed aggregate result differs", getValuesList(response), equalTo(expectedRows));
            EsqlQueryResponse.Profile profile = response.profile();
            assertNotNull("distributed aggregate profile missing", profile);
            assertTrue("distributed aggregate profile did not include a HASH stage sink", hasHashShufflePlan(profile));
            assertTrue("distributed aggregate profile did not report rows sent to an exchange sink", hasStageExchangeSinkRows(profile));
            assertTrue(
                "distributed aggregate profile did not report rows emitted from an exchange source",
                hasStageExchangeSourceRows(profile)
            );

            Set<String> expectedDataNodes = clusterService().state()
                .nodes()
                .getDataNodes()
                .values()
                .stream()
                .map(DiscoveryNode::getName)
                .collect(Collectors.toSet());
            Set<String> workerNodes = profile.plans()
                .stream()
                .filter(plan -> plan.description().startsWith("stage") && plan.planTree().contains("StageSourceExec"))
                .map(plan -> plan.nodeName())
                .collect(Collectors.toSet());
            assertEquals(3, expectedDataNodes.size());
            assertThat("HASH aggregation workers did not run on all data nodes", workerNodes, equalTo(expectedDataNodes));
        }
    }

    public void testLookupJoinShuffle() throws Exception {
        setupJoinIndices();
        assertShuffleJoinMatches(
            "FROM events | LOOKUP JOIN users_lookup ON user_id | STATS c = COUNT(*) BY segment | SORT segment",
            randomIntBetween(1, 4),
            false
        );
        assertShuffleJoinMatches("""
            FROM events
            | WHERE bytes > 100
            | LOOKUP JOIN users_lookup ON user_id
            | KEEP user_id, segment, bytes
            | SORT user_id, segment, bytes
            | LIMIT 500
            """, randomIntBetween(1, 4), false);
    }

    public void testSubqueryJoinShuffle() throws Exception {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());
        setupJoinIndices();
        assertShuffleSubqueryJoinMatches("""
            FROM events
            | LEFT OUTER JOIN (FROM salaries | STATS total = SUM(amount) BY user_id) ON user_id
            | STATS s = SUM(total) BY dept
            | SORT dept
            """, randomIntBetween(1, 4), false);
        assertShuffleSubqueryJoinMatches("""
            FROM events
            | INNER JOIN (FROM salaries | STATS total = SUM(amount) BY user_id) ON user_id
            | STATS s = SUM(total) BY dept
            | SORT dept
            """, randomIntBetween(1, 4), false);
    }

    public void testBroadcastSubqueryJoinShuffle() throws Exception {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());
        setupJoinIndices();
        assertShuffleJoinMatches("""
            FROM events
            | LEFT OUTER JOIN (FROM salaries | STATS total = SUM(amount) BY user_id) ON user_id
            | STATS s = SUM(total) BY dept
            | SORT dept
            """, randomIntBetween(1, 4), false, false, "broadcast");
    }

    public void testBroadcastJoinWithEmptyBuild() throws Exception {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());
        setupJoinIndices();
        for (String joinType : List.of("LEFT OUTER", "INNER")) {
            assertShuffleJoinMatches(
                "FROM events | "
                    + joinType
                    + " JOIN (FROM salaries | WHERE amount > 1000) ON user_id | STATS c = COUNT(*) BY dept | SORT dept",
                randomIntBetween(1, 4),
                false,
                false,
                "broadcast"
            );
        }
    }

    public void testJoinChainsAndBushyJoinShuffle() throws Exception {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());
        setupJoinIndices();
        assertShuffleSubqueryJoinMatches("""
            FROM events
            | LEFT OUTER JOIN (FROM salaries | STATS total = SUM(amount) BY user_id) ON user_id
            | LEFT OUTER JOIN (FROM salaries | STATS n = COUNT(*) BY user_id) ON user_id
            | STATS s = SUM(total), c = SUM(n) BY dept
            | SORT dept
            """, randomIntBetween(1, 4), randomBoolean());
        assertShuffleSubqueryJoinMatches("""
            FROM events
            | LEFT OUTER JOIN (FROM salaries | STATS total = SUM(amount) BY user_id) ON user_id
            | LEFT OUTER JOIN (FROM events | STATS hosts = COUNT_DISTINCT(host) BY dept) ON dept
            | STATS s = SUM(total), h = MAX(hosts) BY dept
            | SORT dept
            """, randomIntBetween(1, 4), randomBoolean());
        assertShuffleSubqueryJoinMatches("""
            FROM events
            | LEFT OUTER JOIN (
                FROM salaries
                | LEFT OUTER JOIN (FROM events | STATS hosts = COUNT_DISTINCT(host) BY user_id) ON user_id
                | STATS total = SUM(amount), h = MAX(hosts) BY user_id
            ) ON user_id
            | STATS s = SUM(total) BY dept
            | SORT dept
            """, randomIntBetween(1, 4), randomBoolean());
    }

    public void testAggregatedInputsAndPostJoinAggregationsShuffle() throws Exception {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());
        setupJoinIndices();
        assertShuffleSubqueryJoinMatches("""
            FROM events
            | LEFT OUTER JOIN (FROM salaries | STATS total = SUM(amount) BY user_id) ON user_id
            | STATS c = COUNT(*), s = SUM(total) BY user_id
            | SORT user_id
            | LIMIT 200
            """, randomIntBetween(1, 4), true);
        for (boolean shuffleAgg : List.of(false, true)) {
            assertShuffleSubqueryJoinMatches("""
                FROM events
                | STATS b = SUM(bytes) BY user_id
                | LEFT OUTER JOIN (FROM salaries | STATS total = SUM(amount) BY user_id) ON user_id
                | SORT user_id
                | LIMIT 200
                """, randomIntBetween(1, 4), shuffleAgg);
        }
        assertShuffleJoinMatches("""
            FROM events
            | STATS c = COUNT(*) BY user_id
            | LOOKUP JOIN users_lookup ON user_id
            | STATS n = SUM(c) BY segment
            | SORT segment
            """, randomIntBetween(1, 4), true);
    }

    public void testUnionShuffleOnEitherJoinSideAndForAggregation() throws Exception {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());
        assumeTrue("subquery in FROM required", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        setupJoinIndices();
        assertShuffleSubqueryJoinMatches("""
            FROM events
            | LEFT OUTER JOIN (FROM salaries, (FROM salaries | WHERE amount > 500)) ON user_id
            | STATS total = SUM(amount) BY dept
            | SORT dept
            | LIMIT 200
            """, randomIntBetween(1, 4), true);
        assertShuffleSubqueryJoinMatches("""
            FROM events, (FROM events | WHERE bytes > 100)
            | LEFT OUTER JOIN (FROM salaries | STATS total = SUM(amount) BY user_id) ON user_id
            | STATS total_amount = SUM(total) BY dept
            | SORT dept
            | LIMIT 200
            """, randomIntBetween(1, 4), true);
        assertShuffleJoinMatches("""
            FROM events, (FROM events | WHERE bytes > 100)
            | STATS c = COUNT(*) BY dept
            | SORT dept
            """, randomIntBetween(1, 4), true);
        assertShuffleJoinMatches("""
            FROM events, (FROM events | WHERE bytes > 100)
            | SORT user_id, dept, host, bytes
            | LIMIT 20
            """, randomIntBetween(1, 4), false, false);
    }

    public void testShuffleAggAndJoin() throws Exception {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());
        setupJoinIndices();
        assertShuffleSubqueryJoinMatches("""
            FROM events
            | LEFT OUTER JOIN (FROM salaries | STATS total = SUM(amount) BY user_id) ON user_id
            | STATS s = SUM(total) BY dept
            | SORT dept
            """, randomIntBetween(1, 4), true);
    }

    public void testShuffleJoinFallback() throws Exception {
        setupJoinIndices();
        assertShuffleJoinMatches("""
            FROM events
            | RENAME user_id AS event_user_id
            | LOOKUP JOIN users_lookup ON event_user_id == user_id AND segment == "segment-0"
            | STATS c = COUNT(*) BY dept
            | SORT dept
            """, randomIntBetween(1, 4), false, false);
    }

    public void testProducerFailure() throws Exception {
        FailingPauseFieldPlugin.resetPlugin();
        addFailingRuntimeField();
        EsqlQueryRequest request = syncEsqlQueryRequest("FROM events | STATS c = COUNT(fail_me) BY user_id").acceptedPragmaRisks(true)
            .pragmas(new QueryPragmas(shufflePragmas(randomIntBetween(1, 4))));
        ActionFuture<EsqlQueryResponse> future = client().execute(EsqlQueryAction.INSTANCE, request);

        try {
            assertTrue(FailingPauseFieldPlugin.startEmitting.await(30, TimeUnit.SECONDS));
        } finally {
            FailingPauseFieldPlugin.release();
        }

        Exception failure = expectThrows(Exception.class, () -> future.actionGet(60, TimeUnit.SECONDS).close());
        assertThat(EsqlTestUtils.unwrapIfWrappedInRemoteException(failure).getMessage(), containsString("Failing query"));
    }

    private void addFailingRuntimeField() throws IOException {
        try (XContentBuilder mapping = JsonXContent.contentBuilder()) {
            mapping.startObject()
                .startObject("runtime")
                .startObject("fail_me")
                .field("type", "long")
                .startObject("script")
                .field("source", "")
                .field("lang", "pause_fail")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
            assertAcked(client().admin().indices().preparePutMapping("events").setSource(mapping));
        }
    }

    private void assertShuffleMatches(String query, int partitionsPerNode) {
        assertThat(runSorted(query, shufflePragmas(partitionsPerNode)), equalTo(runSorted(query, Settings.EMPTY)));
    }

    private void assertShuffleJoinMatches(String query, int partitionsPerNode, boolean shuffleAgg) throws InterruptedException {
        assertShuffleJoinMatches(query, partitionsPerNode, shuffleAgg, true);
    }

    private void assertShuffleSubqueryJoinMatches(String query, int partitionsPerNode, boolean shuffleAgg) throws InterruptedException {
        assertShuffleJoinMatches(query, partitionsPerNode, shuffleAgg, true, "partitioned");
        assertShuffleJoinMatches(query, partitionsPerNode, shuffleAgg, shuffleAgg, "broadcast");
    }

    private void assertShuffleJoinMatches(String query, int partitionsPerNode, boolean shuffleAgg, boolean expectHashShuffle)
        throws InterruptedException {
        assertShuffleJoinMatches(query, partitionsPerNode, shuffleAgg, expectHashShuffle, "partitioned");
    }

    private void assertShuffleJoinMatches(
        String query,
        int partitionsPerNode,
        boolean shuffleAgg,
        boolean expectHashShuffle,
        String joinStrategy
    ) throws InterruptedException {
        QueryResult baseline = runWithWarnings(query, Settings.EMPTY);
        QueryResult shuffled = runWithWarnings(query, shuffleJoinPragmas(partitionsPerNode, shuffleAgg, joinStrategy));
        assertThat("result columns differ", shuffled.columns(), equalTo(baseline.columns()));
        assertThat("result rows differ", shuffled.rows(), equalTo(baseline.rows()));
        assertThat("warning messages differ", shuffled.warnings(), equalTo(baseline.warnings()));
        assertNotNull("baseline query profile missing", baseline.profile());
        assertNotNull("shuffle query profile missing", shuffled.profile());
        assertFalse("baseline unexpectedly ran a HASH shuffle stage", hasHashShufflePlan(baseline.profile()));
        assertEquals("unexpected HASH stage sink presence", expectHashShuffle, hasHashShufflePlan(shuffled.profile()));
        if ((expectHashShuffle || joinStrategy.equals("broadcast")) && shuffled.rows().isEmpty() == false) {
            assertTrue("shuffle query profile did not report rows sent to an exchange sink", hasStageExchangeSinkRows(shuffled.profile()));
            assertTrue(
                "shuffle query profile did not report rows emitted from an exchange source",
                hasStageExchangeSourceRows(shuffled.profile())
            );
        }
        if (joinStrategy.equals("broadcast")) {
            assertTrue("shuffle query profile did not include a BROADCAST stage sink", hasBroadcastShufflePlan(shuffled.profile()));
        }
    }

    private static boolean hasHashShufflePlan(EsqlQueryResponse.Profile profile) {
        return profile.plans().stream().anyMatch(plan -> plan.planTree().contains("StageSinkExec") && plan.planTree().contains("HASH"));
    }

    private static boolean hasBroadcastShufflePlan(EsqlQueryResponse.Profile profile) {
        return profile.plans()
            .stream()
            .anyMatch(plan -> plan.planTree().contains("StageSinkExec") && plan.planTree().contains("BROADCAST"));
    }

    private static boolean hasStageExchangeSinkRows(EsqlQueryResponse.Profile profile) {
        return profile.drivers()
            .stream()
            .filter(driver -> driver.description().startsWith("stage"))
            .flatMap(driver -> driver.operators().stream())
            .anyMatch(operator -> operator.status() instanceof ExchangeSinkOperator.Status status && status.rowsReceived() > 0);
    }

    private static boolean hasStageExchangeSourceRows(EsqlQueryResponse.Profile profile) {
        return profile.drivers()
            .stream()
            .filter(driver -> driver.description().startsWith("stage"))
            .flatMap(driver -> driver.operators().stream())
            .anyMatch(operator -> operator.status() instanceof ExchangeSourceOperator.Status status && status.rowsEmitted() > 0);
    }

    private Settings shufflePragmas(int partitionsPerNode) {
        return Settings.builder()
            .put(QueryPragmas.SHUFFLE_PARTITIONS_PER_NODE.getKey(), partitionsPerNode)
            .put(QueryPragmas.SHUFFLE_AGG.getKey(), true)
            .put(QueryPragmas.EXCHANGE_BUFFER_SIZE.getKey(), randomIntBetween(1, 10))
            .put(QueryPragmas.TASK_CONCURRENCY.getKey(), randomIntBetween(1, 4))
            .build();
    }

    private Settings breakerShufflePragmas() {
        return Settings.builder()
            .put(QueryPragmas.SHUFFLE_PARTITIONS_PER_NODE.getKey(), 1)
            .put(QueryPragmas.SHUFFLE_AGG.getKey(), true)
            .put(QueryPragmas.EXCHANGE_BUFFER_SIZE.getKey(), 1)
            .put(QueryPragmas.TASK_CONCURRENCY.getKey(), 1)
            .build();
    }

    private Settings shuffleJoinPragmas(int partitionsPerNode, boolean shuffleAgg) {
        return shuffleJoinPragmas(partitionsPerNode, shuffleAgg, "partitioned");
    }

    private Settings shuffleJoinPragmas(int partitionsPerNode, boolean shuffleAgg, String joinStrategy) {
        return Settings.builder()
            .put(QueryPragmas.SHUFFLE_PARTITIONS_PER_NODE.getKey(), partitionsPerNode)
            .put(QueryPragmas.SHUFFLE_JOIN.getKey(), true)
            .put(QueryPragmas.SHUFFLE_JOIN_STRATEGY.getKey(), joinStrategy)
            .put(QueryPragmas.SHUFFLE_AGG.getKey(), shuffleAgg)
            .put(QueryPragmas.EXCHANGE_BUFFER_SIZE.getKey(), randomIntBetween(1, 10))
            .put(QueryPragmas.TASK_CONCURRENCY.getKey(), randomIntBetween(1, 4))
            .build();
    }

    private List<List<Object>> runSorted(String query, Settings pragmas) {
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest(query).acceptedPragmaRisks(true).pragmas(new QueryPragmas(pragmas)))) {
            List<List<Object>> rows = new ArrayList<>(getValuesList(response));
            rows.sort(Comparator.comparing(Object::toString));
            return rows;
        }
    }

    private QueryResult runWithWarnings(String query, Settings pragmas) throws InterruptedException {
        DiscoveryNode coordinator = randomFrom(clusterService().state().nodes().stream().toList());
        var coordinatorThreadContext = internalCluster().getInstance(TransportService.class, coordinator.getName())
            .getThreadPool()
            .getThreadContext();
        coordinatorThreadContext.takeResponseHeaders("Warning");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<QueryResult> resultRef = new AtomicReference<>();
        AtomicReference<Exception> failureRef = new AtomicReference<>();
        EsqlQueryRequest request = syncEsqlQueryRequest(query).acceptedPragmaRisks(true).pragmas(new QueryPragmas(pragmas)).profile(true);
        client(coordinator.getName()).execute(EsqlQueryAction.INSTANCE, request, ActionListener.wrap(response -> {
            try {
                List<List<Object>> rows = new ArrayList<>(getValuesList(response));
                rows.sort(Comparator.comparing(Object::toString));
                List<String> columns = response.columns().stream().map(column -> column.name()).toList();
                // Ignore node-setting deprecations and compare the query's warning headers only.
                Set<String> warnings = coordinatorThreadContext.getResponseHeaders()
                    .getOrDefault("Warning", List.of())
                    .stream()
                    .filter(warning -> warning.startsWith("Line ") || warning.startsWith("No limit defined"))
                    .map(warning -> HeaderWarning.extractWarningValueFromWarningHeader(warning, false))
                    .map(ShuffleIT::normalizeJoinWarning)
                    .collect(Collectors.toSet());
                resultRef.set(new QueryResult(rows, columns, Set.copyOf(warnings), response.profile()));
            } catch (RuntimeException e) {
                failureRef.set(e);
            } finally {
                latch.countDown();
            }
        }, failure -> {
            failureRef.set(failure);
            latch.countDown();
        }));
        assertTrue("timed out waiting for ES|QL response", latch.await(60, TimeUnit.SECONDS));
        if (failureRef.get() != null) {
            throw new AssertionError("ES|QL query failed", failureRef.get());
        }
        QueryResult result = resultRef.get();
        if (result == null) {
            throw new AssertionError("ES|QL query completed without a result");
        }
        return result;
    }

    private static String normalizeJoinWarning(String warning) {
        String message = warning.replaceFirst("^Line -?\\d+:-?\\d+: ", "");
        if (message.contains("LOOKUP JOIN encountered multi-value")) {
            return "LOOKUP JOIN encountered multi-value";
        }
        if (message.startsWith("evaluation of [") || message.startsWith("evaluation of []")) {
            return "join evaluation failed for a multi-value key";
        }
        return message;
    }

    private record QueryResult(List<List<Object>> rows, List<String> columns, Set<String> warnings, EsqlQueryResponse.Profile profile) {}
}
