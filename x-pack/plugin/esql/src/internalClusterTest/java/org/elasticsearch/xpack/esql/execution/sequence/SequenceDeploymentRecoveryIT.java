/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.ReadOnlyEngine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.action.SequenceProvenance;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.junit.After;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

/** Real transport and HTTP boundaries after the selected documents have entered the sequence snapshot. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceDeploymentRecoveryIT extends AbstractSequenceIntegTestCase {
    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(45);
    private static final Instant BASE = Instant.parse("2026-11-13T00:00:00Z");
    private final List<String> ownedIndices = new ArrayList<>();

    /** The fixture must select a real read-only engine, rather than a mock engine or merely a write block. */
    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    /** HTTP failures are checked through the same client boundary that a detection runner receives. */
    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        assertTrue(plugins.removeIf(EsqlPlugin.class::isAssignableFrom));
        plugins.add(SequenceFailureIT.SequenceBlockFactoryPlugin.class);
        plugins.add(InternalExchangePlugin.class);
        plugins.add(MockTransportService.TestPlugin.class);
        plugins.add(ReadOnlyFixturePlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int ordinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(ordinal, otherSettings))
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "128mb")
            .put(
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(),
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getDefault(Settings.EMPTY)
            )
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(), 1.0)
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_SIZE_SETTING, "0b")
            .put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_MAX_SIZE_SETTING, "0b")
            .build();
    }

    /** Relocation between selection and payload dispatch must reconstruct the identical read-only searcher after reader loss. */
    public void testReadOnlyPitRecoversAfterRelocationAndReaderLoss() throws Exception {
        assertRelocationAndReaderLoss(true);
    }

    /** A write block alone does not make an ordinary engine's PIT recoverable after its original reader is lost. */
    public void testWriteBlockedOrdinaryEnginePitFailsAfterRelocationAndReaderLoss() throws Exception {
        assertRelocationAndReaderLoss(false);
    }

    private void assertRelocationAndReaderLoss(boolean readOnly) throws Exception {
        String[] nodes = dataNodes();
        Fixture fixture = createFixture(readOnly ? "sequence-deploy-readonly" : "sequence-deploy-ordinary", nodes);
        for (String index : List.of(fixture.first, fixture.second)) {
            assertEquals(0, flush(index).getFailedShards());
            assertAcked(indicesAdmin().prepareClose(index));
            assertAcked(
                indicesAdmin().prepareUpdateSettings(index)
                    .setSettings(Settings.builder().put("index.blocks.write", true).put(ReadOnlyFixturePlugin.READ_ONLY.getKey(), readOnly))
            );
            assertAcked(indicesAdmin().prepareOpen(index));
        }
        ensureGreen(fixture.first, fixture.second);
        String initialSearcher = assertSearcher(fixture.first, nodes[0], readOnly);
        assertSearcher(fixture.second, nodes[1], readOnly);
        assertResult(fixture, client(nodes[0]).execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query(fixture))).actionGet(TIMEOUT));
        assertResourcesReleased();

        ActionFuture<EsqlQueryResponse> future = null;
        try (var gate = new PayloadGate(fixture.first)) {
            future = client(nodes[0]).execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query(fixture)));
            gate.awaitStarted(future);
            assertNotNull(gate.readerId);
            assertEquals(readOnly, gate.readerId.isRetryable());
            assertEquals(initialSearcher, gate.readerId.getSearcherId());
            assertEquals(nodes[0], gate.firstNode);

            assertAcked(
                indicesAdmin().prepareUpdateSettings(fixture.first)
                    .setSettings(Settings.builder().put("index.routing.allocation.require._name", nodes[1]))
            );
            ensureGreen(fixture.first, fixture.second);
            assertBusy(() -> {
                String primary = clusterService().state()
                    .nodes()
                    .get(clusterService().state().routingTable().index(fixture.first).shard(0).primaryShard().currentNodeId())
                    .getName();
                assertEquals(nodes[1], primary);
                var former = internalCluster().getInstance(IndicesService.class, nodes[0]).indexService(resolveIndex(fixture.first));
                assertTrue("the old node must no longer own the relocated shard", former == null || former.getShardOrNull(0) == null);
            });
            assertEquals(initialSearcher, assertSearcher(fixture.first, nodes[1], readOnly));

            // Relocation intentionally permits the original PIT reader to remain open. Explicitly lose that
            // exact reader so a success cannot be explained by continuing to read the old node's snapshot.
            SearchService formerReaders = internalCluster().getInstance(SearchService.class, nodes[0]);
            int retained = formerReaders.getActiveContexts();
            assertEquals("the original PIT reader must exist before the test explicitly evicts it", 1, retained);
            assertTrue("the exact original PIT reader must be explicitly freed", formerReaders.freeReaderContext(gate.readerId));
            assertEquals(0, formerReaders.getActiveContexts());
            logger.info("SEQUENCE relocation fixture read_only={} old_readers_before_eviction={}", readOnly, retained);
            gate.release();
            if (readOnly) {
                EsqlQueryResponse response = future.actionGet(TIMEOUT);
                future = null;
                assertResult(fixture, response);
                assertTrue(
                    "a replacement-node search must use the identical retryable searcher",
                    gate.attempts.stream()
                        .anyMatch(a -> a.node.equals(nodes[1]) && a.readerId != null && a.readerId.sameSearcherIdsAs(initialSearcher))
                );
            } else {
                var pending = future;
                SearchPhaseExecutionException failure = expectThrows(SearchPhaseExecutionException.class, () -> pending.actionGet(TIMEOUT));
                future = null;
                // A nonretryable PIT keeps its old routing; search aggregation reports the unavailable original shard.
                assertEquals("query", failure.getPhaseName());
                assertEquals(1, failure.shardFailures().length);
                var shardFailure = failure.shardFailures()[0];
                assertTrue(shardFailure.getCause() instanceof NoShardAvailableActionException);
                assertEquals(fixture.first, shardFailure.shard().getShardId().getIndexName());
                assertEquals(0, shardFailure.shard().getShardId().id());
                assertEquals(
                    internalCluster().getInstance(ClusterService.class, nodes[0]).localNode().getId(),
                    shardFailure.shard().getNodeId()
                );
                assertTrue(
                    "a nonretryable PIT must not dispatch payload to the replacement node",
                    gate.attempts.stream().noneMatch(attempt -> attempt.node.equals(nodes[1]))
                );
                logger.info(
                    "SEQUENCE nonretryable relocation failed with {} for {}",
                    shardFailure.getCause().getClass(),
                    shardFailure.shard()
                );
            }
        } finally {
            drain(future);
        }
        assertResourcesReleased();
        assertResult(fixture, client(nodes[1]).execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query(fixture))).actionGet(TIMEOUT));
    }

    /** A shard-side producer mismatch is fatal even when the caller permits partial results. */
    public void testMappedIntegralAliasTypeChangeReturnsHttpBadRequestWithPartialResults() throws Exception {
        assertMappedTypeHttpFailure(true);
    }

    /** The same producer mismatch has the same client boundary when partial results are disabled. */
    public void testMappedIntegralAliasTypeChangeReturnsHttpBadRequestWithoutPartialResults() throws Exception {
        assertMappedTypeHttpFailure(false);
    }

    private void assertMappedTypeHttpFailure(boolean partial) throws Exception {
        Fixture fixture = createFixture("sequence-deploy-type-" + partial, dataNodes());
        assertResult(fixture, client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query(fixture))).actionGet(TIMEOUT));
        assertResourcesReleased();
        var future = new PlainActionFuture<Response>();
        Cancellable cancellable = null;
        try (var gate = new PayloadGate(fixture.first)) {
            Request request = new Request("POST", "/_query");
            request.addParameter("allow_partial_results", Boolean.toString(partial));
            request.addParameter("error_trace", "true");
            request.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().startObject().field("query", query(fixture)).endObject()));
            cancellable = getRestClient().performRequestAsync(request, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    future.onResponse(response);
                }

                @Override
                public void onFailure(Exception failure) {
                    future.onFailure(failure);
                }
            });
            gate.awaitStarted(future);
            assertFalse("a fatal shard-side conversion must never become successful partial output", gate.allowPartialSearchResults);
            assertTrue(
                "the request must demand an integral value before its live alias changes",
                gate.source.ext()
                    .stream()
                    .filter(SequenceDocValuesSearchExt.class::isInstance)
                    .map(SequenceDocValuesSearchExt.class::cast)
                    .flatMap(ext -> ext.fields().stream())
                    .anyMatch(field -> field.field.equals("value") && SequenceHitReader.INTEGRAL_FORMAT.equals(field.format))
            );
            setValueAlias(fixture.first, "changed_value");
            gate.release();
            ExecutionException failure = expectThrows(ExecutionException.class, () -> future.get(TIMEOUT.millis(), TimeUnit.MILLISECONDS));
            assertTrue(failure.getCause() instanceof ResponseException);
            Response response = ((ResponseException) failure.getCause()).getResponse();
            String body = EntityUtils.toString(response.getEntity());
            logger.info(
                "SEQUENCE mapped-type HTTP response partial={} status={} body={}",
                partial,
                response.getStatusLine().getStatusCode(),
                body
            );
            assertEquals(body, RestStatus.BAD_REQUEST.getStatus(), response.getStatusLine().getStatusCode());
            assertThat(body, containsString("SEQUENCE integral field [value] no longer has a numeric type"));
            assertThat(
                "the failure must originate in the data-node conversion boundary",
                body,
                containsString("SequenceDocValuesFetchSubPhase")
            );
            Map<String, Object> parsed = XContentHelper.convertToMap(JsonXContent.jsonXContent, body, false);
            assertEquals(RestStatus.BAD_REQUEST.getStatus(), parsed.get("status"));
            assertFalse(parsed.containsKey("values"));
            assertFalse(parsed.containsKey("columns"));
            assertTrue(parsed.get("error") instanceof Map<?, ?>);
            Map<?, ?> error = (Map<?, ?>) parsed.get("error");
            assertEquals("search_phase_execution_exception", error.get("type"));
            assertTrue(error.get("root_cause") instanceof List<?>);
            List<?> rootCauses = (List<?>) error.get("root_cause");
            assertFalse(rootCauses.isEmpty());
            for (Object rootCause : rootCauses) {
                assertTrue(rootCause instanceof Map<?, ?>);
                assertEquals("illegal_argument_exception", ((Map<?, ?>) rootCause).get("type"));
            }
            assertThat(rootCauses.toString(), containsString("no longer has a numeric type"));
            logger.info(
                "SEQUENCE mapped-type HTTP failure partial={} top_type={} root_cause={}",
                partial,
                error.get("type"),
                error.get("root_cause")
            );
        } finally {
            try {
                if (cancellable != null) {
                    if (future.isDone() == false) {
                        cancellable.cancel();
                    }
                    try {
                        EntityUtils.consume(future.get(TIMEOUT.millis(), TimeUnit.MILLISECONDS).getEntity());
                    } catch (ExecutionException failure) {
                        if (failure.getCause() instanceof ResponseException response) {
                            EntityUtils.consume(response.getResponse().getEntity());
                        }
                    }
                }
            } finally {
                setValueAlias(fixture.first, "raw_value");
            }
        }
        assertResourcesReleased();
        assertResult(fixture, client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query(fixture))).actionGet(TIMEOUT));
    }

    /** A demanded textual alias cannot silently stringify a numeric producer after selection. */
    public void testMappedTextAliasTypeChangeReturnsHttpBadRequestWithPartialResults() throws Exception {
        assertMappedTextHttpFailure(true);
    }

    /** Caller partial-result preferences do not turn a coordinator conversion failure into rows. */
    public void testMappedTextAliasTypeChangeReturnsHttpBadRequestWithoutPartialResults() throws Exception {
        assertMappedTextHttpFailure(false);
    }

    private void assertMappedTextHttpFailure(boolean partial) throws Exception {
        Fixture fixture = createFixture("sequence-deploy-text-" + partial, dataNodes());
        setValueAlias(fixture.first, "payload");
        assertResult(
            fixture,
            client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query(fixture))).actionGet(TIMEOUT),
            "anchor"
        );
        assertResourcesReleased();
        var future = new PlainActionFuture<Response>();
        Cancellable cancellable = null;
        try (var gate = new PayloadGate(fixture.first)) {
            Request request = new Request("POST", "/_query");
            request.addParameter("allow_partial_results", Boolean.toString(partial));
            request.addParameter("error_trace", "true");
            request.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().startObject().field("query", query(fixture)).endObject()));
            cancellable = getRestClient().performRequestAsync(request, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    future.onResponse(response);
                }

                @Override
                public void onFailure(Exception failure) {
                    future.onFailure(failure);
                }
            });
            gate.awaitStarted(future);
            assertFalse("payload reads cannot return successful partial shards", gate.allowPartialSearchResults);
            assertTrue(
                "the frozen demand must be textual before its live alias changes",
                gate.source.ext()
                    .stream()
                    .filter(SequenceDocValuesSearchExt.class::isInstance)
                    .map(SequenceDocValuesSearchExt.class::cast)
                    .flatMap(ext -> ext.fields().stream())
                    .anyMatch(field -> field.field.equals("value") && field.format == null)
            );
            setValueAlias(fixture.first, "raw_value");
            gate.release();
            ExecutionException failure = expectThrows(ExecutionException.class, () -> future.get(TIMEOUT.millis(), TimeUnit.MILLISECONDS));
            assertTrue(failure.getCause() instanceof ResponseException);
            Response response = ((ResponseException) failure.getCause()).getResponse();
            String body = EntityUtils.toString(response.getEntity());
            logger.info(
                "SEQUENCE native-text HTTP response partial={} status={} body={}",
                partial,
                response.getStatusLine().getStatusCode(),
                body
            );
            assertEquals(body, RestStatus.BAD_REQUEST.getStatus(), response.getStatusLine().getStatusCode());
            assertThat(body, containsString("SEQUENCE mapped [keyword] values require native textual bytes"));
            assertThat("the conversion must be checked on the coordinator's frozen demand", body, containsString("SequenceHitReader"));
            Map<String, Object> parsed = XContentHelper.convertToMap(JsonXContent.jsonXContent, body, false);
            assertEquals(RestStatus.BAD_REQUEST.getStatus(), parsed.get("status"));
            assertFalse(parsed.containsKey("values"));
            assertFalse(parsed.containsKey("columns"));
            assertTrue(parsed.get("error") instanceof Map<?, ?>);
            Map<?, ?> error = (Map<?, ?>) parsed.get("error");
            assertEquals("illegal_argument_exception", error.get("type"));
            assertTrue(error.get("root_cause") instanceof List<?>);
            List<?> rootCauses = (List<?>) error.get("root_cause");
            assertFalse(rootCauses.isEmpty());
            for (Object rootCause : rootCauses) {
                assertTrue(rootCause instanceof Map<?, ?>);
                assertEquals("illegal_argument_exception", ((Map<?, ?>) rootCause).get("type"));
            }
            assertThat(rootCauses.toString(), containsString("require native textual bytes"));
            logger.info("SEQUENCE native-text HTTP failure partial={} top_type={} root_cause={}", partial, error.get("type"), rootCauses);
        } finally {
            try {
                if (cancellable != null) {
                    if (future.isDone() == false) {
                        cancellable.cancel();
                    }
                    try {
                        EntityUtils.consume(future.get(TIMEOUT.millis(), TimeUnit.MILLISECONDS).getEntity());
                    } catch (ExecutionException failure) {
                        if (failure.getCause() instanceof ResponseException response) {
                            EntityUtils.consume(response.getResponse().getEntity());
                        }
                    }
                }
            } finally {
                setValueAlias(fixture.first, "raw_value");
            }
        }
        assertResourcesReleased();
        assertResult(fixture, client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query(fixture))).actionGet(TIMEOUT));
    }

    private Fixture createFixture(String prefix, String[] nodes) {
        String first = prefix + "-a";
        String second = prefix + "-b";
        for (int ordinal = 0; ordinal < 2; ordinal++) {
            String index = ordinal == 0 ? first : second;
            assertFalse(indexExists(index));
            ownedIndices.add(index);
            assertAcked(
                indicesAdmin().prepareCreate(index)
                    .setSettings(
                        indexSettings(1, 0).put("index.mode", "standard").put("index.routing.allocation.require._name", nodes[ordinal])
                    )
                    .setMapping(
                        Map.of(
                            "properties",
                            Map.of(
                                "@timestamp",
                                Map.of("type", "date"),
                                "key",
                                Map.of("type", "keyword"),
                                "payload",
                                Map.of("type", "keyword"),
                                "raw_value",
                                Map.of("type", "long"),
                                "changed_value",
                                Map.of("type", "date"),
                                "value",
                                Map.of("type", "alias", "path", "raw_value")
                            )
                        )
                    )
            );
            prepareIndex(index).setId(ordinal == 0 ? "A" : "B")
                .setSource(
                    "@timestamp",
                    BASE.plusMillis(ordinal * 1000L).toString(),
                    "key",
                    "shared",
                    "payload",
                    ordinal == 0 ? "anchor" : "completion",
                    "raw_value",
                    7,
                    "changed_value",
                    BASE.toString()
                )
                .get();
        }
        refresh(first, second);
        ensureGreen(first, second);
        return new Fixture(first, second, client().prepareGet(first, "A").get().getSource());
    }

    private static String query(Fixture fixture) {
        return """
            SEQUENCE BY key WITHIN 10s PRIMARY 1
                (FROM %s METADATA _id,_source | EVAL computed=TO_UPPER(payload) | KEEP key,value,computed,_id,_source),
                (FROM %s METADATA _id | KEEP key,payload,_id)
            | EVAL member_payload=MEMBER(2,payload), member_id=MEMBER(2,_id)
            | KEEP key,value,computed,member_payload,_id,member_id,_source,sequence_provenance | LIMIT 1
            """.formatted(fixture.first, fixture.second);
    }

    /** The selected identities, old ordinary values and explicitly requested source survive replay and relocation. */
    private void assertResult(Fixture fixture, EsqlQueryResponse response) throws Exception {
        assertResult(fixture, response, 7L);
    }

    private void assertResult(Fixture fixture, EsqlQueryResponse response, Object expectedValue) throws Exception {
        try (response) {
            assertFalse(response.isPartial());
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertEquals(List.of("shared", expectedValue, "ANCHOR", "completion", "A", "B"), rows.getFirst().subList(0, 6));
            List<String> names = response.columns().stream().map(column -> column.name()).toList();
            int source = names.indexOf("_source");
            int provenance = names.indexOf("sequence_provenance");
            assertTrue(source >= 0 && provenance >= 0);
            assertEquals(fixture.firstSource, rows.getFirst().get(source));
            assertEquals(
                List.of(new SequenceProvenance.Member(1, fixture.first, "A"), new SequenceProvenance.Member(2, fixture.second, "B")),
                SequenceProvenance.decode(rows.getFirst().get(provenance))
            );
        }
    }

    private String[] dataNodes() {
        String[] nodes = Arrays.stream(internalCluster().getNodeNames())
            .filter(node -> internalCluster().getInstance(ClusterService.class, node).localNode().canContainData())
            .sorted()
            .toArray(String[]::new);
        assertEquals(2, nodes.length);
        return nodes;
    }

    private String assertSearcher(String index, String node, boolean readOnly) {
        IndexShard shard = internalCluster().getInstance(IndicesService.class, node).indexServiceSafe(resolveIndex(index)).getShard(0);
        ReadOnlyFixturePlugin plugin = internalCluster().getInstance(PluginsService.class, node)
            .filterPlugins(ReadOnlyFixturePlugin.class)
            .findFirst()
            .orElseThrow();
        try (Engine.SearcherSupplier searcher = shard.acquireExternalSearcherSupplier(SplitShardCountSummary.IRRELEVANT)) {
            if (readOnly) {
                ReadOnlyEngine engine = plugin.engines.get(index);
                assertNotNull("the plugin must have constructed an actual ReadOnlyEngine", engine);
                assertNotNull(searcher.getSearcherId());
                assertEquals(engine.getCommitId(), searcher.getSearcherId());
            } else {
                assertNull(plugin.engines.get(index));
                assertNull("write-blocked ordinary engines do not publish a retryable searcher ID", searcher.getSearcherId());
            }
            return searcher.getSearcherId();
        }
    }

    private void setValueAlias(String index, String target) {
        assertAcked(
            indicesAdmin().preparePutMapping(index)
                .setSource(Map.of("properties", Map.of("value", Map.of("type", "alias", "path", target))))
        );
    }

    /** Check release before deleting the fixture, so deletion cannot disguise a leaked reader. */
    private void assertResourcesReleased() throws Exception {
        assertBusy(() -> {
            for (SearchService service : internalCluster().getInstances(SearchService.class)) {
                assertEquals(0, service.getActiveContexts());
            }
            assertThat(
                clusterAdmin().prepareListTasks()
                    .setActions(EsqlQueryAction.NAME, DriverTaskRunner.ACTION_NAME, TransportSearchAction.TYPE.name() + "*")
                    .get()
                    .getTasks(),
                empty()
            );
        });
        ensureBlocksReleased();
    }

    /** Preserve leak evidence before deleting indices, and release fixture-owned engine references afterwards. */
    @After
    public void releaseFixtures() throws Exception {
        try {
            assertResourcesReleased();
        } finally {
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService.getInstance(node).clearAllRules();
                internalCluster().getInstance(PluginsService.class, node)
                    .filterPlugins(ReadOnlyFixturePlugin.class)
                    .forEach(plugin -> plugin.engines.clear());
            }
            for (String index : ownedIndices) {
                if (indexExists(index)) {
                    assertAcked(indicesAdmin().prepareDelete(index));
                }
            }
            ownedIndices.clear();
        }
    }

    private static void drain(ActionFuture<EsqlQueryResponse> future) {
        if (future != null) {
            try {
                future.actionGet(TIMEOUT).close();
            } catch (Exception failure) {
                assertTrue("the gated query must terminate before cleanup returns", future.isDone());
            }
        }
    }

    private record Fixture(String first, String second, Map<String, Object> firstSource) {}

    /** Selects a real read-only engine only for explicitly closed/reopened test indices. */
    public static class ReadOnlyFixturePlugin extends Plugin implements EnginePlugin, ActionPlugin {
        static final Setting<Boolean> READ_ONLY = Setting.boolSetting(
            "index.sequence_deployment.read_only_engine",
            false,
            Setting.Property.IndexScope
        );
        private final Map<String, ReadOnlyEngine> engines = new ConcurrentHashMap<>();
        private volatile ThreadPool threadPool;

        @Override
        public Collection<?> createComponents(PluginServices services) {
            threadPool = services.threadPool();
            return List.of();
        }

        @Override
        public Collection<ActionFilter> getActionFilters() {
            return List.of(new ActionFilter() {
                @Override
                public int order() {
                    return Integer.MAX_VALUE;
                }

                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                    Task task,
                    String action,
                    Request request,
                    ActionListener<Response> listener,
                    ActionFilterChain<Request, Response> chain
                ) {
                    PayloadGate gate = PayloadGate.ACTIVE.get();
                    if (gate == null
                        || action.equals(TransportSearchAction.NAME) == false
                        || request instanceof SearchRequest == false
                        || gate.claim((SearchRequest) request) == false) {
                        chain.proceed(task, action, request, listener);
                        return;
                    }
                    request.mustIncRef();
                    var terminal = ActionListener.notifyOnce(ActionListener.runAfter(listener, () -> {
                        request.decRef();
                        gate.handled.countDown();
                    }));
                    try {
                        Supplier<ThreadContext.StoredContext> context = threadPool.getThreadContext().newRestorableContext(true);
                        gate.install(() -> {
                            try (var ignored = context.get()) {
                                chain.proceed(task, action, request, terminal);
                            } catch (Exception failure) {
                                terminal.onFailure(failure);
                            }
                        });
                    } catch (Exception failure) {
                        terminal.onFailure(failure);
                    }
                }
            });
        }

        @Override
        public List<Setting<?>> getSettings() {
            return List.of(READ_ONLY);
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings settings) {
            if (READ_ONLY.get(settings.getSettings()) == false) {
                return Optional.empty();
            }
            return Optional.of(config -> {
                var engine = new ReadOnlyEngine(
                    config,
                    null,
                    config.getTranslogConfig().hasTranslog() ? null : new TranslogStats(0, 0, 0, 0, 0),
                    true,
                    Function.identity(),
                    true,
                    true
                );
                engines.put(settings.getIndex().getName(), engine);
                return engine;
            });
        }
    }

    /** Parks payload dispatch before routing; actual candidate and replacement-shard requests remain observable. */
    private final class PayloadGate implements AutoCloseable {
        private static final AtomicReference<PayloadGate> ACTIVE = new AtomicReference<>();
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch handled = new CountDownLatch(1);
        private final AtomicBoolean claimed = new AtomicBoolean();
        private final List<Attempt> attempts = new CopyOnWriteArrayList<>();
        private final String index;
        private volatile boolean closed;
        private volatile ShardSearchContextId readerId;
        private volatile String firstNode;
        private volatile SearchSourceBuilder source;
        private boolean allowPartialSearchResults;
        private Runnable continuation;

        private PayloadGate(String index) {
            this.index = index;
            assertTrue("only one query owns the deployment gate", ACTIVE.compareAndSet(null, this));
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService transport = MockTransportService.getInstance(node);
                transport.<ShardSearchRequest>addRequestHandlingBehavior(
                    SearchTransportService.QUERY_ACTION_NAME,
                    (handler, request, channel, task) -> {
                        if (request.shardId().getIndexName().equals(index)) {
                            if (readerId == null && request.readerId() != null) {
                                readerId = request.readerId();
                                firstNode = node;
                            }
                            if (request.source() != null
                                && request.source().fetchSource() != null
                                && request.source().fetchSource().fetchSource()) {
                                attempts.add(new Attempt(node, request.readerId()));
                            }
                        }
                        handler.messageReceived(request, channel, task);
                    }
                );
            }
        }

        private boolean claim(SearchRequest request) {
            boolean payload = request.getPointInTimeAuthorizationScope() != null
                && Arrays.asList(request.getPointInTimeAuthorizationScope().indices()).contains(index)
                && request.source() != null
                && request.source().fetchSource() != null
                && request.source().fetchSource().fetchSource();
            if (closed || payload == false || claimed.compareAndSet(false, true) == false) {
                return false;
            }
            source = request.source().shallowCopy();
            allowPartialSearchResults = request.allowPartialSearchResults();
            return true;
        }

        /** Closing before installation still runs the continuation, so no request reference is stranded. */
        private void install(Runnable next) {
            boolean run;
            synchronized (this) {
                run = closed;
                if (run == false) {
                    continuation = next;
                }
            }
            started.countDown();
            if (run) {
                next.run();
            }
        }

        private void awaitStarted(ActionFuture<?> future) throws Exception {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT.millis());
            while (started.await(50, TimeUnit.MILLISECONDS) == false) {
                if (future.isDone()) {
                    future.actionGet(TIMEOUT);
                    fail("query completed before reaching its payload gate");
                }
                assertTrue("payload gate was not reached", System.nanoTime() - deadline < 0);
            }
        }

        private void release() {
            Runnable next;
            synchronized (this) {
                next = continuation;
                continuation = null;
            }
            if (next != null) {
                next.run();
            }
        }

        @Override
        public void close() {
            closed = true;
            ACTIVE.compareAndSet(this, null);
            release();
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService.getInstance(node).clearAllRules();
            }
            if (claimed.get()) {
                safeAwait(handled, TIMEOUT);
            }
        }
    }

    private record Attempt(String node, ShardSearchContextId readerId) {}
}
