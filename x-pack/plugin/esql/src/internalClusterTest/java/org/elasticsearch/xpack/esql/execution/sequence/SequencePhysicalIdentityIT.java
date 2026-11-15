/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ShardDocSortField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.junit.After;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.idsQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;

/** Selected physical documents bound payload field evaluation even when routed IDs repeat. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequencePhysicalIdentityIT extends AbstractSequenceIntegTestCase {
    private static final String A = "sequence-physical-identity-a";
    private static final String B = "sequence-physical-identity-b";
    private static final Instant BASE = Instant.parse("2026-09-25T21:14:51.389Z");
    private final List<String> ownedIndices = new ArrayList<>();
    private String coordinator;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), SequenceMappedFieldsIT.FailingRuntimeFieldPlugin.class);
    }

    /** A predicate-excluded routed copy between selected positions must not evaluate an output-only runtime field. */
    public void testFullReplayFetchesOnlySelectedPhysicalDocuments() throws Exception {
        coordinator = Arrays.stream(internalCluster().getNodeNames()).sorted().findFirst().orElseThrow();
        createIndices();
        IndexRouting routing = IndexRouting.fromIndexMetadata(clusterService().state().metadata().getProject().index(A));
        String route = "physical-identity-route0";
        int shard = routing.getShard("middle", route);
        String otherRoute = null;
        for (int value = 1; value < 1000; value++) {
            String candidate = "physical-identity-route" + value;
            if (routing.getShard("middle", candidate) != shard) {
                otherRoute = candidate;
                break;
            }
        }
        assertNotNull(otherRoute);
        List<String> ids = List.of("first", "alpha", "middle", "omega");
        var bulk = client(coordinator).prepareBulk();
        bulk.add(prepareIndex(A).setId(ids.get(0)).setRouting(route).setSource(body(0, 0)));
        bulk.add(prepareIndex(A).setId(ids.get(1)).setRouting(route).setSource(body(1, 100)));
        bulk.add(
            prepareIndex(A).setId("middle")
                .setRouting(route)
                .setSource(Map.of("@timestamp", BASE.plusMillis(250).toString(), "key", "wrong", "disposition", "excluded"))
        );
        bulk.add(prepareIndex(A).setId(ids.get(3)).setRouting(route).setSource(body(3, 300)));
        bulk.add(prepareIndex(A).setId(ids.get(2)).setRouting(otherRoute).setSource(body(2, 200)));
        for (int event = 0; event < 4; event++) {
            bulk.add(prepareIndex(B).setId("B" + event).setSource(body(event, 1000 + event)));
        }
        assertFalse(bulk.get().hasFailures());
        refresh(A, B);
        assertPhysicalOrder();
        List<List<Object>> ordinary = new ArrayList<>();
        for (int event = 0; event < 4; event++) {
            ordinary.add(List.of("key-" + event, ids.get(event), 7L));
        }
        assertEquals(
            ordinary,
            query("FROM " + A + " METADATA _id | WHERE disposition==\"candidate\"" + " | KEEP key,_id,late_value | SORT key | LIMIT 10")
        );
        var wrong = expectThrows(
            RuntimeException.class,
            () -> query("FROM " + A + " | WHERE disposition==\"excluded\" | KEEP late_value | LIMIT 10")
        );
        assertThat(ExceptionsHelper.stackTrace(wrong), containsString("doesn't have a value"));
        for (int primary : List.of(1, 2)) {
            List<List<Object>> pruned = new ArrayList<>();
            List<List<Object>> demanded = new ArrayList<>();
            for (int event = 0; event < 4; event++) {
                String first = primary == 1 ? ids.get(event) : "B" + event;
                String second = primary == 1 ? "B" + event : ids.get(event);
                pruned.add(List.of("key-" + event, first, second));
                demanded.add(List.of("key-" + event, first, second, 8L, 0.0, 0.0));
            }
            assertEquals(pruned, query(sequence(primary, false)));
            // The independent runtime expression forces full replay. Assert raw scores: adding one would
            // round away Float.MIN_VALUE and fail to detect an accidentally scoring physical selector.
            assertEquals(demanded, query(sequence(primary, true), true));
        }
    }

    private void createIndices() {
        for (String index : List.of(A, B)) {
            assertFalse("fixture must not own an existing namespace", indexExists(index));
            ownedIndices.add(index);
            var mapping = new LinkedHashMap<String, Object>();
            mapping.put(
                "properties",
                Map.of(
                    "@timestamp",
                    Map.of("type", "date"),
                    "key",
                    Map.of("type", "keyword"),
                    "disposition",
                    Map.of("type", "keyword"),
                    "changing",
                    Map.of("type", "long")
                )
            );
            if (index.equals(A)) {
                mapping.put(
                    "runtime",
                    Map.of("late_value", Map.of("type", "long", "script", Map.of("lang", "sequence_failure", "source", "dependent")))
                );
            }
            assertAcked(client(coordinator).admin().indices().prepareCreate(index).setSettings(indexSettings(2, 0)).setMapping(mapping));
        }
        ensureGreen(A, B);
    }

    private static Map<String, Object> body(int event, long millis) {
        return Map.of("@timestamp", BASE.plusMillis(millis).toString(), "key", "key-" + event, "disposition", "candidate", "changing", 7L);
    }

    private static String sequence(int primary, boolean demand) {
        return """
            SEQUENCE BY key WITH {"maxspan":"30s"} PRIMARY %s
            (FROM sequence-physical-identity-a METADATA _id,_score | WHERE disposition=="candidate"
              | EVAL computed_value=late_value+1 | KEEP key,_id,computed_value,_score),
            (FROM sequence-physical-identity-b METADATA _id,_score | WHERE disposition=="candidate" | KEEP key,_id,_score)
            | EVAL other_id=MEMBER(%s,_id), other_score=MEMBER(%s,_score)%s
            | KEEP key,_id,other_id%s | SORT key | LIMIT 10
            """.formatted(
            primary,
            3 - primary,
            3 - primary,
            demand ? ",a_value=" + (primary == 1 ? "computed_value" : "MEMBER(1,computed_value)") : "",
            demand ? ",a_value,_score,other_score" : ""
        );
    }

    private List<List<Object>> query(String text) {
        return query(text, false);
    }

    private List<List<Object>> query(String text, boolean requirePayloadReplay) {
        try (
            var response = client(coordinator).execute(
                EsqlQueryAction.INSTANCE,
                syncEsqlQueryRequest(text).allowPartialResults(false).profile(true)
            ).actionGet()
        ) {
            assertFalse(response.isPartial());
            if (requirePayloadReplay) {
                assertNotNull(response.profile());
                var statuses = response.profile()
                    .drivers()
                    .stream()
                    .flatMap(driver -> driver.operators().stream())
                    .map(OperatorStatus::status)
                    .filter(SequenceSourceStatus.class::isInstance)
                    .map(SequenceSourceStatus.class::cast)
                    .toList();
                assertEquals(1, statuses.size());
                SequenceSourceStatus status = statuses.getFirst();
                assertTrue("raw scores must be checked after payload hydration", status.payloadSearchRequests() > 0);
                assertTrue("demanded output must execute beyond the two selection drivers", status.stageDrivers() > 2);
            }
            return getValuesList(response);
        }
    }

    /** Verify real document order independently of PIT shard enumeration and own the latest token until close. */
    private void assertPhysicalOrder() {
        BytesReference pit = new BytesArray(
            BytesRef.deepCopyOf(
                client(coordinator).execute(
                    TransportOpenPointInTimeAction.TYPE,
                    new OpenPointInTimeRequest(A).keepAlive(TimeValue.timeValueMinutes(2))
                ).actionGet().getPointInTimeId().toBytesRef()
            )
        );
        Throwable primary = null;
        try {
            var source = new SearchSourceBuilder().query(idsQuery().addIds("alpha", "middle", "omega"))
                .sort(new FieldSortBuilder(ShardDocSortField.NAME).order(SortOrder.ASC))
                .size(10)
                .fetchSource(true)
                .pointInTimeBuilder(new PointInTimeBuilder(pit).setKeepAlive(TimeValue.timeValueMinutes(2)));
            var request = new SearchRequest().source(source).allowPartialSearchResults(false);
            request.setPreFilterShardSize(Integer.MAX_VALUE);
            var response = client(coordinator).search(request).actionGet();
            try {
                if (response.pointInTimeId() != null) {
                    pit = new BytesArray(BytesRef.deepCopyOf(response.pointInTimeId().toBytesRef()));
                }
                assertFalse(response.isTimedOut());
                assertEquals(0, response.getFailedShards());
                assertEquals(4, response.getHits().getHits().length);
                SearchHit alpha = null, omega = null, selected = null, wrong = null;
                for (SearchHit hit : response.getHits().getHits()) {
                    switch (hit.getId()) {
                        case "alpha" -> alpha = hit;
                        case "omega" -> omega = hit;
                        case "middle" -> {
                            if (hit.getSourceAsMap().get("disposition").equals("excluded")) {
                                wrong = hit;
                            } else {
                                selected = hit;
                            }
                        }
                        default -> throw new AssertionError("unexpected physical control hit " + hit.getId());
                    }
                }
                assertNotNull(alpha);
                assertNotNull(omega);
                assertNotNull(selected);
                assertNotNull(wrong);
                assertEquals(alpha.getShard().getShardId(), wrong.getShard().getShardId());
                assertEquals(alpha.getShard().getShardId(), omega.getShard().getShardId());
                assertNotEquals(alpha.getShard().getShardId(), selected.getShard().getShardId());
                assertTrue((Long) alpha.getSortValues()[0] < (Long) wrong.getSortValues()[0]);
                assertTrue((Long) wrong.getSortValues()[0] < (Long) omega.getSortValues()[0]);
            } finally {
                response.decRef();
            }
        } catch (Exception | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            try {
                assertTrue(
                    client(coordinator).execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pit))
                        .actionGet()
                        .isSucceeded()
                );
            } catch (Exception | Error cleanup) {
                if (primary != null) {
                    if (primary != cleanup) {
                        primary.addSuppressed(cleanup);
                    }
                } else {
                    throw cleanup;
                }
            }
        }
    }

    /** Release actual query resources before deleting only the names this fixture claimed. */
    @After
    public void released() throws Exception {
        assertBusy(() -> {
            for (SearchService service : internalCluster().getInstances(SearchService.class)) {
                assertEquals(0, service.getActiveContexts());
            }
            for (CircuitBreakerService service : internalCluster().getInstances(CircuitBreakerService.class)) {
                assertEquals(0L, service.getBreaker(CircuitBreaker.REQUEST).getUsed());
            }
            assertTrue(
                client(coordinator).admin()
                    .cluster()
                    .prepareListTasks()
                    .setActions(
                        "indices:data/read/esql*",
                        "indices:data/read/search*",
                        TransportOpenPointInTimeAction.TYPE.name(),
                        TransportClosePointInTimeAction.TYPE.name()
                    )
                    .get()
                    .getTasks()
                    .isEmpty()
            );
        });
        ensureBlocksReleased();
        for (String index : ownedIndices) {
            if (indexExists(index)) {
                assertAcked(client(coordinator).admin().indices().prepareDelete(index));
            }
        }
    }
}
