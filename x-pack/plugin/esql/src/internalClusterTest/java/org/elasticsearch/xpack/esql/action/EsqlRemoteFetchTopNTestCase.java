/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperatorStatus;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.plugin.RemoteFetchOperator;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public abstract class EsqlRemoteFetchTopNTestCase extends AbstractEsqlIntegTestCase {
    private String indexName;

    @BeforeClass
    public static void checkSnapshot() {
        assumeTrue("remote_fetch_topn is an experimental query pragma", Build.current().isSnapshot());
    }

    @Before
    public void setupIndex() {
        indexName = "remote_fetch_topn_" + getTestName().toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "_");
        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(indexSettings(4, 0))
            .setMapping(
                "unique_sort",
                "type=long",
                "sorted",
                "type=long",
                "tie_breaker",
                "type=long",
                "payload",
                "type=keyword",
                "source_payload",
                "type=keyword,doc_values=false",
                "category",
                "type=keyword",
                "metric",
                "type=long",
                "_remote_fetch_handle",
                "type=keyword"
            )
            .get();

        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 64; i++) {
            bulk.add(
                prepareIndex(indexName).setId(Integer.toString(i))
                    .setSource(
                        Map.of(
                            "unique_sort",
                            i,
                            "sorted",
                            i / 4,
                            "tie_breaker",
                            i % 4,
                            "payload",
                            "payload-" + i,
                            "source_payload",
                            "source-payload-" + i,
                            "category",
                            "cat-" + (i % 5),
                            "metric",
                            i * 10L,
                            "_remote_fetch_handle",
                            "user-handle-" + i
                        )
                    )
            );
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
    }

    public void testBasicRemoteFetchTopN() {
        try (
            EsqlQueryResponse response = runQuery(
                "FROM " + indexName + " | SORT unique_sort + 1 DESC | LIMIT 5 | KEEP unique_sort, payload, category",
                true
            )
        ) {
            assertThat(
                EsqlTestUtils.getValuesList(response),
                equalTo(
                    List.of(
                        List.of(63L, "payload-63", "cat-3"),
                        List.of(62L, "payload-62", "cat-2"),
                        List.of(61L, "payload-61", "cat-1"),
                        List.of(60L, "payload-60", "cat-0"),
                        List.of(59L, "payload-59", "cat-4")
                    )
                )
            );
            assertRemoteFetchRows(response, 5);
            assertFieldLoadedBeforeFetch(response, "unique_sort");
            assertFieldNotLoadedBeforeFetch(response, "payload");
            assertFieldNotLoadedBeforeFetch(response, "category");
        }
    }

    public void testMultipleSortKeysRemoteFetchTopN() {
        try (
            EsqlQueryResponse response = runQuery(
                "FROM " + indexName + " | SORT sorted + 0 DESC, tie_breaker + 0 ASC | LIMIT 7 | KEEP sorted, tie_breaker, payload, metric",
                true
            )
        ) {
            assertThat(
                EsqlTestUtils.getValuesList(response),
                equalTo(
                    List.of(
                        List.of(15L, 0L, "payload-60", 600L),
                        List.of(15L, 1L, "payload-61", 610L),
                        List.of(15L, 2L, "payload-62", 620L),
                        List.of(15L, 3L, "payload-63", 630L),
                        List.of(14L, 0L, "payload-56", 560L),
                        List.of(14L, 1L, "payload-57", 570L),
                        List.of(14L, 2L, "payload-58", 580L)
                    )
                )
            );
            assertRemoteFetchRows(response, 7);
            assertFieldLoadedBeforeFetch(response, "sorted");
            assertFieldLoadedBeforeFetch(response, "tie_breaker");
            assertFieldNotLoadedBeforeFetch(response, "payload");
            assertFieldNotLoadedBeforeFetch(response, "metric");
        }
    }

    public void testBinaryRemoteFetchHandlesSurviveTopN() {
        String regressionIndex = indexName + "_binary_handles";
        client().admin()
            .indices()
            .prepareCreate(regressionIndex)
            .setSettings(indexSettings(1, 0))
            .setMapping("content", "type=text", "unique_sort", "type=long", "payload", "type=keyword")
            .get();

        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 320; i++) {
            bulk.add(
                prepareIndex(regressionIndex).setId(Integer.toString(i))
                    .setSource("content", "industrial revolution", "unique_sort", i, "payload", "payload-" + i)
            );
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        // Remote-fetch handles are arbitrary binary values. This regression test sends enough handles through TopN to ensure that
        // they are not interpreted or corrupted as UTF-8. The secondary sort key also gives the profile a normal eagerly loaded field.
        try (
            EsqlQueryResponse response = runQuery(
                "FROM "
                    + regressionIndex
                    + " METADATA _score"
                    + " | WHERE MATCH(content, \"industrial revolution\")"
                    + " | SORT _score DESC, unique_sort + 0 DESC"
                    + " | LIMIT 300"
                    + " | KEEP payload",
                true
            )
        ) {
            assertThat(EsqlTestUtils.getValuesList(response), hasSize(300));
            assertRemoteFetchRows(response, 300);
            assertFieldLoadedBeforeFetch(response, "unique_sort");
            assertFieldNotLoadedBeforeFetch(response, "payload");
        }
    }

    public void testRemoteFetchTopNDisabledWithoutPragma() {
        try (
            EsqlQueryResponse response = runQuery(
                "FROM " + indexName + " | SORT unique_sort + 1 DESC | LIMIT 5 | KEEP unique_sort, payload, category",
                false
            )
        ) {
            assertThat(
                EsqlTestUtils.getValuesList(response),
                equalTo(
                    List.of(
                        List.of(63L, "payload-63", "cat-3"),
                        List.of(62L, "payload-62", "cat-2"),
                        List.of(61L, "payload-61", "cat-1"),
                        List.of(60L, "payload-60", "cat-0"),
                        List.of(59L, "payload-59", "cat-4")
                    )
                )
            );
            assertThat(remoteFetchStatuses(response), empty());
            assertFieldLoadedBeforeFetch(response, "unique_sort");
            assertFieldLoadedBeforeFetch(response, "payload");
            assertFieldLoadedBeforeFetch(response, "category");
        }
    }

    public void testUserFieldNamedLikeRemoteFetchHandle() {
        try (
            EsqlQueryResponse response = runQuery(
                "FROM " + indexName + " | SORT unique_sort + 1 DESC | LIMIT 3 | KEEP `_remote_fetch_handle`, payload",
                true
            )
        ) {
            assertThat(
                EsqlTestUtils.getValuesList(response),
                equalTo(
                    List.of(
                        List.of("user-handle-63", "payload-63"),
                        List.of("user-handle-62", "payload-62"),
                        List.of("user-handle-61", "payload-61")
                    )
                )
            );
            assertRemoteFetchRows(response, 3);
            assertFieldLoadedBeforeFetch(response, "unique_sort");
            assertFieldNotLoadedBeforeFetch(response, "_remote_fetch_handle");
            assertFieldNotLoadedBeforeFetch(response, "payload");
        }
    }

    public void testMultipleTopNsRunSingleRemoteFetchAtFirstExchangeTopN() {
        try (
            EsqlQueryResponse response = runQuery(
                "FROM "
                    + indexName
                    + " | SORT unique_sort + 1 DESC"
                    + " | LIMIT 20"
                    + " | SORT tie_breaker + 1 ASC, unique_sort + 1 DESC"
                    + " | LIMIT 5"
                    + " | KEEP unique_sort, tie_breaker, payload",
                true
            )
        ) {
            assertThat(
                EsqlTestUtils.getValuesList(response),
                equalTo(
                    List.of(
                        List.of(60L, 0L, "payload-60"),
                        List.of(56L, 0L, "payload-56"),
                        List.of(52L, 0L, "payload-52"),
                        List.of(48L, 0L, "payload-48"),
                        List.of(44L, 0L, "payload-44")
                    )
                )
            );
            assertThat(remoteFetchStatuses(response), hasSize(1));
            assertThat(remoteFetchRowsEmitted(response), equalTo(20L));
            assertFieldLoadedBeforeFetch(response, "unique_sort");
            assertFieldNotLoadedBeforeFetch(response, "tie_breaker");
            assertFieldNotLoadedBeforeFetch(response, "payload");
        }
    }

    public void testNoRemoteFetchWhenNoDeferredFields() {
        try (
            EsqlQueryResponse response = runQuery("FROM " + indexName + " | SORT unique_sort + 1 DESC | LIMIT 5 | KEEP unique_sort", true)
        ) {
            assertThat(
                EsqlTestUtils.getValuesList(response),
                equalTo(List.of(List.of(63L), List.of(62L), List.of(61L), List.of(60L), List.of(59L)))
            );
            assertThat(remoteFetchStatuses(response), empty());
            assertFieldLoadedBeforeFetch(response, "unique_sort");
        }
    }

    public void testRemoteFetchSourceOnlyField() {
        try (
            EsqlQueryResponse response = runQuery(
                "FROM " + indexName + " | SORT unique_sort + 1 DESC | LIMIT 3 | KEEP unique_sort, source_payload",
                true
            )
        ) {
            assertThat(
                EsqlTestUtils.getValuesList(response),
                equalTo(List.of(List.of(63L, "source-payload-63"), List.of(62L, "source-payload-62"), List.of(61L, "source-payload-61")))
            );
            assertRemoteFetchRows(response, 3);
            assertFieldLoadedBeforeFetch(response, "unique_sort");
            assertFieldNotLoadedBeforeFetch(response, "source_payload");
        }
    }

    public void testExpressionAfterTopNUsesRemoteFetchedField() {
        try (
            EsqlQueryResponse response = runQuery(
                "FROM "
                    + indexName
                    + " | SORT unique_sort + 1 DESC"
                    + " | LIMIT 3"
                    + " | EVAL derived = CONCAT(payload, \"-derived\")"
                    + " | KEEP unique_sort, derived",
                true
            )
        ) {
            assertThat(
                EsqlTestUtils.getValuesList(response),
                equalTo(List.of(List.of(63L, "payload-63-derived"), List.of(62L, "payload-62-derived"), List.of(61L, "payload-61-derived")))
            );
            assertRemoteFetchRows(response, 3);
            assertFieldLoadedBeforeFetch(response, "unique_sort");
            assertFieldNotLoadedBeforeFetch(response, "payload");
        }
    }

    public void testExpressionBeforeTopNIsNotRemoteFetched() {
        try (
            EsqlQueryResponse response = runQuery(
                "FROM "
                    + indexName
                    + " | EVAL derived = CONCAT(payload, \"-derived\")"
                    + " | SORT unique_sort + 1 DESC"
                    + " | LIMIT 3"
                    + " | KEEP unique_sort, derived",
                true
            )
        ) {
            assertThat(
                EsqlTestUtils.getValuesList(response),
                equalTo(List.of(List.of(63L, "payload-63-derived"), List.of(62L, "payload-62-derived"), List.of(61L, "payload-61-derived")))
            );
            assertThat(remoteFetchStatuses(response), empty());
            assertFieldLoadedBeforeFetch(response, "unique_sort");
            assertFieldLoadedBeforeFetch(response, "payload");
        }
    }

    public void testMetadataSortKeys() {
        try (
            EsqlQueryResponse response = runQuery(
                "FROM " + indexName + " METADATA _id, _index | SORT _index DESC, _id DESC | LIMIT 3 | KEEP _index, _id, payload",
                true
            )
        ) {
            assertThat(
                EsqlTestUtils.getValuesList(response),
                equalTo(
                    List.of(
                        List.of(indexName, "9", "payload-9"),
                        List.of(indexName, "8", "payload-8"),
                        List.of(indexName, "7", "payload-7")
                    )
                )
            );
            assertRemoteFetchRows(response, 3);
            assertFieldLoadedBeforeFetch(response, "_index");
            assertFieldLoadedBeforeFetch(response, "_id");
            assertFieldNotLoadedBeforeFetch(response, "payload");
        }
    }

    public void testNoRemoteFetchAfterAggregation() {
        try (
            EsqlQueryResponse response = runQuery(
                "FROM " + indexName + " | STATS total = SUM(metric) BY category | SORT total DESC | LIMIT 2",
                true
            )
        ) {
            assertThat(EsqlTestUtils.getValuesList(response), hasSize(2));
            assertThat(remoteFetchStatuses(response), empty());
        }
    }

    public void testTimeSeriesIndex() {
        String timeSeriesIndex = indexName + "_ts";
        client().admin()
            .indices()
            .prepareCreate(timeSeriesIndex)
            .setSettings(Settings.builder().put("mode", "time_series").putList("routing_path", List.of("host")))
            .setMapping(
                "@timestamp",
                "type=date",
                "host",
                "type=keyword,time_series_dimension=true",
                "metric",
                "type=long,time_series_metric=gauge",
                "payload",
                "type=keyword"
            )
            .get();

        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 8; i++) {
            bulk.add(
                prepareIndex(timeSeriesIndex).setSource(
                    "@timestamp",
                    "2026-01-01T00:00:0" + i + "Z",
                    "host",
                    "host-a",
                    "metric",
                    i,
                    "payload",
                    "ts-payload-" + i
                )
            );
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        try (
            EsqlQueryResponse response = runQuery(
                "FROM " + timeSeriesIndex + " | SORT @timestamp DESC | LIMIT 3 | KEEP @timestamp, payload",
                true
            )
        ) {
            List<List<Object>> values = EsqlTestUtils.getValuesList(response);
            assertThat(values, hasSize(3));
            assertThat(values.stream().map(row -> row.get(1)).toList(), equalTo(List.of("ts-payload-7", "ts-payload-6", "ts-payload-5")));
            assertRemoteFetchRows(response, 3);
            assertFieldLoadedBeforeFetch(response, "@timestamp");
            assertFieldNotLoadedBeforeFetch(response, "payload");
        }
    }

    public void testPotentiallyUnmappedFieldDoesNotUseRemoteFetch() {
        String mappedIndex = indexName + "_mapped";
        String unmappedIndex = indexName + "_unmapped";
        client().admin()
            .indices()
            .prepareCreate(mappedIndex)
            .setSettings(indexSettings(1, 0))
            .setMapping("unique_sort", "type=long", "optional", "type=keyword")
            .get();
        client().admin()
            .indices()
            .prepareCreate(unmappedIndex)
            .setSettings(indexSettings(1, 0))
            // Keep "optional" in _source without adding it to this index's mapping.
            .setMapping("""
                {
                  "dynamic": false,
                  "properties": {
                    "unique_sort": { "type": "long" }
                  }
                }
                """)
            .get();

        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 3; i++) {
            bulk.add(prepareIndex(mappedIndex).setSource("unique_sort", i, "optional", "mapped-" + i));
            bulk.add(prepareIndex(unmappedIndex).setSource("unique_sort", i + 3, "optional", "unmapped-" + (i + 3)));
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        try (
            EsqlQueryResponse response = runQuery(
                "SET unmapped_fields=\"load\"; FROM "
                    + mappedIndex
                    + ","
                    + unmappedIndex
                    + " | SORT unique_sort + 1 DESC | LIMIT 3 | KEEP unique_sort, optional",
                true
            )
        ) {
            assertThat(
                EsqlTestUtils.getValuesList(response),
                equalTo(List.of(List.of(5L, "unmapped-5"), List.of(4L, "unmapped-4"), List.of(3L, "unmapped-3")))
            );
            assertThat(remoteFetchStatuses(response), empty());
            assertFieldLoadedBeforeFetch(response, "unique_sort");
            assertFieldLoadedBeforeFetch(response, "optional");
        }
    }

    public void testUnionTypedFieldDoesNotUseRemoteFetch() {
        String dateIndex = indexName + "_date";
        String dateNanosIndex = indexName + "_date_nanos";
        client().admin()
            .indices()
            .prepareCreate(dateIndex)
            .setSettings(indexSettings(1, 0))
            .setMapping("unique_sort", "type=long", "union_value", "type=date")
            .get();
        client().admin()
            .indices()
            .prepareCreate(dateNanosIndex)
            .setSettings(indexSettings(1, 0))
            .setMapping("unique_sort", "type=long", "union_value", "type=date_nanos")
            .get();

        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 4; i++) {
            bulk.add(prepareIndex(dateIndex).setSource("unique_sort", i, "union_value", "2026-01-01T00:00:0" + i + ".000Z"));
            bulk.add(
                prepareIndex(dateNanosIndex).setSource("unique_sort", i + 4, "union_value", "2026-01-01T00:00:0" + (i + 4) + ".123456789Z")
            );
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        try (
            EsqlQueryResponse response = runQuery(
                "FROM " + dateIndex + "," + dateNanosIndex + " | SORT unique_sort ASC | LIMIT 6 | KEEP unique_sort, union_value",
                true
            )
        ) {
            List<List<Object>> values = EsqlTestUtils.getValuesList(response);
            assertThat(values.stream().map(List::getFirst).toList(), equalTo(List.of(0L, 1L, 2L, 3L, 4L, 5L)));
            assertTrue(values.stream().allMatch(row -> row.get(1).toString().startsWith("2026-01-01T00:00:0")));
            assertThat(remoteFetchStatuses(response), empty());
            assertFieldLoadedBeforeFetch(response, "unique_sort");
            assertFieldLoadedBeforeFetch(response, "union_value");
        }
    }

    private EsqlQueryResponse runQuery(String query, boolean remoteFetchTopN) {
        // Keep the data path deterministic and ensure these tests exercise shard-level node reduction.
        Settings.Builder pragmas = Settings.builder()
            .put(QueryPragmas.TASK_CONCURRENCY.getKey(), 1)
            .put(QueryPragmas.DATA_PARTITIONING.getKey(), "shard");
        if (remoteFetchTopN) {
            pragmas.put(QueryPragmas.REMOTE_FETCH_TOPN.getKey(), true);
        }
        return client().execute(
            EsqlQueryAction.INSTANCE,
            syncEsqlQueryRequest(query).acceptedPragmaRisks(true).pragmas(new QueryPragmas(pragmas.build())).profile(true)
        ).actionGet(1, TimeUnit.MINUTES);
    }

    private static void assertRemoteFetchRows(EsqlQueryResponse response, int rowsEmitted) {
        List<RemoteFetchOperator.Status> statuses = remoteFetchStatuses(response);
        assertThat(statuses, not(empty()));
        assertThat(remoteFetchRowsEmitted(response), equalTo((long) rowsEmitted));
        assertThat(statuses.stream().mapToLong(RemoteFetchOperator.Status::batchesSent).sum(), greaterThan(0L));
        assertThat(statuses.stream().mapToInt(RemoteFetchOperator.Status::exchangesOpened).sum(), greaterThan(0));
    }

    private static long remoteFetchRowsEmitted(EsqlQueryResponse response) {
        return remoteFetchStatuses(response).stream().mapToLong(RemoteFetchOperator.Status::rowsEmitted).sum();
    }

    private static List<RemoteFetchOperator.Status> remoteFetchStatuses(EsqlQueryResponse response) {
        return response.profile()
            .drivers()
            .stream()
            .flatMap(driver -> driver.operators().stream())
            .map(OperatorStatus::status)
            .filter(RemoteFetchOperator.Status.class::isInstance)
            .map(RemoteFetchOperator.Status.class::cast)
            .toList();
    }

    private static void assertFieldLoadedBeforeFetch(EsqlQueryResponse response, String fieldName) {
        Set<String> loadedFields = fieldsLoadedBeforeFetch(response);
        assertTrue(
            "expected [" + fieldName + "] to be loaded before fetch, loaded fields were " + loadedFields,
            containsField(loadedFields, fieldName)
        );
    }

    private static void assertFieldNotLoadedBeforeFetch(EsqlQueryResponse response, String fieldName) {
        Set<String> loadedFields = fieldsLoadedBeforeFetch(response);
        assertFalse(
            "expected [" + fieldName + "] to be deferred until fetch, loaded fields were " + loadedFields,
            containsField(loadedFields, fieldName)
        );
    }

    private static boolean containsField(Set<String> fields, String fieldName) {
        // Reader keys have the shape "field:reader_description"; match the field prefix exactly so that e.g. "id" does not match "docid".
        return fields.stream().anyMatch(field -> field.startsWith(fieldName + ":"));
    }

    private static Set<String> fieldsLoadedBeforeFetch(EsqlQueryResponse response) {
        Set<String> fields = new HashSet<>();
        for (DriverProfile driver : response.profile().drivers()) {
            if (driver.description().equals("data") == false && driver.description().equals("node_reduce") == false) {
                continue;
            }
            for (OperatorStatus operator : driver.operators()) {
                if (operator.status() instanceof ValuesSourceReaderOperatorStatus status && status.valuesLoaded() > 0) {
                    fields.addAll(status.readersBuilt().keySet());
                }
            }
        }
        return fields;
    }
}
