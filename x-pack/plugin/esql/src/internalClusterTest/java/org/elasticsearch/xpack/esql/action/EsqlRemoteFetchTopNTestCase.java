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
        }
    }

    private EsqlQueryResponse runQuery(String query, boolean remoteFetchTopN) {
        Settings.Builder pragmas = Settings.builder()
            .put(QueryPragmas.NODE_LEVEL_REDUCTION.getKey(), true)
            .put(QueryPragmas.TASK_CONCURRENCY.getKey(), 1)
            .put(QueryPragmas.MAX_CONCURRENT_NODES_PER_CLUSTER.getKey(), 10)
            .put(QueryPragmas.MAX_CONCURRENT_SHARDS_PER_NODE.getKey(), 10)
            .put(QueryPragmas.DATA_PARTITIONING.getKey(), "shard");
        if (remoteFetchTopN) {
            pragmas.put(QueryPragmas.REMOTE_FETCH_TOPN.getKey(), true);
        }
        return client().execute(
            EsqlQueryAction.INSTANCE,
            syncEsqlQueryRequest(query).pragmas(new QueryPragmas(pragmas.build())).profile(true)
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
        return fields.stream().anyMatch(field -> field.contains(fieldName));
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
