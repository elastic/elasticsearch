/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.lucene.read.FetchDocsSourceOperator;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperatorStatus;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch.PlanFetch;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * Queries that the fetch phase plans, and queries it leaves alone, each run with the fetch phase on and off. Both runs
 * must return the same columns and rows. The profile of the run with the fetch phase then shows where each column was
 * loaded: by the drivers before the cut, or by the {@code fetch} drivers on the nodes that hold the documents of the rows
 * that survived it.
 */
public abstract class EsqlFetchPhaseTestCase extends AbstractEsqlIntegTestCase {
    private String indexName;

    /**
     * The result and the profile of one run.
     */
    protected record Run(List<String> columns, List<List<Object>> rows, List<DriverProfile> drivers) {}

    @Before
    public void setupIndex() {
        assumeTrue("the fetch phase needs its feature flag", EsqlFlags.FETCH_PHASE_FEATURE_FLAG.isEnabled());
        indexName = "fetch_phase_" + getTestName().toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "_");
        assertAcked(
            indicesAdmin().prepareCreate(indexName)
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
                    PlanFetch.DOC_REF_NAME,
                    "type=keyword"
                )
        );
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
                            PlanFetch.DOC_REF_NAME,
                            "user-value-" + i
                        )
                    )
            );
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
    }

    public void testFetchesTheColumnsOfTheWinners() {
        Run run = runBoth("FROM " + indexName + " | SORT unique_sort + 1 DESC | LIMIT 5 | KEEP unique_sort, payload, category");
        assertThat(
            run.rows(),
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
        assertFetched(run, 5);
        assertLoadedOnlyBeforeTheCut(run, "unique_sort");
        assertFetchedOnly(run, "payload");
        assertFetchedOnly(run, "category");
    }

    /**
     * {@code MV_EXPAND} repeats documents before the cut, so the query loads its columns eagerly.
     */
    public void testMvExpandBeforeTheCutStaysEager() {
        String index = indexName + "_mv_expand";
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(indexSettings(1, 0))
                .setMapping("unique_sort", "type=long", "tags", "type=keyword", "payload", "type=keyword")
        );
        client().prepareBulk()
            .add(prepareIndex(index).setId("0").setSource("unique_sort", 0, "tags", List.of("a", "b", "c"), "payload", "payload-0"))
            .add(prepareIndex(index).setId("1").setSource("unique_sort", 1, "tags", List.of("d"), "payload", "payload-1"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        Run run = runBoth(
            "FROM " + index + " | MV_EXPAND tags | SORT unique_sort + 0 ASC, tags ASC | LIMIT 4 | KEEP tags, unique_sort, payload"
        );
        assertThat(
            run.rows(),
            equalTo(
                List.of(
                    List.of("a", 0L, "payload-0"),
                    List.of("b", 0L, "payload-0"),
                    List.of("c", 0L, "payload-0"),
                    List.of("d", 1L, "payload-1")
                )
            )
        );
        assertNotFetched(run);
        assertLoadedOnlyBeforeTheCut(run, "payload");
    }

    public void testWithoutKeepKeepsTheOutputOrder() {
        String index = indexName + "_without_keep";
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(indexSettings(4, 0))
                .setMapping("unique_sort", "type=long", "payload", "type=keyword")
        );
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 8; i++) {
            bulk.add(prepareIndex(index).setId(Integer.toString(i)).setSource("unique_sort", i, "payload", "payload-" + i));
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        Run run = runBoth("FROM " + index + " | SORT unique_sort + 1 DESC | LIMIT 3");
        assertThat(run.columns(), equalTo(List.of("payload", "unique_sort")));
        assertThat(run.rows(), equalTo(List.of(List.of("payload-7", 7L), List.of("payload-6", 6L), List.of("payload-5", 5L))));
        assertFetched(run, 3);
        assertLoadedOnlyBeforeTheCut(run, "unique_sort");
        assertFetchedOnly(run, "payload");
    }

    public void testSeveralSortKeys() {
        Run run = runBoth(
            "FROM " + indexName + " | SORT sorted + 0 DESC, tie_breaker + 0 ASC | LIMIT 7 | KEEP sorted, tie_breaker, payload, metric"
        );
        assertThat(
            run.rows(),
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
        assertFetched(run, 7);
        assertLoadedOnlyBeforeTheCut(run, "sorted");
        assertLoadedOnlyBeforeTheCut(run, "tie_breaker");
        assertFetchedOnly(run, "payload");
        assertFetchedOnly(run, "metric");
    }

    /**
     * Hundreds of document references cross the exchange and the coordinator's {@code TopN} unchanged.
     */
    public void testManyDocumentReferencesSurviveTheCut() {
        String index = indexName + "_many";
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(indexSettings(1, 0))
                .setMapping("content", "type=text", "unique_sort", "type=long", "payload", "type=keyword")
        );
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 320; i++) {
            bulk.add(
                prepareIndex(index).setId(Integer.toString(i))
                    .setSource("content", "industrial revolution", "unique_sort", i, "payload", "payload-" + i)
            );
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        Run run = runBoth(
            "FROM "
                + index
                + " METADATA _score"
                + " | WHERE MATCH(content, \"industrial revolution\")"
                + " | SORT _score DESC, unique_sort + 0 DESC"
                + " | LIMIT 300"
                + " | KEEP payload"
        );
        assertThat(run.rows(), hasSize(300));
        assertFetched(run, 300);
        assertLoadedOnlyBeforeTheCut(run, "unique_sort");
        assertFetchedOnly(run, "payload");
    }

    /**
     * The cluster setting turns the fetch phase on for queries without the pragma.
     */
    public void testTheClusterSettingTurnsTheFetchPhaseOn() {
        String query = "FROM " + indexName + " | SORT unique_sort + 1 DESC | LIMIT 5 | KEEP unique_sort, payload, category";
        updateClusterSettings(Settings.builder().put(EsqlFlags.ESQL_FETCH_PHASE.getKey(), true));
        try {
            Run run = runQuery(query, null);
            assertFetched(run, 5);
        } finally {
            updateClusterSettings(Settings.builder().putNull(EsqlFlags.ESQL_FETCH_PHASE.getKey()));
        }
        assertNotFetched(runQuery(query, null));
    }

    /**
     * The document reference column is recognized by its type, so a user field with its name is a column like any other.
     */
    public void testAUserFieldNamedLikeTheDocumentReference() {
        Run run = runBoth("FROM " + indexName + " | SORT unique_sort + 1 DESC | LIMIT 3 | KEEP `" + PlanFetch.DOC_REF_NAME + "`, payload");
        assertThat(
            run.rows(),
            equalTo(
                List.of(
                    List.of("user-value-63", "payload-63"),
                    List.of("user-value-62", "payload-62"),
                    List.of("user-value-61", "payload-61")
                )
            )
        );
        assertFetched(run, 3);
        assertFetchedOnly(run, PlanFetch.DOC_REF_NAME);
        assertFetchedOnly(run, "payload");
    }

    /**
     * A second cut after the first is not a plan the fetch phase supports yet, so the query loads its columns eagerly.
     */
    public void testTwoCutsStayEager() {
        Run run = runBoth(
            "FROM "
                + indexName
                + " | SORT unique_sort + 1 DESC"
                + " | LIMIT 20"
                + " | SORT tie_breaker + 1 ASC, unique_sort + 1 DESC"
                + " | LIMIT 5"
                + " | KEEP unique_sort, tie_breaker, payload"
        );
        assertThat(
            run.rows(),
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
        assertNotFetched(run);
        assertLoadedOnlyBeforeTheCut(run, "payload");
    }

    /**
     * The data nodes read {@code unique_sort} to compute the sort key, so it crosses the exchange as a value. The fetch
     * doesn't read it a second time, and with nothing else to fetch the query stays eager.
     */
    public void testAColumnOfAComputedSortKeyIsReadOnce() {
        Run run = runBoth("FROM " + indexName + " | SORT unique_sort + 1 DESC | LIMIT 5 | KEEP unique_sort");
        assertThat(run.rows(), equalTo(List.of(List.of(63L), List.of(62L), List.of(61L), List.of(60L), List.of(59L))));
        assertNotFetched(run);
        assertLoadedOnlyBeforeTheCut(run, "unique_sort");
    }

    public void testNothingToFetchWhenTheCutNeedsEveryColumn() {
        Run run = runBoth("FROM " + indexName + " | SORT unique_sort DESC | LIMIT 5 | KEEP unique_sort");
        assertThat(run.rows(), equalTo(List.of(List.of(63L), List.of(62L), List.of(61L), List.of(60L), List.of(59L))));
        assertNotFetched(run);
    }

    /**
     * A field without doc values loads from {@code _source}, on the fetch side too.
     */
    public void testFetchesAFieldWithoutDocValues() {
        Run run = runBoth("FROM " + indexName + " | SORT unique_sort + 1 DESC | LIMIT 3 | KEEP unique_sort, source_payload");
        assertThat(
            run.rows(),
            equalTo(List.of(List.of(63L, "source-payload-63"), List.of(62L, "source-payload-62"), List.of(61L, "source-payload-61")))
        );
        assertFetched(run, 3);
        assertLoadedOnlyBeforeTheCut(run, "unique_sort");
        assertFetchedOnly(run, "source_payload");
    }

    public void testAnExpressionAfterTheCutReadsAFetchedColumn() {
        Run run = runBoth(
            "FROM "
                + indexName
                + " | SORT unique_sort + 1 DESC"
                + " | LIMIT 3"
                + " | EVAL derived = CONCAT(payload, \"-derived\")"
                + " | KEEP unique_sort, derived"
        );
        assertThat(
            run.rows(),
            equalTo(List.of(List.of(63L, "payload-63-derived"), List.of(62L, "payload-62-derived"), List.of(61L, "payload-61-derived")))
        );
        assertFetched(run, 3);
        assertLoadedOnlyBeforeTheCut(run, "unique_sort");
        assertFetchedOnly(run, "payload");
    }

    /**
     * An expression before the cut runs on every candidate row, so its input loads before the cut and its value crosses
     * the exchange. The input crosses too when the query returns it, instead of being read a second time. The other
     * columns are still fetched.
     */
    public void testAnExpressionBeforeTheCutStaysEager() {
        Run run = runBoth(
            "FROM "
                + indexName
                + " | EVAL derived = CONCAT(payload, \"-derived\")"
                + " | SORT unique_sort DESC"
                + " | LIMIT 3"
                + " | KEEP unique_sort, payload, derived, category"
        );
        assertThat(
            run.rows(),
            equalTo(
                List.of(
                    List.of(63L, "payload-63", "payload-63-derived", "cat-3"),
                    List.of(62L, "payload-62", "payload-62-derived", "cat-2"),
                    List.of(61L, "payload-61", "payload-61-derived", "cat-1")
                )
            )
        );
        assertFetched(run, 3);
        assertLoadedOnlyBeforeTheCut(run, "payload");
        assertFetchedOnly(run, "category");
    }

    public void testMetadataSortKeys() {
        Run run = runBoth("FROM " + indexName + " METADATA _id, _index | SORT _index DESC, _id DESC | LIMIT 3 | KEEP _index, _id, payload");
        assertThat(
            run.rows(),
            equalTo(
                List.of(List.of(indexName, "9", "payload-9"), List.of(indexName, "8", "payload-8"), List.of(indexName, "7", "payload-7"))
            )
        );
        assertFetched(run, 3);
        assertLoadedOnlyBeforeTheCut(run, "_id");
        assertFetchedOnly(run, "payload");
    }

    public void testFetchesTheId() {
        Run run = runBoth("FROM " + indexName + " METADATA _id | SORT unique_sort + 1 DESC | LIMIT 3 | KEEP _id, payload");
        assertThat(run.rows(), equalTo(List.of(List.of("63", "payload-63"), List.of("62", "payload-62"), List.of("61", "payload-61"))));
        assertFetched(run, 3);
        assertFetchedOnly(run, "_id");
        assertFetchedOnly(run, "payload");
    }

    public void testFetchesTheSource() {
        Run run = runBoth("FROM " + indexName + " METADATA _source | SORT unique_sort DESC | LIMIT 3 | KEEP _source");
        assertThat(run.rows(), hasSize(3));
        for (int row = 0; row < run.rows().size(); row++) {
            Map<?, ?> source = (Map<?, ?>) run.rows().get(row).getFirst();
            assertThat(((Number) source.get("unique_sort")).longValue(), equalTo(63L - row));
            assertThat(source.get("payload"), equalTo("payload-" + (63 - row)));
        }
        assertFetched(run, 3);
        assertFetchedOnly(run, "_source");
    }

    public void testFetchesTheSyntheticSource() {
        String index = indexName + "_synthetic";
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(indexSettings(4, 0).put("index.mapping.source.mode", "synthetic"))
                .setMapping("unique_sort", "type=long", "payload", "type=keyword")
        );
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 8; i++) {
            bulk.add(prepareIndex(index).setId(Integer.toString(i)).setSource("unique_sort", i, "payload", "payload-" + i));
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        Run run = runBoth("FROM " + index + " METADATA _source | SORT unique_sort DESC | LIMIT 3 | KEEP _source");
        assertThat(run.rows(), hasSize(3));
        for (int row = 0; row < run.rows().size(); row++) {
            Map<?, ?> source = (Map<?, ?>) run.rows().get(row).getFirst();
            assertThat(((Number) source.get("unique_sort")).longValue(), equalTo(7L - row));
            assertThat(source.get("payload"), equalTo("payload-" + (7 - row)));
        }
        assertFetched(run, 3);
        assertFetchedOnly(run, "_source");
    }

    /**
     * A logsdb index sorts by host and time and rebuilds {@code _source} from the fields. Its {@code message} is the shape
     * the loader finds hardest: a short message loads from the doc values of its keyword subfield, a long one, above
     * {@code ignore_above}, from the ignored source.
     */
    public void testALogsdbIndex() {
        String index = indexName + "_logsdb";
        assertAcked(indicesAdmin().prepareCreate(index).setSettings(indexSettings(2, 0).put("index.mode", "logsdb")).setMapping("""
            {
              "properties": {
                "@timestamp": { "type": "date" },
                "host.name": { "type": "keyword" },
                "message": { "type": "text", "fields": { "raw": { "type": "keyword", "ignore_above": 16 } } },
                "payload": { "type": "keyword" }
              }
            }
            """));
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 12; i++) {
            String message = i % 3 == 0 ? "a long message that passes ignore_above " + i : "short " + i;
            bulk.add(
                prepareIndex(index).setSource(
                    "@timestamp",
                    "2026-01-01T00:00:" + (10 + i) + "Z",
                    "host.name",
                    "host-" + (i % 3),
                    "message",
                    message,
                    "payload",
                    "payload-" + i
                )
            );
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        Run run = runBoth(
            "FROM "
                + index
                + " METADATA _id, _source | SORT @timestamp DESC | LIMIT 6 | KEEP @timestamp, host.name, message, payload, _id, _source"
        );
        assertThat(run.rows(), hasSize(6));
        assertThat(
            run.rows().stream().map(row -> row.get(2)).toList(),
            equalTo(
                List.of(
                    "short 11",
                    "short 10",
                    "a long message that passes ignore_above 9",
                    "short 8",
                    "short 7",
                    "a long message that passes ignore_above 6"
                )
            )
        );
        for (List<Object> row : run.rows()) {
            Map<?, ?> source = (Map<?, ?>) row.get(5);
            assertThat(source.get("message"), equalTo(row.get(2)));
        }
        assertFetched(run, 6);
        assertLoadedOnlyBeforeTheCut(run, "@timestamp");
        assertFetchedOnly(run, "message");
        assertFetchedOnly(run, "_source");
    }

    public void testNothingToFetchAfterAnAggregation() {
        Run run = runBoth("FROM " + indexName + " | STATS total = SUM(metric) BY category | SORT total DESC | LIMIT 2");
        assertThat(run.rows(), hasSize(2));
        assertNotFetched(run);
    }

    public void testATimeSeriesIndex() {
        String index = indexName + "_ts";
        assertAcked(
            indicesAdmin().prepareCreate(index)
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
        );
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 8; i++) {
            bulk.add(
                prepareIndex(index).setSource(
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

        Run run = runBoth("FROM " + index + " | SORT @timestamp DESC | LIMIT 3 | KEEP @timestamp, payload");
        assertThat(run.rows().stream().map(row -> row.get(1)).toList(), equalTo(List.of("ts-payload-7", "ts-payload-6", "ts-payload-5")));
        assertFetched(run, 3);
        assertLoadedOnlyBeforeTheCut(run, "@timestamp");
        assertFetchedOnly(run, "payload");
    }

    /**
     * A field that some indices don't map loads through a rule that picks the loader per node, which the fetch side
     * doesn't run, so it crosses the exchange as a value. The other columns are still fetched.
     */
    public void testAPotentiallyUnmappedFieldStaysEager() {
        String mapped = indexName + "_mapped";
        String unmapped = indexName + "_unmapped";
        assertAcked(
            indicesAdmin().prepareCreate(mapped)
                .setSettings(indexSettings(1, 0))
                .setMapping("unique_sort", "type=long", "optional", "type=keyword", "payload", "type=keyword")
        );
        assertAcked(indicesAdmin().prepareCreate(unmapped).setSettings(indexSettings(1, 0)).setMapping("""
            {
              "dynamic": false,
              "properties": {
                "unique_sort": { "type": "long" },
                "payload": { "type": "keyword" }
              }
            }
            """));
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 3; i++) {
            bulk.add(prepareIndex(mapped).setSource("unique_sort", i, "optional", "mapped-" + i, "payload", "payload-" + i));
            bulk.add(
                prepareIndex(unmapped).setSource("unique_sort", i + 3, "optional", "unmapped-" + (i + 3), "payload", "payload-" + (i + 3))
            );
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        Run run = runBoth(
            "SET unmapped_fields=\"load\"; FROM "
                + mapped
                + ","
                + unmapped
                + " | SORT unique_sort DESC | LIMIT 3 | KEEP unique_sort, optional, payload"
        );
        assertThat(
            run.rows(),
            equalTo(
                List.of(
                    List.of(5L, "unmapped-5", "payload-5"),
                    List.of(4L, "unmapped-4", "payload-4"),
                    List.of(3L, "unmapped-3", "payload-3")
                )
            )
        );
        assertFetched(run, 3);
        assertLoadedOnlyBeforeTheCut(run, "optional");
        assertFetchedOnly(run, "payload");
    }

    /**
     * Each index converts its own type of a union typed field, on the fetch side as on the data drivers.
     */
    public void testFetchesAUnionTypedField() {
        String dates = indexName + "_date";
        String nanos = indexName + "_date_nanos";
        assertAcked(
            indicesAdmin().prepareCreate(dates)
                .setSettings(indexSettings(1, 0))
                .setMapping("unique_sort", "type=long", "union_value", "type=date")
        );
        assertAcked(
            indicesAdmin().prepareCreate(nanos)
                .setSettings(indexSettings(1, 0))
                .setMapping("unique_sort", "type=long", "union_value", "type=date_nanos")
        );
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 4; i++) {
            bulk.add(prepareIndex(dates).setSource("unique_sort", i, "union_value", "2026-01-01T00:00:0" + i + ".000Z"));
            bulk.add(prepareIndex(nanos).setSource("unique_sort", i + 4, "union_value", "2026-01-01T00:00:0" + (i + 4) + ".123456789Z"));
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        Run run = runBoth("FROM " + dates + "," + nanos + " | SORT unique_sort ASC | LIMIT 6 | KEEP unique_sort, union_value");
        assertThat(run.rows().stream().map(List::getFirst).toList(), equalTo(List.of(0L, 1L, 2L, 3L, 4L, 5L)));
        assertFetched(run, 6);
        assertLoadedOnlyBeforeTheCut(run, "unique_sort");
        assertFetchedOnly(run, "union_value");
    }

    /**
     * Runs {@code query} with the fetch phase off and on, checks that both return the same columns and rows, and returns
     * the run with the fetch phase.
     */
    protected Run runBoth(String query) {
        Run eager = runQuery(query, false);
        assertNotFetched(eager);
        Run fetched = runQuery(query, true);
        assertThat(fetched.columns(), equalTo(eager.columns()));
        assertThat(fetched.rows(), equalTo(eager.rows()));
        return fetched;
    }

    /**
     * @param fetchPhase the {@code fetch_phase} pragma, {@code null} to follow the cluster setting
     */
    protected Run runQuery(String query, Boolean fetchPhase) {
        // one driver per shard keeps the shards and the node reduction of the plans deterministic
        Settings.Builder pragmas = Settings.builder()
            .put(QueryPragmas.TASK_CONCURRENCY.getKey(), 1)
            .put(QueryPragmas.DATA_PARTITIONING.getKey(), "shard");
        if (fetchPhase != null) {
            pragmas.put(QueryPragmas.FETCH_PHASE.getKey(), fetchPhase);
        }
        EsqlQueryRequest request = syncEsqlQueryRequest(query).acceptedPragmaRisks(true)
            .pragmas(new QueryPragmas(pragmas.build()))
            .profile(true);
        try (EsqlQueryResponse response = client().execute(EsqlQueryAction.INSTANCE, request).actionGet(1, TimeUnit.MINUTES)) {
            return new Run(
                response.columns().stream().map(ColumnInfoImpl::name).toList(),
                EsqlTestUtils.getValuesList(response),
                response.profile().drivers()
            );
        }
    }

    /**
     * The query loaded {@code rows} documents in {@code fetch} drivers, the drivers of the nodes that hold them.
     */
    protected static void assertFetched(Run run, int rows) {
        List<DriverProfile> fetch = fetchDrivers(run);
        assertThat("fetch drivers", fetch, not(empty()));
        long documents = fetch.stream()
            .flatMap(driver -> driver.operators().stream())
            .map(OperatorStatus::status)
            .filter(FetchDocsSourceOperator.Status.class::isInstance)
            .mapToLong(status -> ((FetchDocsSourceOperator.Status) status).docsEmitted())
            .sum();
        assertThat("documents the fetch loaded", documents, equalTo((long) rows));
    }

    protected static void assertNotFetched(Run run) {
        assertThat("fetch drivers", fetchDrivers(run), empty());
    }

    /**
     * The field loaded before the cut, for the candidate rows, and not a second time in the {@code fetch} drivers.
     */
    protected static void assertLoadedOnlyBeforeTheCut(Run run, String field) {
        Set<String> loaded = fieldsLoadedBy(run, Set.of(ComputeService.DATA_DESCRIPTION, ComputeService.REDUCE_DESCRIPTION));
        assertTrue("expected [" + field + "] to load before the cut, loaded were " + loaded, containsField(loaded, field));
        Set<String> fetched = fieldsLoadedBy(run, Set.of(ComputeService.FETCH_DESCRIPTION));
        assertFalse("expected [" + field + "] not to load in the fetch, loaded were " + fetched, containsField(fetched, field));
    }

    /**
     * The field loaded in the {@code fetch} drivers, for the rows that survived the cut, and not before.
     */
    protected static void assertFetchedOnly(Run run, String field) {
        Set<String> before = fieldsLoadedBy(run, Set.of(ComputeService.DATA_DESCRIPTION, ComputeService.REDUCE_DESCRIPTION));
        assertFalse("expected [" + field + "] not to load before the cut, loaded were " + before, containsField(before, field));
        Set<String> fetched = fieldsLoadedBy(run, Set.of(ComputeService.FETCH_DESCRIPTION));
        assertTrue("expected [" + field + "] to load in the fetch, loaded were " + fetched, containsField(fetched, field));
    }

    private static List<DriverProfile> fetchDrivers(Run run) {
        return run.drivers().stream().filter(driver -> driver.description().equals(ComputeService.FETCH_DESCRIPTION)).toList();
    }

    private static Set<String> fieldsLoadedBy(Run run, Set<String> descriptions) {
        Set<String> fields = new HashSet<>();
        for (DriverProfile driver : run.drivers()) {
            if (descriptions.contains(driver.description()) == false) {
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

    /**
     * Reader keys look like {@code field:reader}, so the field is a prefix, and {@code id} doesn't match {@code docid}.
     */
    private static boolean containsField(Set<String> fields, String field) {
        return fields.stream().anyMatch(key -> key.startsWith(field + ":"));
    }
}
