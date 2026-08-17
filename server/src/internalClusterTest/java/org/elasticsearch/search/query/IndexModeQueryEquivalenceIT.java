/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.query;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

/**
 * Randomized equivalence test asserting that identical documents indexed into indices with different
 * {@code index.mode} values ({@code standard}, {@code logsdb}, {@code columnar}, {@code time_series},
 * {@code logsdb_columnar}) return identical query results for the same queries.
 *
 * <p>The test targets the execution-path diversity across modes: {@code standard}/{@code logsdb} serve
 * term/terms/range queries via postings, while {@code columnar}/{@code logsdb_columnar} default to
 * {@code index: false} on all fields (via {@code IndexSettings.INDEX_DISABLED_BY_DEFAULT}) and serve
 * every query through doc-values iterators — the exact path where elastic/elasticsearch#155653 produced
 * false positives due to {@code DocValuesRangeIterator.docIDRunEnd()} skipping per-doc {@code matches()}
 * calls. {@code time_series} is included because its forced {@code _tsid} sort and dimension-derived
 * {@code _id} make it structurally different from the rest.
 */
public class IndexModeQueryEquivalenceIT extends ESIntegTestCase {

    // Small set of discrete values per field drives long runs of the same value within each index,
    // which is what produces the mixed dense/sparse Lucene blocks (MAYBE/YES_IF_PRESENT) that the
    // doc-values bulk-scorer optimizes. Uniformly random values per document would rarely build them.
    private static final String[] DIM_A_VALUES = { "target", "other" };
    private static final String[] DIM_B_VALUES = { "match", "mismatch" };
    private static final String[] TAG_VALUES = { "alpha", "excluded", "gamma" };
    private static final long[] VALUE_VALS = { 10L, 20L, 30L, 40L };

    // Shared across all five index-mode indices.
    private static final String TS_START = "2024-01-01T00:00:00Z";
    private static final String TS_END = "2025-01-01T00:00:00Z";
    // 2024-01-01T00:00:00Z in epoch-millis.
    private static final long BASE_TS = 1704067200000L;

    @Override
    protected Settings.Builder setRandomIndexSettings(Random random, Settings.Builder builder) {
        // Columnar modes require DOC_VALUES_ONLY for seq_no. Strip the randomly chosen value so it
        // does not conflict with each mode's seq_no default (DOC_VALUES_ONLY or disabled entirely).
        return super.setRandomIndexSettings(random, builder).remove(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey());
    }

    public void testFixedAndRandomQueriesAreEquivalentAcrossAllIndexModes() throws Exception {
        final String standard = "idx_standard";
        final String logsdb = "idx_logsdb";
        final String columnar = "idx_columnar";
        final String timeSeries = "idx_time_series";
        final String logsdbColumnar = "idx_logsdb_columnar";
        final List<String> allIndices = List.of(standard, logsdb, columnar, timeSeries, logsdbColumnar);

        createIndices(standard, logsdb, columnar, timeSeries, logsdbColumnar);

        final int numDocs = randomIntBetween(5000, 10_000);
        final List<Doc> docs = generateDocs(numDocs);
        indexAll(docs, allIndices);

        // Independently force-merge each index so segment layouts differ across modes. A single-segment
        // index reliably triggers DenseConjunctionBulkScorer, which the original bug lived inside.
        for (String index : allIndices) {
            if (randomBoolean()) {
                indicesAdmin().prepareForceMerge(index).setMaxNumSegments(randomIntBetween(1, 3)).get();
            }
        }
        indicesAdmin().prepareRefresh(allIndices.toArray(new String[0])).get();

        final List<QueryBuilder> queries = buildQuerySet(docs);
        for (QueryBuilder query : queries) {
            assertEquivalentAcrossModes(query, standard, logsdb, columnar, timeSeries, logsdbColumnar);
        }
    }

    // ---- index creation ----------------------------------------------------

    private void createIndices(String standard, String logsdb, String columnar, String timeSeries, String logsdbColumnar) throws Exception {
        assertAcked(prepareCreate(standard).setSettings(commonSettings(IndexMode.STANDARD)).setMapping(sharedMapping(false)));
        assertAcked(prepareCreate(logsdb).setSettings(commonSettings(IndexMode.LOGSDB)).setMapping(sharedMapping(false)));
        assertAcked(prepareCreate(columnar).setSettings(commonSettings(IndexMode.COLUMNAR)).setMapping(sharedMapping(false)));
        assertAcked(prepareCreate(timeSeries).setSettings(timeSeriesSettings()).setMapping(sharedMapping(true)));
        assertAcked(prepareCreate(logsdbColumnar).setSettings(commonSettings(IndexMode.LOGSDB_COLUMNAR)).setMapping(sharedMapping(false)));
    }

    private Settings commonSettings(IndexMode mode) {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), mode.getName())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 3))
            .put("index.queries.cache.enabled", false)
            .build();
    }

    private Settings timeSeriesSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim_a,dim_b")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), TS_START)
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), TS_END)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 3))
            .put("index.queries.cache.enabled", false)
            .build();
    }

    /**
     * A single mapping shared across all five modes.
     * <p>{@code dim_a} and {@code dim_b} are declared as {@code time_series_dimension: true} so that
     * they can appear in {@code index.routing_path} for the {@code time_series} index. The
     * {@code dimensionParam} validator only checks {@code doc_values: true}, which is the default, so
     * the annotation is accepted by every other mode without any special handling.
     * <p>{@code tag} uses {@code index: false} to ensure it is always served via doc values — mirroring
     * the repro test — in every mode, including those that do not default to {@code index: false}.
     *
     * @param withDataStream when {@code true} the {@code _data_stream_timestamp} meta-field is enabled,
     *                       required by {@code time_series}.
     */
    private XContentBuilder sharedMapping(boolean withDataStream) throws Exception {
        var b = XContentFactory.jsonBuilder().startObject();
        if (withDataStream) {
            b.startObject("_data_stream_timestamp").field("enabled", true).endObject();
        }
        b.startObject("properties")
            .startObject("@timestamp")
            .field("type", "date")
            .endObject()
            .startObject("dim_a")
            .field("type", "keyword")
            .field("time_series_dimension", true)
            .endObject()
            .startObject("dim_b")
            .field("type", "keyword")
            .field("time_series_dimension", true)
            .endObject()
            // index:false forces doc-values execution in every mode, keeping the playing field even for
            // "tag" (the must_not target in all fixed queries) regardless of the mode's index defaults.
            .startObject("tag")
            .field("type", "keyword")
            .field("index", false)
            .endObject()
            .startObject("value")
            .field("type", "long")
            .endObject()
            .startObject("doc_id")
            .field("type", "long")
            .endObject()
            .endObject()
            .endObject();
        return b;
    }

    // ---- document generation -----------------------------------------------

    record Doc(long ts, String dimA, String dimB, String tag, long value, long docId) {}

    /**
     * Generates documents using a run-length strategy: each field value is held constant for a random
     * number of documents before switching. Long runs of a low-cardinality value build the mixed
     * dense/sparse Lucene blocks (MAYBE/YES_IF_PRESENT) that the doc-values bulk-scorer optimizes over.
     * <p>Timestamps are strictly increasing (BASE_TS + docId) so that two documents that happen to
     * share the same {@code dim_a}+{@code dim_b} combination do not collide in the {@code time_series}
     * index, where {@code _id} is derived from {@code _tsid} and {@code @timestamp}.
     */
    private List<Doc> generateDocs(int n) {
        final List<Doc> docs = new ArrayList<>(n);
        int dimAIdx = 0, dimBIdx = 0, tagIdx = 0, valueIdx = 0;
        int dimARun = 0, dimBRun = 0, tagRun = 0, valueRun = 0;
        for (int i = 0; i < n; i++) {
            if (dimARun-- <= 0) {
                dimAIdx = (dimAIdx + 1) % DIM_A_VALUES.length;
                dimARun = randomIntBetween(100, 3000);
            }
            if (dimBRun-- <= 0) {
                dimBIdx = (dimBIdx + 1) % DIM_B_VALUES.length;
                dimBRun = randomIntBetween(100, 3000);
            }
            if (tagRun-- <= 0) {
                tagIdx = (tagIdx + 1) % TAG_VALUES.length;
                tagRun = randomIntBetween(100, 3000);
            }
            if (valueRun-- <= 0) {
                valueIdx = (valueIdx + 1) % VALUE_VALS.length;
                valueRun = randomIntBetween(50, 200);
            }
            docs.add(new Doc(BASE_TS + i, DIM_A_VALUES[dimAIdx], DIM_B_VALUES[dimBIdx], TAG_VALUES[tagIdx], VALUE_VALS[valueIdx], i));
        }
        return docs;
    }

    private void indexAll(List<Doc> docs, List<String> indices) throws Exception {
        // Build all requests once; builders is ordered as [all docs for index0, all docs for index1, ...].
        final List<IndexRequestBuilder> builders = new ArrayList<>(docs.size() * indices.size());
        for (String index : indices) {
            for (Doc doc : docs) {
                builders.add(
                    prepareIndex(index).setSource(
                        "@timestamp",
                        doc.ts(),
                        "dim_a",
                        doc.dimA(),
                        "dim_b",
                        doc.dimB(),
                        "tag",
                        doc.tag(),
                        "value",
                        doc.value(),
                        "doc_id",
                        doc.docId()
                    )
                );
            }
        }

        if (randomBoolean()) {
            // Ordered: send one bulk per index in run-length order so MAYBE/YES_IF_PRESENT Lucene
            // blocks form in the segment, exercising the doc-values bulk-scorer path.
            int offset = 0;
            for (String index : indices) {
                var bulk = client().prepareBulk();
                builders.subList(offset, offset + docs.size()).forEach(bulk::add);
                bulk.get();
                offset += docs.size();
            }
            indicesAdmin().prepareRefresh(indices.toArray(new String[0])).get();
        } else {
            // Shuffled: exercises realistic multi-segment layouts and random arrival order.
            // dummyDocuments=false: synthetic docs lack @timestamp and carry explicit routing,
            // both of which are rejected by logsdb/time_series modes.
            indexRandom(true, false, builders);
        }
    }

    // ---- query set ---------------------------------------------------------

    /**
     * Fixed queries ported from {@link org.elasticsearch.search.TsdbBoolMustNotReproductionTests},
     * plus a small set of randomly generated bool queries combining term/terms/range filters.
     * <p>All queries are wrapped in {@code constant_score} to neutralise BM25 scoring differences:
     * {@code standard}/{@code logsdb} score keyword terms via postings, while
     * {@code columnar}/{@code logsdb_columnar} default to {@code index: false} for keyword fields and
     * use constant-score doc-values queries. Without wrapping, the score difference is legitimate, but
     * it complicates result comparison unnecessarily.
     */
    private List<QueryBuilder> buildQuerySet(List<Doc> docs) {
        final List<QueryBuilder> queries = new ArrayList<>();

        // Fixed queries (shapes derived from TsdbBoolMustNotReproductionTests).
        queries.add(
            QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("dim_a", "target")).mustNot(QueryBuilders.termQuery("tag", "excluded"))
        );
        queries.add(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("dim_a", "target"))
                .filter(QueryBuilders.termsQuery("dim_b", "match"))
                .mustNot(QueryBuilders.termQuery("tag", "excluded"))
        );
        queries.add(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.rangeQuery("@timestamp").gte(BASE_TS).lt(BASE_TS + 1500))
                .mustNot(QueryBuilders.termsQuery("tag", "excluded", "alpha"))
        );
        queries.add(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("dim_a", "target"))
                .filter(QueryBuilders.rangeQuery("value").gte(20L).lte(30L))
                .mustNot(QueryBuilders.termQuery("dim_b", "mismatch"))
        );

        // Randomly generated queries.
        final int numRandom = randomIntBetween(5, 8);
        for (int i = 0; i < numRandom; i++) {
            queries.add(randomBoolQuery());
        }

        return queries;
    }

    /**
     * Builds a random {@code bool} query with 1–3 clauses drawn from term/terms/range over the mapped
     * fields. Clauses are placed randomly in filter/must/must_not/should positions.
     */
    private BoolQueryBuilder randomBoolQuery() {
        final BoolQueryBuilder bool = QueryBuilders.boolQuery();
        final int numClauses = randomIntBetween(1, 3);
        boolean hasShouldOnly = true;
        for (int i = 0; i < numClauses; i++) {
            final QueryBuilder leaf = randomLeafQuery();
            final int pos = randomIntBetween(0, 3);
            switch (pos) {
                case 0 -> {
                    bool.filter(leaf);
                    hasShouldOnly = false;
                }
                case 1 -> {
                    bool.must(leaf);
                    hasShouldOnly = false;
                }
                case 2 -> bool.mustNot(leaf);
                case 3 -> bool.should(leaf);
            }
        }
        if (hasShouldOnly && bool.should().isEmpty() == false) {
            bool.minimumShouldMatch(1);
        }
        // A pure must_not bool with no positive clause matches everything — make it explicit.
        if (bool.filter().isEmpty() && bool.must().isEmpty() && bool.should().isEmpty()) {
            bool.filter(QueryBuilders.matchAllQuery());
        }
        return bool;
    }

    private QueryBuilder randomLeafQuery() {
        return switch (randomIntBetween(0, 4)) {
            case 0 -> QueryBuilders.termQuery("dim_a", randomFrom(DIM_A_VALUES));
            case 1 -> QueryBuilders.termQuery("dim_b", randomFrom(DIM_B_VALUES));
            case 2 -> QueryBuilders.termQuery("tag", randomFrom(TAG_VALUES));
            case 3 -> QueryBuilders.termsQuery("tag", randomSubset(TAG_VALUES));
            default -> QueryBuilders.rangeQuery("value").gte(VALUE_VALS[randomIntBetween(0, 1)]).lte(VALUE_VALS[randomIntBetween(2, 3)]);
        };
    }

    private String[] randomSubset(String[] values) {
        final int size = randomIntBetween(1, values.length);
        final List<String> copy = new ArrayList<>(Arrays.asList(values));
        java.util.Collections.shuffle(copy, random());
        return copy.subList(0, size).toArray(new String[0]);
    }

    // ---- equivalence assertion ---------------------------------------------

    /**
     * Runs {@code query} against every supplied index and asserts that all return the same total hit
     * count and the same ordered list of {@code doc_id} values.
     * <p>Sorting by {@code doc_id} asc gives a strict, mode-independent total order. Each mode has a
     * different default index sort ({@code time_series}: {@code [_tsid asc, @timestamp desc]};
     * {@code logsdb}/{@code logsdb_columnar}: {@code [@timestamp desc]}; others: none), so an
     * explicit sort is required for comparable results.
     * <p>Additionally verifies that {@code profile: true} and {@code profile: false} agree for each
     * index, since profiling bypasses the bulk scorer path where elastic/elasticsearch#155653 was
     * producing false positives.
     */
    private void assertEquivalentAcrossModes(QueryBuilder query, String... indices) {
        final QueryBuilder wrapped = QueryBuilders.constantScoreQuery(query).boost(1.0f);
        final int size = randomIntBetween(10, 500);

        // Collect (totalHits, doc_id list) from each index.
        final long[] totalHitsByIndex = new long[indices.length];
        final List<List<Long>> docIdsByIndex = new ArrayList<>(indices.length);

        for (int i = 0; i < indices.length; i++) {
            final String index = indices[i];
            final int idx = i;
            assertResponse(
                client().prepareSearch(index)
                    .setTrackTotalHits(true)
                    .setQuery(wrapped)
                    .addSort(SortBuilders.fieldSort("doc_id").order(SortOrder.ASC))
                    .setSize(size)
                    .addDocValueField("doc_id"),
                response -> {
                    totalHitsByIndex[idx] = response.getHits().getTotalHits().value();
                    final List<Long> docIds = new ArrayList<>();
                    for (SearchHit hit : response.getHits().getHits()) {
                        docIds.add(((Number) hit.field("doc_id").getValue()).longValue());
                    }
                    docIdsByIndex.add(docIds);
                }
            );

            // profile:true must agree with profile:false on total hits.
            final long[] profileHits = new long[1];
            assertResponse(
                client().prepareSearch(index).setTrackTotalHits(true).setProfile(true).setQuery(wrapped).setSize(0),
                response -> profileHits[0] = response.getHits().getTotalHits().value()
            );
            assertEquals(
                "profile:true and profile:false disagree for index [" + index + "] query [" + query + "]",
                profileHits[0],
                totalHitsByIndex[idx]
            );
        }

        // All indices must agree on total hits and the ordered doc_id prefix returned.
        final long baselineHits = totalHitsByIndex[0];
        final List<Long> baselineDocIds = docIdsByIndex.get(0);
        final String baselineIndex = indices[0];

        // Guard: the first fixed query must return a non-trivial result on the baseline index. Randomized
        // queries can match anything, so we only check the baseline count is non-negative (always true).
        assertTrue(
            "baseline index [" + baselineIndex + "] returned no hits for query [" + query + "] — query matches nothing",
            baselineHits >= 0
        );

        for (int i = 1; i < indices.length; i++) {
            assertEquals(
                "total hits differ between [" + baselineIndex + "] and [" + indices[i] + "] for query [" + query + "]",
                baselineHits,
                totalHitsByIndex[i]
            );
            assertEquals(
                "doc_id list differs between [" + baselineIndex + "] and [" + indices[i] + "] for query [" + query + "]",
                baselineDocIds,
                docIdsByIndex.get(i)
            );
        }
    }
}
