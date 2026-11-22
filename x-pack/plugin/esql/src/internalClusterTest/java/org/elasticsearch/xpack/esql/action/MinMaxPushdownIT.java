/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.tests.util.LuceneTestCase.SuppressCodecs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.Predicate;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * End to end coverage of ungrouped {@code MIN}/{@code MAX} that the data nodes answer from segment metadata instead of reading the
 * field. The same checks run on a standard index (points) and a columnar index (doc values skipper), because the metadata that bounds
 * a segment differs between them. The important property is that documents the query does not see never leak into the result:
 * the segment bounds still include deleted documents, and an alias filter is not reflected in the segments at all.
 */
// The production per-field codec, so the columnar fields are real ColumNAR columns. ESIntegTestCase otherwise picks a codec that bypasses
// it.
@SuppressCodecs("*")
public class MinMaxPushdownIT extends AbstractEsqlIntegTestCase {
    private static final String LOCAL_SOURCE_OPERATOR = "LocalSourceOperator";
    private static final String LUCENE_SOURCE_OPERATOR = "LuceneSourceOperator";

    /** The fields every document holds a value of, each derived from the document number. */
    private static final List<String> FIELDS = List.of("d", "dn", "l", "i", "s", "b");

    private static final long BASE_DATE = 1_600_000_000_000L;
    private static final int DOCS_PER_SEGMENT = 50;
    private static final int SEGMENTS = 4;

    @Override
    protected Settings.Builder setRandomIndexSettings(Random random, Settings.Builder builder) {
        return super.setRandomIndexSettings(random, builder).remove(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey());
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    /** The outcome of a profiled query: the single result row and which sources the data node drivers used. */
    private record Outcome(List<Object> row, boolean localSource, boolean luceneSource) {}

    /** What the index holds: document id to the value of each field, in the order of {@link #FIELDS}. */
    private final Map<String, long[]> docs = new TreeMap<>();

    /** The value of a field in document {@code n}, spread so that the extremes are in different documents for every field. */
    private static long valueOf(String field, int n) {
        return switch (field) {
            case "d" -> BASE_DATE + n * 1000L;
            case "dn" -> BASE_DATE * 1_000_000L + (n * 31L % 1000) * 1_000_000L;
            case "l" -> 5_000_000_000L - n * 1_000_003L;
            case "i" -> n * 7 - 300;
            case "s" -> (n * 13) % 200 - 100;
            case "b" -> (n * 5) % 100 - 50;
            default -> throw new IllegalArgumentException(field);
        };
    }

    private void createIndex(String index, boolean columnar, int shards) {
        Settings.Builder settings = Settings.builder().put("index.number_of_shards", shards).put("index.number_of_replicas", 0);
        if (columnar) {
            settings.put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName());
        }
        assertAcked(
            prepareCreate(index).setSettings(settings)
                .setMapping(
                    "d",
                    "type=date",
                    "dn",
                    "type=date_nanos",
                    "l",
                    "type=long",
                    "i",
                    "type=integer",
                    "s",
                    "type=short",
                    "b",
                    "type=byte",
                    "other",
                    "type=long",
                    "d_alias",
                    "type=alias,path=d",
                    "i_alias",
                    "type=alias,path=i"
                )
        );
    }

    private void indexDoc(String index, int n, boolean withValues) {
        if (withValues == false) {
            prepareIndex(index).setId("doc_" + n).setSource("other", n).get();
            return;
        }
        Object[] source = new Object[FIELDS.size() * 2];
        long[] values = new long[FIELDS.size()];
        for (int f = 0; f < FIELDS.size(); f++) {
            values[f] = valueOf(FIELDS.get(f), n);
            source[f * 2] = FIELDS.get(f);
            // a number is read as milliseconds, so nanoseconds are given as a timestamp
            source[f * 2 + 1] = FIELDS.get(f).equals("dn") ? Instant.ofEpochSecond(0, values[f]).toString() : values[f];
        }
        prepareIndex(index).setId("doc_" + n).setSource(source).get();
        docs.put("doc_" + n, values);
    }

    /** Indexes dense data in several segments, so that deletions can later hit some segments and not others. */
    private void indexDense(String index) {
        docs.clear();
        for (int segment = 0; segment < SEGMENTS; segment++) {
            for (int doc = 0; doc < DOCS_PER_SEGMENT; doc++) {
                indexDoc(index, segment * DOCS_PER_SEGMENT + doc, true);
            }
            indicesAdmin().prepareFlush(index).get();
        }
        indicesAdmin().prepareRefresh(index).get();
    }

    /** The smallest or largest value of the field over the documents that hold it, accepted by {@code keep}. */
    private Long expected(String field, boolean min, Predicate<long[]> keep) {
        int f = FIELDS.indexOf(field);
        Long result = null;
        for (long[] doc : docs.values()) {
            if (keep.test(doc)) {
                result = result == null ? doc[f] : min ? Math.min(result, doc[f]) : Math.max(result, doc[f]);
            }
        }
        return result;
    }

    /** The expected result row of {@link #allFieldsQuery}. */
    private List<Long> expectedRow(Predicate<long[]> keep) {
        List<Long> row = new ArrayList<>();
        for (String field : FIELDS) {
            row.add(expected(field, true, keep));
            row.add(expected(field, false, keep));
        }
        return row;
    }

    /**
     * Asks for the smallest and largest of every field in one query. Dates become longs afterwards, which leaves the data node plan
     * alone.
     */
    private static String allFieldsQuery(String source) {
        StringBuilder stats = new StringBuilder();
        StringBuilder convert = new StringBuilder();
        for (String field : FIELDS) {
            stats.append(stats.isEmpty() ? "" : ", ").append("min_").append(field).append(" = MIN(").append(field).append("), ");
            stats.append("max_").append(field).append(" = MAX(").append(field).append(")");
            for (String bound : List.of("min_", "max_")) {
                convert.append(convert.isEmpty() ? "" : ", ").append(bound).append(field).append(" = TO_LONG(").append(bound).append(field);
                convert.append(")");
            }
        }
        return source + " | STATS " + stats + " | EVAL " + convert;
    }

    private Outcome profiled(String query, QueryBuilder filter) {
        return profiled(query, filter, getPragmas());
    }

    private Outcome profiled(String query, QueryBuilder filter, QueryPragmas pragmas) {
        EsqlQueryRequest request = syncEsqlQueryRequest(query).pragmas(pragmas);
        request.profile(true);
        if (filter != null) {
            request.filter(filter);
        }
        try (EsqlQueryResponse response = run(request)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), is(1));
            boolean local = false;
            boolean lucene = false;
            for (DriverProfile driver : response.profile().drivers()) {
                for (OperatorStatus op : driver.operators()) {
                    local |= op.operator().startsWith(LOCAL_SOURCE_OPERATOR);
                    lucene |= op.operator().startsWith(LUCENE_SOURCE_OPERATOR);
                }
            }
            return new Outcome(rows.get(0), local, lucene);
        }
    }

    private static List<Long> asLongs(List<Object> row) {
        List<Long> result = new ArrayList<>();
        for (Object value : row) {
            result.add(value == null ? null : ((Number) value).longValue());
        }
        return result;
    }

    private void assertAnsweredFromMetadata(String index, String source, Predicate<long[]> keep) {
        Outcome outcome = profiled(allFieldsQuery(source), null);
        assertThat(index, asLongs(outcome.row), equalTo(expectedRow(keep)));
        assertThat(index + " is answered without reading the index", outcome.luceneSource, is(false));
        assertThat(index + " is answered by a local source", outcome.localSource, is(true));
    }

    private void assertAnsweredByScan(String index, String source, Predicate<long[]> keep, QueryBuilder filter) {
        Outcome outcome = profiled(allFieldsQuery(source), filter);
        assertThat(index, asLongs(outcome.row), equalTo(expectedRow(keep)));
        assertThat(index + " reads the index", outcome.luceneSource, is(true));
        assertThat(index + " is not answered by a local source", outcome.localSource, is(false));
    }

    /**
     * A field that is indexed but left out of {@code _source} is read as it is stored when the stored preference asks for it. For a
     * number that differs from the metadata, which holds the indexed value, so the query must read the index and give what a scan
     * gives. A date is read from its doc values whatever the preference, so it is still answered from the metadata, with what a scan
     * gives.
     */
    public void testStoredFieldExtractPreference() {
        assumeTrue("requires the snapshot-only field_extract_preference pragma", canUseQueryPragmas());
        assertAcked(
            prepareCreate("stored_pref").setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .setMapping("""
                    {
                      "_source": { "excludes": ["x", "d", "dn"] },
                      "properties": {
                        "x": { "type": "long" },
                        "d": { "type": "date" },
                        "dn": { "type": "date_nanos" },
                        "other": { "type": "long" }
                      }
                    }
                    """)
        );
        for (int n = 0; n < 20; n++) {
            prepareIndex("stored_pref").setId("doc_" + n)
                .setSource(
                    "x",
                    100 + n,
                    "d",
                    BASE_DATE + n * 1000L,
                    "dn",
                    Instant.ofEpochSecond(0, BASE_DATE * 1_000_000L + n * 1_000_000L).toString(),
                    "other",
                    n
                )
                .get();
        }
        indicesAdmin().prepareRefresh("stored_pref").get();

        QueryPragmas stored = new QueryPragmas(Settings.builder().put("field_extract_preference", "stored").build());
        for (String field : List.of("x", "d", "dn")) {
            boolean isDate = field.equals("x") == false;
            String query = "FROM stored_pref | STATS min = MIN("
                + field
                + "), max = MAX("
                + field
                + ") | EVAL min = TO_LONG(min), max = TO_LONG(max)";
            // the bounds of what is indexed, which is what the metadata holds
            List<Long> indexed = switch (field) {
                case "x" -> List.of(100L, 119L);
                case "d" -> List.of(BASE_DATE, BASE_DATE + 19_000L);
                case "dn" -> List.of(BASE_DATE * 1_000_000L, BASE_DATE * 1_000_000L + 19_000_000L);
                default -> throw new IllegalArgumentException(field);
            };

            Outcome metadata = profiled(query, null);
            assertThat(field, metadata.localSource, is(true));
            assertThat(field, asLongs(metadata.row), equalTo(indexed));

            Outcome storedPlan = profiled(query, null, stored);
            // a filter that keeps every document makes the same query a scan for sure
            Outcome reference = profiled(query, QueryBuilders.rangeQuery("other").gte(0), stored);
            assertThat(field, reference.luceneSource, is(true));
            assertThat(field, reference.localSource, is(false));
            // a number that is not in the stored source reads as null, a date still comes from its doc values
            List<Long> scanned = isDate ? indexed : Arrays.asList(null, null);
            assertThat(field + " as a scan reads it", asLongs(reference.row), equalTo(scanned));
            assertThat(field, asLongs(storedPlan.row), equalTo(scanned));
            assertThat(field + " is answered from the metadata only if it is a date", storedPlan.localSource, is(isDate));
            assertThat(field, storedPlan.luceneSource, is(isDate == false));
        }
    }

    public void testMinMaxFromMetadata() {
        for (boolean columnar : new boolean[] { false, true }) {
            String index = "dense_" + columnar;
            createIndex(index, columnar, 1);
            indexDense(index);
            assertAnsweredFromMetadata(index, "FROM " + index, doc -> true);
        }
    }

    public void testMinMaxFromMetadataAcrossShards() {
        for (boolean columnar : new boolean[] { false, true }) {
            String index = "sharded_" + columnar;
            createIndex(index, columnar, 3);
            indexDense(index);
            assertAnsweredFromMetadata(index, "FROM " + index, doc -> true);
        }
    }

    /** Several indices are read by one node, and the answer is the bounds of all of them. */
    public void testMinMaxFromMetadataAcrossIndices() {
        createIndex("across_points", false, 1);
        createIndex("across_skipper", true, 2);
        docs.clear();
        for (int n = 0; n < DOCS_PER_SEGMENT * SEGMENTS; n++) {
            indexDoc(n % 2 == 0 ? "across_points" : "across_skipper", n, true);
        }
        indicesAdmin().prepareRefresh("across_points", "across_skipper").get();
        assertAnsweredFromMetadata("across", "FROM across_points,across_skipper", doc -> true);
    }

    /** A segment of an index that has deleted documents still has bounds that include them, so the index is read. */
    public void testDeletedAndUpdatedDocumentsAreIgnored() {
        for (boolean columnar : new boolean[] { false, true }) {
            String index = "deletes_" + columnar;
            createIndex(index, columnar, 1);
            indexDense(index);
            assertAnsweredFromMetadata(index, "FROM " + index, doc -> true);

            // delete the documents holding the extremes of most fields
            client().prepareDelete(index, "doc_0").get();
            docs.remove("doc_0");
            int last = SEGMENTS * DOCS_PER_SEGMENT - 1;
            client().prepareDelete(index, "doc_" + last).get();
            docs.remove("doc_" + last);
            indicesAdmin().prepareRefresh(index).get();
            assertAnsweredByScan(index, "FROM " + index, doc -> true, null);

            // an update leaves its old version in a segment
            indexDoc(index, 1, true);
            indicesAdmin().prepareRefresh(index).get();
            assertAnsweredByScan(index, "FROM " + index, doc -> true, null);

            // after the merge the deleted documents may or may not still be in the segment (soft deletes are retained for a
            // while), either way the result must not change
            indicesAdmin().prepareForceMerge(index).setMaxNumSegments(1).get();
            indicesAdmin().prepareRefresh(index).get();
            Outcome outcome = profiled(allFieldsQuery("FROM " + index), null);
            assertThat(index, asLongs(outcome.row), equalTo(expectedRow(doc -> true)));
        }
    }

    /** A filter means the metadata does not describe the matching documents: the index is read. */
    public void testFilterIsNotAnsweredFromMetadata() {
        for (boolean columnar : new boolean[] { false, true }) {
            String index = "filtered_" + columnar;
            createIndex(index, columnar, 1);
            indexDense(index);
            int threshold = (int) valueOf("i", 100);
            int fi = FIELDS.indexOf("i");
            assertAnsweredByScan(index, "FROM " + index + " | WHERE i > " + threshold, doc -> doc[fi] > threshold, null);
            // the same through the filter of the request
            assertAnsweredByScan(index, "FROM " + index, doc -> doc[fi] > threshold, QueryBuilders.rangeQuery("i").gt(threshold));
        }
    }

    /**
     * An index alias with a filter restricts every shard query, but the filter is added when the shard query is built, so it is
     * invisible to the plan and to the statistics of the segments. The result has to cover only the documents the filter lets through.
     */
    public void testIndexAliasFilter() {
        for (boolean columnar : new boolean[] { false, true }) {
            String index = "aliased_" + columnar;
            createIndex(index, columnar, 1);
            indexDense(index);
            int threshold = (int) valueOf("i", 100);
            int fi = FIELDS.indexOf("i");
            String alias = index + "_filtered";
            assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .addAlias(index, alias, QueryBuilders.rangeQuery("i").gt(threshold))
            );
            assertAnsweredByScan(alias, "FROM " + alias, doc -> doc[fi] > threshold, null);
        }
    }

    /** Documents without the field are ignored, even when whole segments hold none. */
    public void testMissingValues() {
        for (boolean columnar : new boolean[] { false, true }) {
            String index = "sparse_" + columnar;
            createIndex(index, columnar, 1);
            docs.clear();
            // a segment without any value for the fields, then segments where only some documents have one
            for (int doc = 0; doc < DOCS_PER_SEGMENT; doc++) {
                indexDoc(index, doc, false);
            }
            indicesAdmin().prepareFlush(index).get();
            for (int segment = 1; segment < SEGMENTS; segment++) {
                for (int doc = 0; doc < DOCS_PER_SEGMENT; doc++) {
                    indexDoc(index, segment * DOCS_PER_SEGMENT + doc, doc % 3 != 0);
                }
                indicesAdmin().prepareFlush(index).get();
            }
            indicesAdmin().prepareRefresh(index).get();
            assertAnsweredFromMetadata(index, "FROM " + index, doc -> true);
        }
    }

    /** A field alias returns the bounds of the field it points to, which is the one the segments hold. */
    public void testFieldAlias() {
        for (boolean columnar : new boolean[] { false, true }) {
            String index = "aliased_field_" + columnar;
            createIndex(index, columnar, 1);
            indexDense(index);
            Outcome outcome = profiled(
                "FROM "
                    + index
                    + " | STATS lo = MIN(d_alias), hi = MAX(d_alias), lo_i = MIN(i_alias), hi_i = MAX(i_alias)"
                    + " | EVAL lo = TO_LONG(lo), hi = TO_LONG(hi)",
                null
            );
            // the date columns come last: EVAL moves the columns it replaces to the end
            assertThat(
                index,
                asLongs(outcome.row),
                equalTo(
                    List.of(
                        expected("i", true, doc -> true),
                        expected("i", false, doc -> true),
                        expected("d", true, doc -> true),
                        expected("d", false, doc -> true)
                    )
                )
            );
            assertThat(outcome.luceneSource, is(false));
            assertThat(outcome.localSource, is(true));
        }
    }

    /** An index without documents has no values: MIN and MAX are null. */
    public void testEmptyIndex() {
        for (boolean columnar : new boolean[] { false, true }) {
            String index = "empty_" + columnar;
            createIndex(index, columnar, 2);
            docs.clear();
            Outcome outcome = profiled(allFieldsQuery("FROM " + index), null);
            for (Object value : outcome.row) {
                assertThat(index, value, equalTo(null));
            }
        }
    }

    /** A multi-valued field takes the smallest and the largest of all values: the segment bounds cover all of them. */
    public void testMultiValuedField() {
        for (boolean columnar : new boolean[] { false, true }) {
            String index = "multivalued_" + columnar;
            createIndex(index, columnar, 1);
            long lowest = Long.MAX_VALUE;
            long highest = Long.MIN_VALUE;
            for (int n = 0; n < 100; n++) {
                long a = BASE_DATE + n * 1000L;
                long b = BASE_DATE + (1000 - n) * 1000L;
                lowest = Math.min(lowest, Math.min(a, b));
                highest = Math.max(highest, Math.max(a, b));
                prepareIndex(index).setId("doc_" + n).setSource("d", List.of(a, b)).get();
                if (n % 25 == 24) {
                    indicesAdmin().prepareFlush(index).get();
                }
            }
            indicesAdmin().prepareRefresh(index).get();

            Outcome outcome = profiled(
                "FROM " + index + " | STATS lo = MIN(d), hi = MAX(d) | EVAL lo = TO_LONG(lo), hi = TO_LONG(hi)",
                null
            );
            assertThat(asLongs(outcome.row), equalTo(List.of(lowest, highest)));
            assertThat(outcome.luceneSource, is(false));
            assertThat(outcome.localSource, is(true));
        }
    }
}
