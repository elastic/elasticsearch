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
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * End to end coverage of {@code MIN}/{@code MAX} over one date or integer field answered by the {@code LuceneMinMaxOperator}.
 * The same checks run on a standard index (points) and a columnar index (doc values skipper), because the metadata that
 * bounds a segment differs between them. The important property is that deleted and updated documents never leak into the
 * result: the segment bounds still include them, so segments with deletions must be scanned.
 */
// The production per-field codec, so the columnar fields are real ColumNAR columns. ESIntegTestCase otherwise picks a codec that bypasses
// it.
@SuppressCodecs("*")
public class MinMaxPushdownIT extends AbstractEsqlIntegTestCase {
    private static final String MIN_MAX_OPERATOR = "LuceneMinMaxOperator";
    private static final String SOURCE_OPERATOR = "LuceneSourceOperator";
    private static final String VALUES_READER_OPERATOR = "ValuesSourceReaderOperator";

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
    private record Outcome(List<Object> row, boolean minMaxSource, boolean luceneSource, boolean valuesReader) {}

    /** What the index holds: document id to {date, integer}. */
    private final Map<String, long[]> docs = new TreeMap<>();

    private static long dateOf(int n) {
        return BASE_DATE + n * 1000L;
    }

    private static int intOf(int n) {
        return n * 7 - 300;
    }

    private void createIndex(String index, boolean columnar) {
        Settings.Builder settings = Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0);
        if (columnar) {
            settings.put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName());
        }
        assertAcked(prepareCreate(index).setSettings(settings).setMapping("d", "type=date", "i", "type=integer", "other", "type=long"));
    }

    private void indexDoc(String index, int n, long date, int value) {
        prepareIndex(index).setId("doc_" + n).setSource("d", date, "i", value).get();
        docs.put("doc_" + n, new long[] { date, value });
    }

    /** Indexes dense data in several segments, so that deletions can later hit some segments and not others. */
    private void indexDense(String index) {
        docs.clear();
        for (int segment = 0; segment < SEGMENTS; segment++) {
            for (int doc = 0; doc < DOCS_PER_SEGMENT; doc++) {
                int n = segment * DOCS_PER_SEGMENT + doc;
                indexDoc(index, n, dateOf(n), intOf(n));
            }
            indicesAdmin().prepareFlush(index).get();
        }
        indicesAdmin().prepareRefresh(index).get();
    }

    private long expected(boolean date, boolean min) {
        long result = min ? Long.MAX_VALUE : Long.MIN_VALUE;
        for (long[] doc : docs.values()) {
            long value = doc[date ? 0 : 1];
            result = min ? Math.min(result, value) : Math.max(result, value);
        }
        return result;
    }

    private Outcome profiled(String query) {
        EsqlQueryRequest request = syncEsqlQueryRequest(query).pragmas(getPragmas());
        request.profile(true);
        try (EsqlQueryResponse response = run(request)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), is(1));
            boolean minMax = false;
            boolean source = false;
            boolean reader = false;
            for (DriverProfile driver : response.profile().drivers()) {
                for (OperatorStatus op : driver.operators()) {
                    minMax |= op.operator().startsWith(MIN_MAX_OPERATOR);
                    source |= op.operator().startsWith(SOURCE_OPERATOR);
                    reader |= op.operator().startsWith(VALUES_READER_OPERATOR);
                }
            }
            return new Outcome(rows.get(0), minMax, source, reader);
        }
    }

    private static List<Long> asLongs(List<Object> row) {
        List<Long> result = new ArrayList<>();
        for (Object value : row) {
            result.add(value == null ? null : ((Number) value).longValue());
        }
        return result;
    }

    /** Queries MIN and MAX of one field. Dates are converted to longs after the aggregation, which does not change the data node plan. */
    private Outcome minMax(String index, boolean date, String filter) {
        String field = date ? "d" : "i";
        String convert = date ? " | EVAL lo = TO_LONG(lo), hi = TO_LONG(hi)" : "";
        return profiled("FROM " + index + filter + " | STATS lo = MIN(" + field + "), hi = MAX(" + field + ")" + convert);
    }

    private void assertMinMax(String index, boolean date) {
        Outcome outcome = minMax(index, date, "");
        assertThat(
            index + (date ? " date" : " integer"),
            asLongs(outcome.row),
            equalTo(List.of(expected(date, true), expected(date, false)))
        );
        assertThat(index + " uses the min/max source", outcome.minMaxSource, is(true));
        assertThat(index + " does not load values", outcome.valuesReader, is(false));
    }

    public void testMinMaxFromMetadata() {
        for (boolean columnar : new boolean[] { false, true }) {
            String index = "dense_" + columnar;
            createIndex(index, columnar);
            indexDense(index);
            assertMinMax(index, true);
            assertMinMax(index, false);
        }
    }

    /** With deleted documents the segment metadata is not used: the query is answered by the regular scan. */
    private void assertMinMaxByScan(String index, boolean date) {
        Outcome outcome = minMax(index, date, "");
        assertThat(
            index + (date ? " date" : " integer"),
            asLongs(outcome.row),
            equalTo(List.of(expected(date, true), expected(date, false)))
        );
        assertThat(index + " does not use the min/max source", outcome.minMaxSource, is(false));
    }

    /**
     * Segment bounds include deleted documents, so an index with deletions is not answered from metadata. Deleting or
     * updating the documents that hold the extremes must change the result, with or without a merge.
     */
    public void testDeletedAndUpdatedDocumentsAreIgnored() {
        for (boolean columnar : new boolean[] { false, true }) {
            String index = "deletes_" + columnar;
            createIndex(index, columnar);
            indexDense(index);
            assertMinMax(index, true);
            assertMinMax(index, false);

            // delete the documents holding both extremes
            client().prepareDelete(index, "doc_0").get();
            docs.remove("doc_0");
            int last = SEGMENTS * DOCS_PER_SEGMENT - 1;
            client().prepareDelete(index, "doc_" + last).get();
            docs.remove("doc_" + last);
            indicesAdmin().prepareRefresh(index).get();
            assertMinMaxByScan(index, true);
            assertMinMaxByScan(index, false);

            // the document holding the new minimum moves to the middle of the range: an update leaves its old version in a segment
            indexDoc(index, 1, dateOf(100) + 1, intOf(100) + 1);
            indicesAdmin().prepareRefresh(index).get();
            assertMinMaxByScan(index, true);
            assertMinMaxByScan(index, false);

            // after the merge the deleted documents may or may not still be in the segment (soft deletes are retained for a
            // while), either way the result must not change
            indicesAdmin().prepareForceMerge(index).setMaxNumSegments(1).get();
            indicesAdmin().prepareRefresh(index).get();
            for (boolean date : new boolean[] { true, false }) {
                Outcome outcome = minMax(index, date, "");
                assertThat(index, asLongs(outcome.row), equalTo(List.of(expected(date, true), expected(date, false))));
            }
        }
    }

    /**
     * A field alias returns the extrema of the field it points to. This only checks the result and that the min/max source is used;
     * that the metadata of the backing field is read is covered by {@code LuceneMinMaxFactoryTests}.
     */
    public void testFieldAlias() {
        String index = "alias_target";
        Settings.Builder settings = Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0);
        assertAcked(prepareCreate(index).setSettings(settings).setMapping("i", "type=integer", "i_alias", "type=alias,path=i"));
        docs.clear();
        for (int segment = 0; segment < SEGMENTS; segment++) {
            for (int doc = 0; doc < DOCS_PER_SEGMENT; doc++) {
                int n = segment * DOCS_PER_SEGMENT + doc;
                prepareIndex(index).setId("doc_" + n).setSource("i", intOf(n)).get();
                docs.put("doc_" + n, new long[] { 0, intOf(n) });
            }
            indicesAdmin().prepareFlush(index).get();
        }
        indicesAdmin().prepareRefresh(index).get();
        Outcome outcome = profiled("FROM " + index + " | STATS lo = MIN(i_alias), hi = MAX(i_alias)");
        assertThat(asLongs(outcome.row), equalTo(List.of(expected(false, true), expected(false, false))));
        assertThat("the alias uses the min/max source", outcome.minMaxSource, is(true));
    }

    /** A filter means the metadata does not describe the matching documents: the query is not answered by the min/max source. */
    public void testFilterIsNotAnsweredFromMetadata() {
        for (boolean columnar : new boolean[] { false, true }) {
            String index = "filtered_" + columnar;
            createIndex(index, columnar);
            indexDense(index);
            int threshold = intOf(100);

            Outcome outcome = minMax(index, true, " | WHERE i > " + threshold);
            long lowest = Long.MAX_VALUE;
            long highest = Long.MIN_VALUE;
            for (long[] doc : docs.values()) {
                if (doc[1] > threshold) {
                    lowest = Math.min(lowest, doc[0]);
                    highest = Math.max(highest, doc[0]);
                }
            }
            assertThat(asLongs(outcome.row), equalTo(List.of(lowest, highest)));
            assertThat(outcome.minMaxSource, is(false));
            assertThat(outcome.luceneSource, is(true));
        }
    }

    /** Documents without the field are ignored, even when whole segments hold none. */
    public void testMissingValues() {
        for (boolean columnar : new boolean[] { false, true }) {
            String index = "sparse_" + columnar;
            createIndex(index, columnar);
            docs.clear();
            // a segment without any value for the fields, then segments where only some documents have one
            for (int doc = 0; doc < DOCS_PER_SEGMENT; doc++) {
                prepareIndex(index).setId("empty_" + doc).setSource("other", doc).get();
            }
            indicesAdmin().prepareFlush(index).get();
            for (int segment = 1; segment < SEGMENTS; segment++) {
                for (int doc = 0; doc < DOCS_PER_SEGMENT; doc++) {
                    int n = segment * DOCS_PER_SEGMENT + doc;
                    if (doc % 3 == 0) {
                        prepareIndex(index).setId("doc_" + n).setSource("other", n).get();
                    } else {
                        indexDoc(index, n, dateOf(n), intOf(n));
                    }
                }
                indicesAdmin().prepareFlush(index).get();
            }
            indicesAdmin().prepareRefresh(index).get();

            for (boolean date : new boolean[] { true, false }) {
                Outcome outcome = minMax(index, date, "");
                assertThat(index, asLongs(outcome.row), equalTo(List.of(expected(date, true), expected(date, false))));
                // documents without a value must not turn the metadata path into a scan: MIN/MAX ignore them anyway
                assertThat(index + " uses the min/max source", outcome.minMaxSource, is(true));
                assertThat(index + " does not use the regular scan", outcome.luceneSource, is(false));
            }
        }
    }

    public void testNoDocumentHasAValue() {
        for (boolean columnar : new boolean[] { false, true }) {
            String index = "novalues_" + columnar;
            createIndex(index, columnar);
            for (int doc = 0; doc < 10; doc++) {
                prepareIndex(index).setId("empty_" + doc).setSource("other", doc).get();
            }
            indicesAdmin().prepareRefresh(index).get();
            for (boolean date : new boolean[] { true, false }) {
                Outcome outcome = minMax(index, date, "");
                assertThat(index, outcome.row.get(0), equalTo(null));
                assertThat(index, outcome.row.get(1), equalTo(null));
            }
        }
    }

    /** A multi-valued field takes the smallest and the largest of all values, whether or not the source answers it. */
    public void testMultiValuedField() {
        String index = "multivalued";
        createIndex(index, false);
        long lowest = Long.MAX_VALUE;
        long highest = Long.MIN_VALUE;
        for (int n = 0; n < 100; n++) {
            long a = dateOf(n);
            long b = dateOf(1000 - n);
            lowest = Math.min(lowest, Math.min(a, b));
            highest = Math.max(highest, Math.max(a, b));
            prepareIndex(index).setId("doc_" + n).setSource("d", List.of(a, b)).get();
            if (n % 25 == 24) {
                indicesAdmin().prepareFlush(index).get();
            }
        }
        indicesAdmin().prepareRefresh(index).get();

        Outcome outcome = minMax(index, true, "");
        assertThat(asLongs(outcome.row), equalTo(List.of(lowest, highest)));
        // not provably single valued, so it is not pushed and the values are loaded as before
        assertThat(outcome.minMaxSource, is(false));
    }

    /** Aggregates mixed with others, or over different fields, are not answered by the min/max source. */
    public void testUnsupportedShapesStillWork() {
        String index = "shapes";
        createIndex(index, false);
        indexDense(index);

        Outcome withCount = profiled("FROM " + index + " | STATS lo = MIN(i), hi = MAX(i), c = COUNT(*)");
        assertThat(asLongs(withCount.row), equalTo(List.of(expected(false, true), expected(false, false), (long) docs.size())));
        assertThat(withCount.minMaxSource, is(false));

        Outcome twoFields = profiled("FROM " + index + " | STATS lo = MIN(i), hi = MAX(d) | EVAL hi = TO_LONG(hi)");
        assertThat(asLongs(twoFields.row), equalTo(List.of(expected(false, true), expected(true, false))));
        assertThat(twoFields.minMaxSource, is(false));
    }
}
