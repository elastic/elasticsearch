/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.tests.util.LuceneTestCase.SuppressCodecs;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

/**
 * Counts by a {@code keyword} field of a columnar index. Such a field has no terms in the index unless it asks for them, and
 * keeps the nulls of an array in its doc values: a document that holds only nulls has doc values and no value.
 */
// The production per-field codec, so the keywords are ColumNAR columns. ESIntegTestCase otherwise picks a codec that bypasses it.
@SuppressCodecs("*")
public class CountByKeywordColumnarIT extends AbstractEsqlIntegTestCase {

    private static final String INDEX = "columnar_keywords";
    private static final String[] VALUES = { "a", "b", "bb", "c d", "E", "é" };
    private static final String[] FIELDS = { "k", "indexed", "indexed_low", "indexed_single" };

    /** The number of documents of every value, {@code null} for the documents with no value. */
    private final Map<String, Long> expected = new HashMap<>();
    /** The same for the field that holds a single value, which is the first value of a document. */
    private final Map<String, Long> expectedSingle = new HashMap<>();

    @Override
    protected Settings.Builder setRandomIndexSettings(Random random, Settings.Builder builder) {
        return super.setRandomIndexSettings(random, builder).remove(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey());
    }

    @Before
    public void populate() {
        IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        String mapping = """
            {"properties":{
              "@timestamp":{"type":"date"},
              "k":{"type":"keyword"},
              "indexed":{"type":"keyword","index":true},
              "indexed_low":{"type":"keyword","index":true,"doc_values":{"cardinality":"low"}},
              "indexed_single":{"type":"keyword","index":true,"doc_values":{"multi_value":false}},
              "n":{"type":"integer"}
            }}""";
        assertAcked(
            prepareCreate(INDEX).setSettings(
                Settings.builder()
                    .put(IndexSettings.MODE.getKey(), mode.getName())
                    .put("index.number_of_shards", between(1, 3))
                    .put("index.number_of_replicas", 0)
            ).setMapping(mapping)
        );
        int docs = between(50, 500);
        for (int from = 0; from < docs;) {
            // Many requests, for many segments: the layout of a column is chosen for each of them
            int to = Math.min(docs, from + between(1, 200));
            BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int i = from; i < to; i++) {
                bulk.add(prepareIndex(INDEX).setId(Integer.toString(i)).setSource(source(i), XContentType.JSON));
            }
            assertNoFailures(bulk.get());
            from = to;
        }
        if (randomBoolean()) {
            indicesAdmin().prepareForceMerge(INDEX).setMaxNumSegments(1).get();
            indicesAdmin().prepareRefresh(INDEX).get();
        }
    }

    /**
     * A document with no field, with a null, with an array of nulls only, or with values among which there may be nulls.
     */
    private String source(int id) {
        StringBuilder source = new StringBuilder("{\"@timestamp\":").append(1_700_000_000_000L + id).append(",\"n\":").append(id);
        String array;
        String single;
        List<String> values = List.of();
        switch (between(0, 4)) {
            case 0 -> {
                array = null;
                single = null;
            }
            case 1 -> {
                array = "null";
                single = "null";
            }
            case 2 -> {
                array = randomBoolean() ? "[null]" : "[null,null]";
                single = "null";
            }
            case 3 -> {
                array = "[]";
                single = null;
            }
            default -> {
                values = randomSubsetOf(between(1, 3), VALUES);
                List<String> slots = new ArrayList<>();
                for (String value : values) {
                    if (randomInt(3) == 0) {
                        slots.add("null");
                    }
                    slots.add("\"" + value + "\"");
                }
                if (randomInt(3) == 0) {
                    slots.add("null");
                }
                array = slots.size() == 1 ? slots.getFirst() : slots.toString();
                single = "\"" + values.getFirst() + "\"";
            }
        }
        if (values.isEmpty()) {
            expected.merge(null, 1L, Long::sum);
            expectedSingle.merge(null, 1L, Long::sum);
        } else {
            for (String value : values) {
                expected.merge(value, 1L, Long::sum);
            }
            expectedSingle.merge(values.getFirst(), 1L, Long::sum);
        }
        for (String field : FIELDS) {
            String value = field.equals("indexed_single") ? single : array;
            if (value != null) {
                source.append(",\"").append(field).append("\":").append(value);
            }
        }
        return source.append("}").toString();
    }

    /**
     * A field with no terms in the index, which is what a columnar index maps unless told otherwise: its values are not listed.
     */
    public void testNotIndexed() {
        assertThat(count("FROM " + INDEX + " | STATS c = COUNT(*) BY k", false), equalTo(expected));
    }

    public void testIndexed() {
        assertThat(count("FROM " + INDEX + " | STATS c = COUNT(*) BY indexed", true), equalTo(expected));
    }

    public void testIndexedLowCardinality() {
        assertThat(count("FROM " + INDEX + " | STATS c = COUNT(*) BY indexed_low", true), equalTo(expected));
    }

    public void testIndexedSingleValue() {
        assertThat(count("FROM " + INDEX + " | STATS c = COUNT(*) BY indexed_single", true), equalTo(expectedSingle));
    }

    /**
     * Runs a query that returns a count and a key, and checks whether the count is pushed down to Lucene. Then checks that
     * the counts are the same when it is not, and returns them by key.
     */
    private Map<String, Long> count(String query, boolean pushedDown) {
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query(query);
        request.profile(true);
        Map<String, Long> counts = new HashMap<>();
        try (EsqlQueryResponse response = run(request)) {
            response.values().forEachRemaining(row -> {
                Long count = (Long) row.next();
                assertNull(counts.put((String) row.next(), count));
            });
            List<String> operators = new ArrayList<>();
            boolean countsInLucene = false;
            for (DriverProfile driver : response.profile().drivers()) {
                if (driver.description().equals(ComputeService.DATA_DESCRIPTION)) {
                    for (OperatorStatus operator : driver.operators()) {
                        operators.add(operator.operator());
                        countsInLucene |= operator.operator().startsWith("LuceneCountOperator");
                    }
                }
            }
            assertThat(operators.toString(), countsInLucene, equalTo(pushedDown));
        }
        EsqlQueryRequest withoutPushDown = new EsqlQueryRequest();
        withoutPushDown.query(query);
        withoutPushDown.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.ROUNDTO_PUSHDOWN_THRESHOLD.getKey(), 0).build()));
        withoutPushDown.acceptedPragmaRisks(true);
        Map<String, Long> countsWithoutPushDown = new HashMap<>();
        try (EsqlQueryResponse response = run(withoutPushDown)) {
            response.values().forEachRemaining(row -> {
                Long count = (Long) row.next();
                countsWithoutPushDown.put((String) row.next(), count);
            });
        }
        assertThat(counts, equalTo(countsWithoutPushDown));
        return counts;
    }
}
