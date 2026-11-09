/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.columnar;

import org.apache.lucene.tests.util.LuceneTestCase.SuppressCodecs;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.columnar.ColumnarDocValuesFormatSelector;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

/**
 * ES|QL over a {@code long} field the ColumNAR codec stores answers what it answers over the same documents in
 * {@code SORTED_NUMERIC} doc values: loading the values, filtering on them and aggregating them.
 */
// The production per-field codec, so the long is a ColumNAR column. ESIntegTestCase otherwise picks a codec that bypasses it.
@SuppressCodecs("*")
public class ColumnarLongEsqlIT extends AbstractEsqlIntegTestCase {

    private static final String WITH_CODEC = "columnar_long";
    private static final String WITHOUT_CODEC = "sorted_numeric_long";

    @Before
    public void requireFeatureFlag() {
        assumeTrue("behind a feature flag", ColumnarDocValuesFormatSelector.COLUMNAR_NUMERIC_CODEC_FEATURE_FLAG.isEnabled());
    }

    @Override
    protected Settings.Builder setRandomIndexSettings(Random random, Settings.Builder builder) {
        return super.setRandomIndexSettings(random, builder).remove(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey());
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    public void testAgreesWithSortedNumeric() {
        final IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        final String mapping = "{\"properties\":{\"@timestamp\":{\"type\":\"date\"},\"id\":{\"type\":\"keyword\"},"
            + "\"k\":{\"type\":\"keyword\"},\"l\":{\"type\":\"long\",\"index\":"
            + randomBoolean()
            + "}}}";
        for (String index : List.of(WITH_CODEC, WITHOUT_CODEC)) {
            assertAcked(
                prepareCreate(index).setSettings(
                    Settings.builder()
                        .put(IndexSettings.MODE.getKey(), mode.getName())
                        .put(IndexSettings.COLUMNAR_NUMERIC_CODEC_ENABLED_SETTING.getKey(), index.equals(WITH_CODEC))
                        .put("index.number_of_shards", between(1, 3))
                        .put("index.number_of_replicas", 0)
                ).setMapping(mapping)
            );
        }

        final boolean singleValued = randomBoolean();
        final int numDocs = between(1, 500);
        for (int from = 0; from < numDocs;) {
            final int to = Math.min(numDocs, from + between(1, 200));
            final BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int d = from; d < to; d++) {
                final String source = source(d, singleValued);
                for (String index : List.of(WITH_CODEC, WITHOUT_CODEC)) {
                    bulk.add(prepareIndex(index).setId(Integer.toString(d)).setSource(source, XContentType.JSON));
                }
            }
            assertNoFailures(bulk.get());
            from = to;
        }
        if (randomBoolean()) {
            indicesAdmin().prepareForceMerge(WITH_CODEC, WITHOUT_CODEC).setMaxNumSegments(1).get();
            indicesAdmin().prepareRefresh(WITH_CODEC, WITHOUT_CODEC).get();
        }

        final long lower = randomLongBetween(-60, 20);
        final long upper = randomLongBetween(lower, 60);
        assertSameResults(String.format(Locale.ROOT, """
            | WHERE l >= %d AND l < %d
            | STATS s = SUM(l), c = COUNT(l), mn = MIN(l), mx = MAX(l) BY k
            | SORT k
            | LIMIT 100""", lower, upper));
        assertSameResults("""
            | STATS s = SUM(l), c = COUNT(l), mn = MIN(l), mx = MAX(l), n = COUNT(*) BY k
            | SORT k
            | LIMIT 100""");
        assertSameResults("""
            | KEEP id, l
            | SORT id
            | LIMIT 1000""");
        // Multi-value functions take shortcuts on blocks that say their values are sorted, so these catch a block of values in
        // array order that claims to be sorted.
        assertSameResults("""
            | EVAL mn = MV_MIN(l), mx = MV_MAX(l), sorted = MV_SORT(l)
            | KEEP id, mn, mx, sorted
            | SORT id
            | LIMIT 1000""");
        assertSameResults("""
            | EVAL mn = MV_MIN(l), mx = MV_MAX(l)
            | STATS smn = SUM(mn), smx = SUM(mx) BY k
            | SORT k
            | LIMIT 100""");
    }

    private static String source(int doc, boolean singleValued) {
        final StringBuilder source = new StringBuilder("{\"@timestamp\":\"2024-01-01T00:00:00Z\",\"id\":\"").append(
            String.format(Locale.ROOT, "%05d", doc)
        ).append("\",\"k\":\"k").append(doc % 7).append('"');
        if (rarely()) {
            return source.append('}').toString();
        }
        source.append(",\"l\":");
        if (singleValued) {
            source.append(randomLongBetween(-100, 100));
        } else {
            final int count = between(1, 4);
            final List<Long> values = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                values.add(randomLongBetween(-100, 100));
            }
            source.append(values);
        }
        return source.append('}').toString();
    }

    private void assertSameResults(String query) {
        assertEquals(query, rows(WITHOUT_CODEC, query), rows(WITH_CODEC, query));
    }

    private List<List<Object>> rows(String index, String query) {
        final List<List<Object>> rows = new ArrayList<>();
        try (EsqlQueryResponse response = run("FROM " + index + "\n" + query)) {
            for (Iterator<Iterator<Object>> it = response.values(); it.hasNext();) {
                final List<Object> row = new ArrayList<>();
                it.next().forEachRemaining(row::add);
                rows.add(row);
            }
        }
        return rows;
    }
}
