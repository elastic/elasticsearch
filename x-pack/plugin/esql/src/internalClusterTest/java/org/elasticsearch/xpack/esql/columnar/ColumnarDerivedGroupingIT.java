/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.columnar;

import org.apache.lucene.tests.util.LuceneTestCase.SuppressCodecs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.PlanProfile;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;

/**
 * A {@code STATS BY} key derived from a field the mapping guarantees single-valued ({@code doc_values.multi_value=false}) is dropped
 * from the aggregation and recomputed above it. This runs the same data and query against an index with the guarantee and one
 * without, so the results are the oracle, and reads the plan to see that only the guaranteed index is rewritten.
 */
// The production per-field codec, so the fields are ColumNAR columns. ESIntegTestCase otherwise picks a codec that bypasses it.
@SuppressCodecs("*")
public class ColumnarDerivedGroupingIT extends AbstractEsqlIntegTestCase {

    private static final String QUERY = "FROM %s | EVAL next = x + 1, prev = -x | STATS c = COUNT(*), s = SUM(n) BY x, next, prev | SORT x";

    /** The index setting these tests create with is only registered while the feature flag is on, as a release build is not. */
    @BeforeClass
    public static void checkColumnarCodecEnabled() {
        assumeTrue("behind a feature flag", IndexSettings.BINARY_NUMERIC_DOC_VALUES_FEATURE_FLAG.isEnabled());
    }

    @Override
    protected Settings.Builder setRandomIndexSettings(Random random, Settings.Builder builder) {
        return super.setRandomIndexSettings(random, builder).remove(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey());
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    public void testSameResultsWithAndWithoutTheGuarantee() {
        createIndex("single", false);
        createIndex("multi", true);
        int docs = between(50, 300);
        for (int doc = 0; doc < docs; doc++) {
            // Some documents lack x: a null key must come out null for the derived keys too.
            for (String index : List.of("single", "multi")) {
                if (doc % 7 == 0) {
                    prepareIndex(index).setSource("n", doc).get();
                } else {
                    prepareIndex(index).setSource("x", doc % 9, "n", doc).get();
                }
            }
        }
        indicesAdmin().prepareRefresh("single", "multi").get();

        assertThat(rows("single"), equalTo(rows("multi")));
    }

    public void testOnlyTheGuaranteedIndexIsRewritten() {
        createIndex("single", false);
        createIndex("multi", true);
        for (String index : List.of("single", "multi")) {
            for (int doc = 0; doc < 20; doc++) {
                prepareIndex(index).setSource("x", doc % 4, "n", doc).get();
            }
        }
        indicesAdmin().prepareRefresh("single", "multi").get();

        // The data node's aggregation groups on x alone when the guarantee lets next and prev be recomputed after it.
        assertThat(plans("single"), matchesPattern("(?s).*AggregateExec\\[\\[x\\{f\\}#\\d+\\],.*"));
        assertThat(plans("multi"), matchesPattern("(?s).*AggregateExec\\[\\[x\\{f\\}#\\d+, next\\{r\\}#\\d+, prev\\{r\\}#\\d+\\],.*"));
    }

    private void createIndex(String index, boolean multiValue) {
        assertAcked(
            prepareCreate(index).setSettings(
                Settings.builder()
                    .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                    .put(FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.getKey(), multiValue)
                    .put(IndexSettings.BINARY_NUMERIC_DOC_VALUES_ENABLED_SETTING.getKey(), true)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
            ).setMapping("x", "type=long", "n", "type=long")
        );
    }

    private List<List<Object>> rows(String index) {
        try (EsqlQueryResponse response = run(String.format(Locale.ROOT, QUERY, index))) {
            List<List<Object>> rows = new ArrayList<>();
            response.rows().forEach(row -> {
                List<Object> copy = new ArrayList<>();
                row.forEach(copy::add);
                rows.add(copy);
            });
            return rows;
        }
    }

    private String plans(String index) {
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest(String.format(Locale.ROOT, QUERY, index));
        request.profile(true);
        request.pragmas(getPragmas());
        StringBuilder plans = new StringBuilder();
        try (EsqlQueryResponse response = run(request)) {
            for (PlanProfile plan : response.profile().plans()) {
                plans.append(plan.logicalPlanTree()).append("\n---\n").append(plan.planTree()).append("\n=====\n");
            }
        }
        return plans.toString();
    }
}
