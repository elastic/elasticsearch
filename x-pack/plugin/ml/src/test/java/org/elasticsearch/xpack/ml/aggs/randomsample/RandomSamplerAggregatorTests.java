/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.randomsample;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class RandomSamplerAggregatorTests extends AggregatorTestCase {

    private static final String NUMERIC_FIELD_NAME = "value";

    @Override
    protected AnalysisModule createAnalysisModule() throws Exception {
        return new AnalysisModule(
            TestEnvironment.newEnvironment(
                Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build()
            ),
            List.of(new MachineLearning(Settings.EMPTY))
        );
    }

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new MachineLearning(Settings.EMPTY));
    }

    public void testAggregationSampling() throws IOException {
        testCase(
            new RandomSamplerAggregationBuilder("my_agg").subAggregation(AggregationBuilders.avg("avg").field(NUMERIC_FIELD_NAME))
                .setProbability(0.25),
            new MatchAllDocsQuery(),
            RandomSamplerAggregatorTests::writeTestDocs,
            (InternalRandomSampler result) -> {
                assertThat(result.getDocCount(), allOf(greaterThan(20L), lessThan(60L)));
                Avg agg = result.getAggregations().get("avg");
                assertThat(agg.getValue(), closeTo(2, 1));
            },
            longField(NUMERIC_FIELD_NAME)
        );
    }

    private static void writeTestDocs(RandomIndexWriter w) throws IOException {
        for (int i = 0; i < 50; i++) {
            w.addDocument(List.of(new SortedNumericDocValuesField(NUMERIC_FIELD_NAME, 1)));
        }
        for (int i = 0; i < 50; i++) {
            w.addDocument(List.of(new SortedNumericDocValuesField(NUMERIC_FIELD_NAME, 2)));
        }
        for (int i = 0; i < 25; i++) {
            w.addDocument(List.of(new SortedNumericDocValuesField(NUMERIC_FIELD_NAME, 4)));
        }
    }

}
