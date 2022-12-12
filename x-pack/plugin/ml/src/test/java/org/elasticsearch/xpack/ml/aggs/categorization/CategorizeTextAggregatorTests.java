/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class CategorizeTextAggregatorTests extends AggregatorTestCase {

    @Override
    protected AnalysisModule createAnalysisModule() throws Exception {
        return new AnalysisModule(
            TestEnvironment.newEnvironment(
                Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build()
            ),
            List.of(new MachineLearning(Settings.EMPTY), new CommonAnalysisPlugin()),
            new StablePluginsRegistry()
        );
    }

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new MachineLearning(Settings.EMPTY));
    }

    private static final String TEXT_FIELD_NAME = "text";
    private static final String NUMERIC_FIELD_NAME = "value";

    public void testCategorizationWithoutSubAggs() throws Exception {
        testCase(CategorizeTextAggregatorTests::writeTestDocs, (InternalCategorizationAggregation result) -> {
            assertThat(result.getBuckets(), hasSize(2));
            assertThat(result.getBuckets().get(0).getDocCount(), equalTo(6L));
            assertThat(result.getBuckets().get(0).getKeyAsString(), equalTo("Node started"));
            assertThat(result.getBuckets().get(0).getSerializableCategory().maxMatchingStringLen(), equalTo(15));
            assertThat(result.getBuckets().get(0).getSerializableCategory().getRegex(), equalTo(".*?Node.+?started.*?"));
            assertThat(result.getBuckets().get(1).getDocCount(), equalTo(2L));
            assertThat(
                result.getBuckets().get(1).getKeyAsString(),
                equalTo("Failed to shutdown error org.aaaa.bbbb.Cccc line caused by foo exception")
            );
            assertThat(result.getBuckets().get(1).getSerializableCategory().maxMatchingStringLen(), equalTo(84));
            assertThat(
                result.getBuckets().get(1).getSerializableCategory().getRegex(),
                equalTo(".*?Failed.+?to.+?shutdown.+?error.+?org\\.aaaa\\.bbbb\\.Cccc.+?line.+?caused.+?by.+?foo.+?exception.*?")
            );
        },
            new AggTestConfig(
                new CategorizeTextAggregationBuilder("my_agg", TEXT_FIELD_NAME),
                new TextFieldMapper.TextFieldType(TEXT_FIELD_NAME, randomBoolean()),
                longField(NUMERIC_FIELD_NAME)
            )
        );
    }

    public void testCategorizationWithSubAggs() throws Exception {
        CategorizeTextAggregationBuilder aggBuilder = new CategorizeTextAggregationBuilder("my_agg", TEXT_FIELD_NAME).subAggregation(
            new MaxAggregationBuilder("max").field(NUMERIC_FIELD_NAME)
        )
            .subAggregation(new AvgAggregationBuilder("avg").field(NUMERIC_FIELD_NAME))
            .subAggregation(new MinAggregationBuilder("min").field(NUMERIC_FIELD_NAME));
        testCase(CategorizeTextAggregatorTests::writeTestDocs, (InternalCategorizationAggregation result) -> {
            assertThat(result.getBuckets(), hasSize(2));
            assertThat(result.getBuckets().get(0).getDocCount(), equalTo(6L));
            assertThat(result.getBuckets().get(0).getKeyAsString(), equalTo("Node started"));
            assertThat(result.getBuckets().get(0).getSerializableCategory().maxMatchingStringLen(), equalTo(15));
            assertThat(result.getBuckets().get(0).getSerializableCategory().getRegex(), equalTo(".*?Node.+?started.*?"));
            assertThat(((Max) result.getBuckets().get(0).getAggregations().get("max")).value(), equalTo(5.0));
            assertThat(((Min) result.getBuckets().get(0).getAggregations().get("min")).value(), equalTo(0.0));
            assertThat(((Avg) result.getBuckets().get(0).getAggregations().get("avg")).getValue(), equalTo(2.5));

            assertThat(result.getBuckets().get(1).getDocCount(), equalTo(2L));
            assertThat(
                result.getBuckets().get(1).getKeyAsString(),
                equalTo("Failed to shutdown error org.aaaa.bbbb.Cccc line caused by foo exception")
            );
            assertThat(result.getBuckets().get(1).getSerializableCategory().maxMatchingStringLen(), equalTo(84));
            assertThat(
                result.getBuckets().get(1).getSerializableCategory().getRegex(),
                equalTo(".*?Failed.+?to.+?shutdown.+?error.+?org\\.aaaa\\.bbbb\\.Cccc.+?line.+?caused.+?by.+?foo.+?exception.*?")
            );
            assertThat(((Max) result.getBuckets().get(1).getAggregations().get("max")).value(), equalTo(4.0));
            assertThat(((Min) result.getBuckets().get(1).getAggregations().get("min")).value(), equalTo(0.0));
            assertThat(((Avg) result.getBuckets().get(1).getAggregations().get("avg")).getValue(), equalTo(2.0));
        },
            new AggTestConfig(
                aggBuilder,
                new TextFieldMapper.TextFieldType(TEXT_FIELD_NAME, randomBoolean()),
                longField(NUMERIC_FIELD_NAME)
            )
        );
    }

    public void testCategorizationWithMultiBucketSubAggs() throws Exception {
        CategorizeTextAggregationBuilder aggBuilder = new CategorizeTextAggregationBuilder("my_agg", TEXT_FIELD_NAME).subAggregation(
            new HistogramAggregationBuilder("histo").field(NUMERIC_FIELD_NAME)
                .interval(2)
                .subAggregation(new MaxAggregationBuilder("max").field(NUMERIC_FIELD_NAME))
                .subAggregation(new AvgAggregationBuilder("avg").field(NUMERIC_FIELD_NAME))
                .subAggregation(new MinAggregationBuilder("min").field(NUMERIC_FIELD_NAME))
        );
        testCase(CategorizeTextAggregatorTests::writeTestDocs, (InternalCategorizationAggregation result) -> {
            assertThat(result.getBuckets(), hasSize(2));
            assertThat(result.getBuckets().get(0).getDocCount(), equalTo(6L));
            assertThat(result.getBuckets().get(0).getKeyAsString(), equalTo("Node started"));
            assertThat(result.getBuckets().get(0).getSerializableCategory().maxMatchingStringLen(), equalTo(15));
            assertThat(result.getBuckets().get(0).getSerializableCategory().getRegex(), equalTo(".*?Node.+?started.*?"));
            Histogram histo = result.getBuckets().get(0).getAggregations().get("histo");
            assertThat(histo.getBuckets(), hasSize(3));
            for (Histogram.Bucket bucket : histo.getBuckets()) {
                assertThat(bucket.getDocCount(), equalTo(2L));
            }
            assertThat(((Max) histo.getBuckets().get(0).getAggregations().get("max")).value(), equalTo(1.0));
            assertThat(((Min) histo.getBuckets().get(0).getAggregations().get("min")).value(), equalTo(0.0));
            assertThat(((Avg) histo.getBuckets().get(0).getAggregations().get("avg")).getValue(), equalTo(0.5));
            assertThat(((Max) histo.getBuckets().get(1).getAggregations().get("max")).value(), equalTo(3.0));
            assertThat(((Min) histo.getBuckets().get(1).getAggregations().get("min")).value(), equalTo(2.0));
            assertThat(((Avg) histo.getBuckets().get(1).getAggregations().get("avg")).getValue(), equalTo(2.5));
            assertThat(((Max) histo.getBuckets().get(2).getAggregations().get("max")).value(), equalTo(5.0));
            assertThat(((Min) histo.getBuckets().get(2).getAggregations().get("min")).value(), equalTo(4.0));
            assertThat(((Avg) histo.getBuckets().get(2).getAggregations().get("avg")).getValue(), equalTo(4.5));

            assertThat(result.getBuckets().get(1).getDocCount(), equalTo(2L));
            assertThat(
                result.getBuckets().get(1).getKeyAsString(),
                equalTo("Failed to shutdown error org.aaaa.bbbb.Cccc line caused by foo exception")
            );
            assertThat(result.getBuckets().get(1).getSerializableCategory().maxMatchingStringLen(), equalTo(84));
            assertThat(
                result.getBuckets().get(1).getSerializableCategory().getRegex(),
                equalTo(".*?Failed.+?to.+?shutdown.+?error.+?org\\.aaaa\\.bbbb\\.Cccc.+?line.+?caused.+?by.+?foo.+?exception.*?")
            );
            histo = result.getBuckets().get(1).getAggregations().get("histo");
            assertThat(histo.getBuckets(), hasSize(3));
            assertThat(histo.getBuckets().get(0).getDocCount(), equalTo(1L));
            assertThat(histo.getBuckets().get(1).getDocCount(), equalTo(0L));
            assertThat(histo.getBuckets().get(2).getDocCount(), equalTo(1L));
            assertThat(((Avg) histo.getBuckets().get(0).getAggregations().get("avg")).getValue(), equalTo(0.0));
            assertThat(((Avg) histo.getBuckets().get(2).getAggregations().get("avg")).getValue(), equalTo(4.0));
        },
            new AggTestConfig(
                aggBuilder,
                new TextFieldMapper.TextFieldType(TEXT_FIELD_NAME, randomBoolean()),
                longField(NUMERIC_FIELD_NAME)
            )
        );
    }

    public void testCategorizationAsSubAgg() throws Exception {
        HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("histo").field(NUMERIC_FIELD_NAME)
            .interval(2)
            .subAggregation(
                new CategorizeTextAggregationBuilder("my_agg", TEXT_FIELD_NAME).subAggregation(
                    new MaxAggregationBuilder("max").field(NUMERIC_FIELD_NAME)
                )
                    .subAggregation(new AvgAggregationBuilder("avg").field(NUMERIC_FIELD_NAME))
                    .subAggregation(new MinAggregationBuilder("min").field(NUMERIC_FIELD_NAME))
            );
        // First histo bucket
        // Second histo bucket
        // Third histo bucket
        testCase(CategorizeTextAggregatorTests::writeTestDocs, (InternalHistogram result) -> {
            assertThat(result.getBuckets(), hasSize(3));

            // First histo bucket
            assertThat(result.getBuckets().get(0).getDocCount(), equalTo(3L));
            InternalCategorizationAggregation categorizationAggregation = result.getBuckets().get(0).getAggregations().get("my_agg");
            assertThat(categorizationAggregation.getBuckets(), hasSize(2));
            assertThat(categorizationAggregation.getBuckets().get(0).getDocCount(), equalTo(2L));
            assertThat(categorizationAggregation.getBuckets().get(0).getKeyAsString(), equalTo("Node started"));
            assertThat(categorizationAggregation.getBuckets().get(0).getSerializableCategory().maxMatchingStringLen(), equalTo(15));
            assertThat(categorizationAggregation.getBuckets().get(0).getSerializableCategory().getRegex(), equalTo(".*?Node.+?started.*?"));
            assertThat(((Max) categorizationAggregation.getBuckets().get(0).getAggregations().get("max")).value(), equalTo(1.0));
            assertThat(((Min) categorizationAggregation.getBuckets().get(0).getAggregations().get("min")).value(), equalTo(0.0));
            assertThat(((Avg) categorizationAggregation.getBuckets().get(0).getAggregations().get("avg")).getValue(), equalTo(0.5));

            assertThat(categorizationAggregation.getBuckets().get(1).getDocCount(), equalTo(1L));
            assertThat(
                categorizationAggregation.getBuckets().get(1).getKeyAsString(),
                equalTo("Failed to shutdown error org.aaaa.bbbb.Cccc line caused by foo exception")
            );
            assertThat(categorizationAggregation.getBuckets().get(1).getSerializableCategory().maxMatchingStringLen(), equalTo(84));
            assertThat(
                categorizationAggregation.getBuckets().get(1).getSerializableCategory().getRegex(),
                equalTo(".*?Failed.+?to.+?shutdown.+?error.+?org\\.aaaa\\.bbbb\\.Cccc.+?line.+?caused.+?by.+?foo.+?exception.*?")
            );
            assertThat(((Max) categorizationAggregation.getBuckets().get(1).getAggregations().get("max")).value(), equalTo(0.0));
            assertThat(((Min) categorizationAggregation.getBuckets().get(1).getAggregations().get("min")).value(), equalTo(0.0));
            assertThat(((Avg) categorizationAggregation.getBuckets().get(1).getAggregations().get("avg")).getValue(), equalTo(0.0));

            // Second histo bucket
            assertThat(result.getBuckets().get(1).getDocCount(), equalTo(2L));
            categorizationAggregation = result.getBuckets().get(1).getAggregations().get("my_agg");
            assertThat(categorizationAggregation.getBuckets(), hasSize(1));
            assertThat(categorizationAggregation.getBuckets().get(0).getDocCount(), equalTo(2L));
            assertThat(categorizationAggregation.getBuckets().get(0).getKeyAsString(), equalTo("Node started"));
            assertThat(categorizationAggregation.getBuckets().get(0).getSerializableCategory().maxMatchingStringLen(), equalTo(15));
            assertThat(categorizationAggregation.getBuckets().get(0).getSerializableCategory().getRegex(), equalTo(".*?Node.+?started.*?"));
            assertThat(((Max) categorizationAggregation.getBuckets().get(0).getAggregations().get("max")).value(), equalTo(3.0));
            assertThat(((Min) categorizationAggregation.getBuckets().get(0).getAggregations().get("min")).value(), equalTo(2.0));
            assertThat(((Avg) categorizationAggregation.getBuckets().get(0).getAggregations().get("avg")).getValue(), equalTo(2.5));

            // Third histo bucket
            assertThat(result.getBuckets().get(2).getDocCount(), equalTo(3L));
            categorizationAggregation = result.getBuckets().get(2).getAggregations().get("my_agg");
            assertThat(categorizationAggregation.getBuckets(), hasSize(2));
            assertThat(categorizationAggregation.getBuckets().get(0).getDocCount(), equalTo(2L));
            assertThat(categorizationAggregation.getBuckets().get(0).getKeyAsString(), equalTo("Node started"));
            assertThat(categorizationAggregation.getBuckets().get(0).getSerializableCategory().maxMatchingStringLen(), equalTo(15));
            assertThat(categorizationAggregation.getBuckets().get(0).getSerializableCategory().getRegex(), equalTo(".*?Node.+?started.*?"));
            assertThat(((Max) categorizationAggregation.getBuckets().get(0).getAggregations().get("max")).value(), equalTo(5.0));
            assertThat(((Min) categorizationAggregation.getBuckets().get(0).getAggregations().get("min")).value(), equalTo(4.0));
            assertThat(((Avg) categorizationAggregation.getBuckets().get(0).getAggregations().get("avg")).getValue(), equalTo(4.5));

            assertThat(categorizationAggregation.getBuckets().get(1).getDocCount(), equalTo(1L));
            assertThat(
                categorizationAggregation.getBuckets().get(1).getKeyAsString(),
                equalTo("Failed to shutdown error org.aaaa.bbbb.Cccc line caused by foo exception")
            );
            assertThat(categorizationAggregation.getBuckets().get(1).getSerializableCategory().maxMatchingStringLen(), equalTo(84));
            assertThat(
                categorizationAggregation.getBuckets().get(1).getSerializableCategory().getRegex(),
                equalTo(".*?Failed.+?to.+?shutdown.+?error.+?org\\.aaaa\\.bbbb\\.Cccc.+?line.+?caused.+?by.+?foo.+?exception.*?")
            );
            assertThat(((Max) categorizationAggregation.getBuckets().get(1).getAggregations().get("max")).value(), equalTo(4.0));
            assertThat(((Min) categorizationAggregation.getBuckets().get(1).getAggregations().get("min")).value(), equalTo(4.0));
            assertThat(((Avg) categorizationAggregation.getBuckets().get(1).getAggregations().get("avg")).getValue(), equalTo(4.0));
        },
            new AggTestConfig(
                aggBuilder,
                new TextFieldMapper.TextFieldType(TEXT_FIELD_NAME, randomBoolean()),
                longField(NUMERIC_FIELD_NAME)
            )
        );
    }

    public void testCategorizationWithSubAggsManyDocs() throws Exception {
        CategorizeTextAggregationBuilder aggBuilder = new CategorizeTextAggregationBuilder("my_agg", TEXT_FIELD_NAME).subAggregation(
            new HistogramAggregationBuilder("histo").field(NUMERIC_FIELD_NAME)
                .interval(2)
                .subAggregation(new MaxAggregationBuilder("max").field(NUMERIC_FIELD_NAME))
                .subAggregation(new AvgAggregationBuilder("avg").field(NUMERIC_FIELD_NAME))
                .subAggregation(new MinAggregationBuilder("min").field(NUMERIC_FIELD_NAME))
        );
        testCase(CategorizeTextAggregatorTests::writeManyTestDocs, (InternalCategorizationAggregation result) -> {
            assertThat(result.getBuckets(), hasSize(2));
            assertThat(result.getBuckets().get(0).getDocCount(), equalTo(30000L));
            assertThat(result.getBuckets().get(0).getKeyAsString(), equalTo("Node started"));
            assertThat(result.getBuckets().get(0).getSerializableCategory().maxMatchingStringLen(), equalTo(15));
            assertThat(result.getBuckets().get(0).getSerializableCategory().getRegex(), equalTo(".*?Node.+?started.*?"));
            Histogram histo = result.getBuckets().get(0).getAggregations().get("histo");
            assertThat(histo.getBuckets(), hasSize(3));
            for (Histogram.Bucket bucket : histo.getBuckets()) {
                assertThat(bucket.getDocCount(), equalTo(10000L));
            }
            assertThat(((Max) histo.getBuckets().get(0).getAggregations().get("max")).value(), equalTo(1.0));
            assertThat(((Min) histo.getBuckets().get(0).getAggregations().get("min")).value(), equalTo(0.0));
            assertThat(((Avg) histo.getBuckets().get(0).getAggregations().get("avg")).getValue(), equalTo(0.5));
            assertThat(((Max) histo.getBuckets().get(1).getAggregations().get("max")).value(), equalTo(3.0));
            assertThat(((Min) histo.getBuckets().get(1).getAggregations().get("min")).value(), equalTo(2.0));
            assertThat(((Avg) histo.getBuckets().get(1).getAggregations().get("avg")).getValue(), equalTo(2.5));
            assertThat(((Max) histo.getBuckets().get(2).getAggregations().get("max")).value(), equalTo(5.0));
            assertThat(((Min) histo.getBuckets().get(2).getAggregations().get("min")).value(), equalTo(4.0));
            assertThat(((Avg) histo.getBuckets().get(2).getAggregations().get("avg")).getValue(), equalTo(4.5));

            assertThat(result.getBuckets().get(1).getDocCount(), equalTo(10000L));
            assertThat(
                result.getBuckets().get(1).getKeyAsString(),
                equalTo("Failed to shutdown error org.aaaa.bbbb.Cccc line caused by foo exception")
            );
            assertThat(result.getBuckets().get(1).getSerializableCategory().maxMatchingStringLen(), equalTo(84));
            assertThat(
                result.getBuckets().get(1).getSerializableCategory().getRegex(),
                equalTo(".*?Failed.+?to.+?shutdown.+?error.+?org\\.aaaa\\.bbbb\\.Cccc.+?line.+?caused.+?by.+?foo.+?exception.*?")
            );
            histo = result.getBuckets().get(1).getAggregations().get("histo");
            assertThat(histo.getBuckets(), hasSize(3));
            assertThat(histo.getBuckets().get(0).getDocCount(), equalTo(5000L));
            assertThat(histo.getBuckets().get(1).getDocCount(), equalTo(0L));
            assertThat(histo.getBuckets().get(2).getDocCount(), equalTo(5000L));
            assertThat(((Avg) histo.getBuckets().get(0).getAggregations().get("avg")).getValue(), equalTo(0.0));
            assertThat(((Avg) histo.getBuckets().get(2).getAggregations().get("avg")).getValue(), equalTo(4.0));
        },
            new AggTestConfig(
                aggBuilder,
                new TextFieldMapper.TextFieldType(TEXT_FIELD_NAME, randomBoolean()),
                longField(NUMERIC_FIELD_NAME)
            )
        );
    }

    private static void writeTestDocs(RandomIndexWriter w) throws IOException {
        w.addDocument(
            Arrays.asList(
                new StoredField("_source", new BytesRef("{\"text\":\"Node 1 started\"}")),
                new SortedNumericDocValuesField(NUMERIC_FIELD_NAME, 0)
            )
        );
        w.addDocument(
            Arrays.asList(
                new StoredField("_source", new BytesRef("{\"text\":\"Node 1 started\"}")),
                new SortedNumericDocValuesField(NUMERIC_FIELD_NAME, 1)
            )
        );
        w.addDocument(
            Arrays.asList(
                new StoredField(
                    "_source",
                    new BytesRef("{\"text\":\"Failed to shutdown [error org.aaaa.bbbb.Cccc line 54 caused by foo exception]\"}")
                ),
                new SortedNumericDocValuesField(NUMERIC_FIELD_NAME, 0)
            )
        );
        w.addDocument(
            Arrays.asList(
                new StoredField(
                    "_source",
                    new BytesRef("{\"text\":\"Failed to shutdown [error org.aaaa.bbbb.Cccc line 54 caused by foo exception]\"}")
                ),
                new SortedNumericDocValuesField(NUMERIC_FIELD_NAME, 4)
            )
        );
        w.addDocument(
            Arrays.asList(
                new StoredField("_source", new BytesRef("{\"text\":\"Node 2 started\"}")),
                new SortedNumericDocValuesField(NUMERIC_FIELD_NAME, 2)
            )
        );
        w.addDocument(
            Arrays.asList(
                new StoredField("_source", new BytesRef("{\"text\":\"Node 2 started\"}")),
                new SortedNumericDocValuesField(NUMERIC_FIELD_NAME, 3)
            )
        );
        w.addDocument(
            Arrays.asList(
                new StoredField("_source", new BytesRef("{\"text\":\"Node 3 started\"}")),
                new SortedNumericDocValuesField(NUMERIC_FIELD_NAME, 4)
            )
        );
        w.addDocument(
            Arrays.asList(
                new StoredField("_source", new BytesRef("{\"text\":\"Node 3 started\"}")),
                new SortedNumericDocValuesField(NUMERIC_FIELD_NAME, 5)
            )
        );
    }

    private static void writeManyTestDocs(RandomIndexWriter w) throws IOException {
        for (int i = 0; i < 5000; i++) {
            writeTestDocs(w);
        }
    }
}
