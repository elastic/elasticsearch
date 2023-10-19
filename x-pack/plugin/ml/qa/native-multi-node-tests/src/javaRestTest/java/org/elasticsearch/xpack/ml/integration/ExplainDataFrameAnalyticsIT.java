/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.core.ml.action.ExplainDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.BoostedTreeParams;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Classification;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.OutlierDetection;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression;
import org.elasticsearch.xpack.core.ml.dataframe.explain.FieldSelection;
import org.elasticsearch.xpack.core.ml.utils.QueryProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ExplainDataFrameAnalyticsIT extends MlNativeDataFrameAnalyticsIntegTestCase {

    public void testExplain_GivenMissingSourceIndex() {
        DataFrameAnalyticsConfig config = new DataFrameAnalyticsConfig.Builder().setSource(
            new DataFrameAnalyticsSource(new String[] { "missing_index" }, null, null, Collections.emptyMap())
        ).setAnalysis(new OutlierDetection.Builder().build()).buildForExplain();

        ResourceNotFoundException e = expectThrows(ResourceNotFoundException.class, () -> explainDataFrame(config));
        assertThat(e.getMessage(), equalTo("cannot retrieve data because index [missing_index] does not exist"));
    }

    public void testSourceQueryIsApplied() throws IOException {
        // To test the source query is applied when we extract data,
        // we set up a job where we have a query which excludes all but one document.
        // We then assert the memory estimation is low enough.

        String sourceIndex = "test-source-query-is-applied";

        indicesAdmin().prepareCreate(sourceIndex)
            .setMapping(
                "numeric_1",
                "type=double",
                "numeric_2",
                "type=unsigned_long",
                "categorical",
                "type=keyword",
                "filtered_field",
                "type=keyword"
            )
            .get();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        for (int i = 0; i < 30; i++) {
            IndexRequest indexRequest = new IndexRequest(sourceIndex);
            indexRequest.source(
                "numeric_1",
                1.0,
                "numeric_2",
                2,
                "categorical",
                i % 2 == 0 ? "class_1" : "class_2",
                "filtered_field",
                i < 2 ? "bingo" : "rest"
            ); // We tag bingo on the first two docs to ensure we have 2 classes
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }

        String id = "test_source_query_is_applied";

        DataFrameAnalyticsConfig config = new DataFrameAnalyticsConfig.Builder().setId(id)
            .setSource(
                new DataFrameAnalyticsSource(
                    new String[] { sourceIndex },
                    QueryProvider.fromParsedQuery(QueryBuilders.termQuery("filtered_field", "bingo")),
                    null,
                    Collections.emptyMap()
                )
            )
            .setAnalysis(new Classification("categorical"))
            .buildForExplain();

        ExplainDataFrameAnalyticsAction.Response explainResponse = explainDataFrame(config);

        assertThat(explainResponse.getMemoryEstimation().getExpectedMemoryWithoutDisk().getKb(), lessThanOrEqualTo(1024L));
    }

    public void testTrainingPercentageIsApplied() throws IOException {
        String sourceIndex = "test-training-percentage-applied";
        RegressionIT.indexData(sourceIndex, 100, 0);

        DataFrameAnalyticsConfig config = new DataFrameAnalyticsConfig.Builder().setId("dfa-training-100-" + sourceIndex)
            .setSource(
                new DataFrameAnalyticsSource(
                    new String[] { sourceIndex },
                    QueryProvider.fromParsedQuery(QueryBuilders.matchAllQuery()),
                    null,
                    Collections.emptyMap()
                )
            )
            .setAnalysis(
                new Regression(
                    RegressionIT.DEPENDENT_VARIABLE_FIELD,
                    BoostedTreeParams.builder().build(),
                    null,
                    100.0,
                    null,
                    null,
                    null,
                    null,
                    null
                )
            )
            .buildForExplain();

        ExplainDataFrameAnalyticsAction.Response explainResponse = explainDataFrame(config);

        ByteSizeValue allDataUsedForTraining = explainResponse.getMemoryEstimation().getExpectedMemoryWithoutDisk();

        config = new DataFrameAnalyticsConfig.Builder().setId("dfa-training-50-" + sourceIndex)
            .setSource(
                new DataFrameAnalyticsSource(
                    new String[] { sourceIndex },
                    QueryProvider.fromParsedQuery(QueryBuilders.matchAllQuery()),
                    null,
                    Collections.emptyMap()
                )
            )
            .setAnalysis(
                new Regression(
                    RegressionIT.DEPENDENT_VARIABLE_FIELD,
                    BoostedTreeParams.builder().build(),
                    null,
                    50.0,
                    null,
                    null,
                    null,
                    null,
                    null
                )
            )
            .buildForExplain();

        explainResponse = explainDataFrame(config);

        assertThat(explainResponse.getMemoryEstimation().getExpectedMemoryWithoutDisk(), lessThanOrEqualTo(allDataUsedForTraining));
    }

    public void testSimultaneousExplainSameConfig() throws IOException {

        final int simultaneousInvocationCount = 10;

        String sourceIndex = "test-simultaneous-explain";
        RegressionIT.indexData(sourceIndex, 100, 0);

        DataFrameAnalyticsConfig config = new DataFrameAnalyticsConfig.Builder().setId("dfa-simultaneous-explain-" + sourceIndex)
            .setSource(
                new DataFrameAnalyticsSource(
                    new String[] { sourceIndex },
                    QueryProvider.fromParsedQuery(QueryBuilders.matchAllQuery()),
                    null,
                    Collections.emptyMap()
                )
            )
            .setAnalysis(
                new Regression(
                    RegressionIT.DEPENDENT_VARIABLE_FIELD,
                    BoostedTreeParams.builder().build(),
                    null,
                    100.0,
                    null,
                    null,
                    null,
                    null,
                    null
                )
            )
            .buildForExplain();

        List<ActionFuture<ExplainDataFrameAnalyticsAction.Response>> futures = new ArrayList<>();

        for (int i = 0; i < simultaneousInvocationCount; ++i) {
            futures.add(client().execute(ExplainDataFrameAnalyticsAction.INSTANCE, new ExplainDataFrameAnalyticsAction.Request(config)));
        }

        ExplainDataFrameAnalyticsAction.Response previous = null;
        for (ActionFuture<ExplainDataFrameAnalyticsAction.Response> future : futures) {
            // The main purpose of this test is that actionGet() here will throw an exception
            // if any of the simultaneous calls returns an error due to interaction between
            // the many estimation processes that get run
            ExplainDataFrameAnalyticsAction.Response current = future.actionGet(10000);
            if (previous != null) {
                // A secondary check the test can perform is that the multiple invocations
                // return the same result (but it was failures due to unwanted interactions
                // that caused this test to be written)
                assertEquals(previous, current);
            }
            previous = current;
        }
    }

    public void testRuntimeFields() {
        String sourceIndex = "test-explain-runtime-fields";
        String mapping = """
            {
                  "properties": {
                    "mapped_field": {
                      "type": "double"
                    }
                  },
                  "runtime": {
                    "mapped_runtime_field": {
                      "type": "double",
                      "script": "emit(doc['mapped_field'].value + 10.0)"
                    }
                  }
                }""";
        client().admin().indices().prepareCreate(sourceIndex).setMapping(mapping).get();
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < 10; i++) {
            Object[] source = new Object[] { "mapped_field", i };
            IndexRequest indexRequest = new IndexRequest(sourceIndex).source(source).opType(DocWriteRequest.OpType.CREATE);
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }

        Map<String, Object> configRuntimeField = new HashMap<>();
        configRuntimeField.put("type", "double");
        configRuntimeField.put("script", "emit(doc['mapped_field'].value + 20.0)");
        Map<String, Object> configRuntimeFields = Collections.singletonMap("config_runtime_field", configRuntimeField);

        DataFrameAnalyticsConfig config = new DataFrameAnalyticsConfig.Builder().setId(sourceIndex + "-job")
            .setSource(new DataFrameAnalyticsSource(new String[] { sourceIndex }, null, null, configRuntimeFields))
            .setDest(new DataFrameAnalyticsDest(sourceIndex + "-results", null))
            .setAnalysis(new OutlierDetection.Builder().build())
            .build();

        ExplainDataFrameAnalyticsAction.Response explainResponse = explainDataFrame(config);
        List<FieldSelection> fieldSelection = explainResponse.getFieldSelection();

        assertThat(fieldSelection.size(), equalTo(3));
        assertThat(
            fieldSelection.stream().map(FieldSelection::getName).collect(Collectors.toList()),
            contains("config_runtime_field", "mapped_field", "mapped_runtime_field")
        );
        assertThat(fieldSelection.stream().map(FieldSelection::isIncluded).allMatch(isIncluded -> isIncluded), is(true));
    }

    @Override
    boolean supportsInference() {
        return false;
    }
}
