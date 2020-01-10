/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.core.ml.action.ExplainDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Classification;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.OutlierDetection;
import org.elasticsearch.xpack.core.ml.utils.QueryProvider;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ExplainDataFrameAnalyticsIT extends MlNativeDataFrameAnalyticsIntegTestCase {

    public void testExplain_GivenMissingSourceIndex() {
        DataFrameAnalyticsConfig config = new DataFrameAnalyticsConfig.Builder()
            .setSource(new DataFrameAnalyticsSource(new String[] {"missing_index"}, null, null))
            .setAnalysis(new OutlierDetection.Builder().build())
            .buildForExplain();

        ResourceNotFoundException e = expectThrows(ResourceNotFoundException.class, () -> explainDataFrame(config));
        assertThat(e.getMessage(), equalTo("cannot retrieve data because index [missing_index] does not exist"));
    }

    public void testSourceQueryIsApplied() throws IOException {
        // To test the source query is applied when we extract data,
        // we set up a job where we have a query which excludes all but one document.
        // We then assert the memory estimation is low enough.

        String sourceIndex = "test-source-query-is-applied";

        client().admin().indices().prepareCreate(sourceIndex)
            .addMapping("_doc", "numeric_1", "type=double", "numeric_2", "type=float", "categorical", "type=keyword")
            .get();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        for (int i = 0; i < 30; i++) {
            IndexRequest indexRequest = new IndexRequest(sourceIndex);

            // We insert one odd value out of 5 for one feature
            indexRequest.source("numeric_1", 1.0, "numeric_2", 2.0, "categorical", i == 0 ? "only-one" : "normal");
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }

        String id = "test_source_query_is_applied";

        DataFrameAnalyticsConfig config = new DataFrameAnalyticsConfig.Builder()
            .setId(id)
            .setSource(new DataFrameAnalyticsSource(new String[] { sourceIndex },
                QueryProvider.fromParsedQuery(QueryBuilders.termQuery("categorical", "only-one")),
                null))
            .setAnalysis(new Classification("categorical"))
            .buildForExplain();

        ExplainDataFrameAnalyticsAction.Response explainResponse = explainDataFrame(config);

        assertThat(explainResponse.getMemoryEstimation().getExpectedMemoryWithoutDisk().getKb(), lessThanOrEqualTo(1024L));
    }
}
