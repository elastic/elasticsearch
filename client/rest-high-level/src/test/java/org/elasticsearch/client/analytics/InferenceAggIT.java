/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.analytics;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.ml.PutTrainedModelRequest;
import org.elasticsearch.client.ml.inference.TrainedModelConfig;
import org.elasticsearch.client.ml.inference.TrainedModelDefinition;
import org.elasticsearch.client.ml.inference.TrainedModelInput;
import org.elasticsearch.client.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.client.ml.inference.trainedmodel.tree.Tree;
import org.elasticsearch.client.ml.inference.trainedmodel.tree.TreeNode;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class InferenceAggIT extends ESRestHighLevelClientTestCase {

    public void testInferenceAgg() throws IOException {

        // create a very simple decision tree with a root node and 2 leaves
        List<String> featureNames = Collections.singletonList("cost");
        Tree.Builder builder = Tree.builder();
        builder.setFeatureNames(featureNames);
        TreeNode.Builder root = builder.addJunction(0, 0, true, 1.0);
        int leftChild = root.getLeftChild();
        int rightChild = root.getRightChild();
        builder.addLeaf(leftChild, 10.0);
        builder.addLeaf(rightChild, 20.0);

        final String modelId = "simple_regression";
        putTrainedModel(modelId, featureNames, builder.build());

        final String index = "inference-test-data";
        indexData(index);

        TermsAggregationBuilder termsAgg = new TermsAggregationBuilder("fruit_type").field("fruit");
        AvgAggregationBuilder avgAgg = new AvgAggregationBuilder("avg_cost").field("cost");
        termsAgg.subAggregation(avgAgg);

        Map<String, String> bucketPaths = new HashMap<>();
        bucketPaths.put("cost", "avg_cost");
        InferencePipelineAggregationBuilder inferenceAgg = new InferencePipelineAggregationBuilder("infer", modelId,  bucketPaths);
        termsAgg.subAggregation(inferenceAgg);

        SearchRequest search = new SearchRequest(index);
        search.source().aggregation(termsAgg);
        SearchResponse response = highLevelClient().search(search, RequestOptions.DEFAULT);
        ParsedTerms terms = response.getAggregations().get("fruit_type");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        {
            assertThat(buckets.get(0).getKey(), equalTo("apple"));
            ParsedInference inference = buckets.get(0).getAggregations().get("infer");
            assertThat((Double) inference.getValue(), closeTo(20.0, 0.01));
            assertNull(inference.getWarning());
            assertNull(inference.getFeatureImportance());
            assertNull(inference.getTopClasses());
        }
        {
            assertThat(buckets.get(1).getKey(), equalTo("banana"));
            ParsedInference inference = buckets.get(1).getAggregations().get("infer");
            assertThat((Double) inference.getValue(), closeTo(10.0, 0.01));
            assertNull(inference.getWarning());
            assertNull(inference.getFeatureImportance());
            assertNull(inference.getTopClasses());
        }
    }

    private void putTrainedModel(String modelId, List<String> inputFields, Tree tree) throws IOException {
        TrainedModelDefinition definition = new TrainedModelDefinition.Builder().setTrainedModel(tree).build();
        TrainedModelConfig trainedModelConfig = TrainedModelConfig.builder()
            .setDefinition(definition)
            .setModelId(modelId)
            .setInferenceConfig(new RegressionConfig())
            .setInput(new TrainedModelInput(inputFields))
            .setDescription("test model")
            .build();
        highLevelClient().machineLearning().putTrainedModel(new PutTrainedModelRequest(trainedModelConfig), RequestOptions.DEFAULT);
    }

    private void indexData(String index) throws IOException {
        CreateIndexRequest create = new CreateIndexRequest(index);
        create.mapping("{\"properties\": {\"fruit\": {\"type\": \"keyword\"}," +
            "\"cost\": {\"type\": \"double\"}}}", XContentType.JSON);
        highLevelClient().indices().create(create, RequestOptions.DEFAULT);
        BulkRequest bulk = new BulkRequest(index).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulk.add(new IndexRequest().source(XContentType.JSON, "fruit", "apple", "cost", "1.2"));
        bulk.add(new IndexRequest().source(XContentType.JSON, "fruit", "banana", "cost", "0.8"));
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        highLevelClient().bulk(bulk, RequestOptions.DEFAULT);
    }
}
