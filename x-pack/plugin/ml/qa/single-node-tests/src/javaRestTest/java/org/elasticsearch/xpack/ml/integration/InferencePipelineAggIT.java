/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.contains;


public class InferencePipelineAggIT extends InferenceTestCase {

    private static final String MODEL_ID = "a-complex-regression-model";
    private static final String INDEX_NAME = "store";

    @Before
    public void setupModelAndData() throws IOException {
        putRegressionModel(
            MODEL_ID,
            "{\n" +
                "            \"description\": \"super complex model for tests\",\n" +
                "            \"input\": {\"field_names\": [\"avg_cost\", \"item\"]},\n" +
                "            \"inference_config\": {\n" +
                "              \"regression\": {\n" +
                "                \"results_field\": \"regression-value\",\n" +
                "                \"num_top_feature_importance_values\": 2\n" +
                "              }\n" +
                "            },\n" +
                "            \"definition\": {\n" +
                "              \"preprocessors\" : [{\n" +
                "                \"one_hot_encoding\": {\n" +
                "                  \"field\": \"product_type\",\n" +
                "                  \"hot_map\": {\n" +
                "                    \"TV\": \"type_tv\",\n" +
                "                    \"VCR\": \"type_vcr\",\n" +
                "                    \"Laptop\": \"type_laptop\"\n" +
                "                  }\n" +
                "                }\n" +
                "              }],\n" +
                "              \"trained_model\": {\n" +
                "                \"ensemble\": {\n" +
                "                  \"feature_names\": [],\n" +
                "                  \"target_type\": \"regression\",\n" +
                "                  \"trained_models\": [\n" +
                "                  {\n" +
                "                    \"tree\": {\n" +
                "                      \"feature_names\": [\n" +
                "                        \"avg_cost\", \"type_tv\", \"type_vcr\", \"type_laptop\"\n" +
                "                      ],\n" +
                "                      \"tree_structure\": [\n" +
                "                      {\n" +
                "                        \"node_index\": 0,\n" +
                "                        \"split_feature\": 0,\n" +
                "                        \"split_gain\": 12,\n" +
                "                        \"threshold\": 38,\n" +
                "                        \"decision_type\": \"lte\",\n" +
                "                        \"default_left\": true,\n" +
                "                        \"left_child\": 1,\n" +
                "                        \"right_child\": 2\n" +
                "                      },\n" +
                "                      {\n" +
                "                        \"node_index\": 1,\n" +
                "                        \"leaf_value\": 5.0\n" +
                "                      },\n" +
                "                      {\n" +
                "                        \"node_index\": 2,\n" +
                "                        \"leaf_value\": 2.0\n" +
                "                      }\n" +
                "                      ],\n" +
                "                      \"target_type\": \"regression\"\n" +
                "                    }\n" +
                "                  }\n" +
                "                  ]\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          }");
        createIndex(INDEX_NAME, Settings.EMPTY, "\"properties\":{\n" +
            " \"product\":{\"type\": \"keyword\"},\n" +
            " \"cost\":{\"type\": \"integer\"},\n" +
            " \"time\": {\"type\": \"date\"}" +
            "}");
        indexData("{ \"product\": \"TV\", \"cost\": 300, \"time\": 1587501233000 }");
        indexData("{ \"product\": \"TV\", \"cost\": 400, \"time\": 1587501233000}");
        indexData("{ \"product\": \"VCR\", \"cost\": 150, \"time\": 1587501233000 }");
        indexData("{ \"product\": \"VCR\", \"cost\": 180, \"time\": 1587501233000 }");
        indexData("{ \"product\": \"Laptop\", \"cost\": 15000, \"time\": 1587501233000 }");
        adminClient().performRequest(new Request("POST", INDEX_NAME + "/_refresh"));
    }

    private void indexData(String data) throws IOException {
        Request request = new Request("POST", INDEX_NAME + "/_doc");
        request.setJsonEntity(data);
        client().performRequest(request);
    }

    private Response search(String searchBody) throws IOException {
        Request request = new Request("POST", INDEX_NAME + "/_search");
        request.setJsonEntity(searchBody);
        return client().performRequest(request);
    }

    @SuppressWarnings("unchecked")
    public void testPipelineRegressionSimple() throws Exception {
        Response searchResponse = search(
            "{\n" +
                "            \"size\": 0,\n" +
                "            \"aggs\": {\n" +
                "              \"good\": {\n" +
                "                \"terms\": {\n" +
                "                  \"field\": \"product\",\n" +
                "                  \"size\": 10\n" +
                "                },\n" +
                "                \"aggs\": {\n" +
                "                  \"avg_cost_agg\": {\n" +
                "                    \"avg\": {\n" +
                "                      \"field\": \"cost\"\n" +
                "                    }\n" +
                "                  },\n" +
                "                  \"regression_agg\": {\n" +
                "                    \"inference\": {\n" +
                "                      \"model_id\": \"a-complex-regression-model\",\n" +
                "                      \"inference_config\": {\n" +
                "                        \"regression\": {\n" +
                "                          \"results_field\": \"value\"\n" +
                "                        }\n" +
                "                      },\n" +
                "                      \"buckets_path\": {\n" +
                "                        \"avg_cost\": \"avg_cost_agg\"\n" +
                "                      }\n" +
                "                    }\n" +
                "                  }\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          }"
        );
        assertThat(
            (List<Double>)XContentMapValues.extractValue(
                "aggregations.good.buckets.regression_agg.value",
                responseAsMap(searchResponse)
            ),
            contains(2.0, 2.0, 2.0)
        );
    }

    @SuppressWarnings("unchecked")
    public void testPipelineAggReferencingSingleBucket() throws Exception {
        Response searchResponse = search(
            "{\n" +
                "              \"size\": 0,\n" +
                "              \"query\": {\n" +
                "                \"match_all\": {}\n" +
                "              },\n" +
                "              \"aggs\": {\n" +
                "                \"date_histo\": {\n" +
                "                  \"date_histogram\": {\n" +
                "                    \"field\": \"time\",\n" +
                "                    \"fixed_interval\": \"1d\"\n" +
                "                  },\n" +
                "                  \"aggs\": {\n" +
                "                    \"good\": {\n" +
                "                      \"terms\": {\n" +
                "                        \"field\": \"product\",\n" +
                "                        \"size\": 10\n" +
                "                      },\n" +
                "                      \"aggs\": {\n" +
                "                        \"avg_cost_agg\": {\n" +
                "                          \"avg\": {\n" +
                "                            \"field\": \"cost\"\n" +
                "                          }\n" +
                "                        }\n" +
                "                      }\n" +
                "                    },\n" +
                "                    \"regression_agg\": {\n" +
                "                      \"inference\": {\n" +
                "                        \"model_id\": \"a-complex-regression-model\",\n" +
                "                        \"buckets_path\": {\n" +
                "                          \"avg_cost\": \"good['TV']>avg_cost_agg\",\n" +
                "                          \"product_type\": \"good['TV']\"\n" +
                "                        }\n" +
                "                      }\n" +
                "                    }\n" +
                "                  }\n" +
                "                }\n" +
                "              }\n" +
                "            }"
        );
        assertThat(
            (List<Double>)XContentMapValues.extractValue(
                "aggregations.date_histo.buckets.regression_agg.value",
                responseAsMap(searchResponse)
            ),
            contains(2.0)
        );
    }

    @SuppressWarnings("unchecked")
    public void testAllFieldsMissingWarning() throws IOException {
        Response searchResponse = search(
            "{\n" +
                "            \"size\": 0,\n" +
                "            \"query\": { \"match_all\" : { } },\n" +
                "            \"aggs\": {\n" +
                "              \"good\": {\n" +
                "                \"terms\": {\n" +
                "                  \"field\": \"product\",\n" +
                "                  \"size\": 10\n" +
                "                },\n" +
                "                \"aggs\": {\n" +
                "                  \"avg_cost_agg\": {\n" +
                "                    \"avg\": {\n" +
                "                      \"field\": \"cost\"\n" +
                "                    }\n" +
                "                  },\n" +
                "                  \"regression_agg\" : {\n" +
                "                    \"inference\": {\n" +
                "                      \"model_id\": \"a-complex-regression-model\",\n" +
                "                      \"buckets_path\": {\n" +
                "                        \"cost\" : \"avg_cost_agg\"\n" +
                "                      }\n" +
                "                    }\n" +
                "                  }\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          }"
        );
        assertThat(
            (List<String>)XContentMapValues.extractValue(
                "aggregations.good.buckets.regression_agg.warning",
                responseAsMap(searchResponse)
            ),
            contains(
                "Model [a-complex-regression-model] could not be inferred as all fields were missing",
                "Model [a-complex-regression-model] could not be inferred as all fields were missing",
                "Model [a-complex-regression-model] could not be inferred as all fields were missing"
            )
        );
    }

}
