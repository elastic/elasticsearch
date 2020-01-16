/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.ingest.SimulateDocumentBaseResult;
import org.elasticsearch.action.ingest.SimulatePipelineResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ml.action.DeleteTrainedModelAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class InferenceIngestIT extends MlNativeAutodetectIntegTestCase {

    @Before
    public void createBothModels() throws Exception {
        client().execute(PutTrainedModelAction.INSTANCE, new PutTrainedModelAction.Request(buildClassificationModel())).actionGet();
        client().execute(PutTrainedModelAction.INSTANCE, new PutTrainedModelAction.Request(buildRegressionModel())).actionGet();
    }

    @After
    public void deleteBothModels() {
        client().execute(DeleteTrainedModelAction.INSTANCE, new DeleteTrainedModelAction.Request("test_classification")).actionGet();
        client().execute(DeleteTrainedModelAction.INSTANCE, new DeleteTrainedModelAction.Request("test_regression")).actionGet();
    }

    public void testPipelineCreationAndDeletion() throws Exception {

        for (int i = 0; i < 10; i++) {
            assertThat(client().admin().cluster().preparePutPipeline("simple_classification_pipeline",
                new BytesArray(CLASSIFICATION_PIPELINE.getBytes(StandardCharsets.UTF_8)),
                XContentType.JSON).get().isAcknowledged(), is(true));

            client().prepareIndex("index_for_inference_test")
                .setSource(new HashMap<>(){{
                    put("col1", randomFrom("female", "male"));
                    put("col2", randomFrom("S", "M", "L", "XL"));
                    put("col3", randomFrom("true", "false", "none", "other"));
                    put("col4", randomIntBetween(0, 10));
                }})
                .setPipeline("simple_classification_pipeline")
                .get();

            assertThat(client().admin().cluster().prepareDeletePipeline("simple_classification_pipeline").get().isAcknowledged(),
                is(true));

            assertThat(client().admin().cluster().preparePutPipeline("simple_regression_pipeline",
                new BytesArray(REGRESSION_PIPELINE.getBytes(StandardCharsets.UTF_8)),
                XContentType.JSON).get().isAcknowledged(), is(true));

            client().prepareIndex("index_for_inference_test")
                .setSource(new HashMap<>(){{
                    put("col1", randomFrom("female", "male"));
                    put("col2", randomFrom("S", "M", "L", "XL"));
                    put("col3", randomFrom("true", "false", "none", "other"));
                    put("col4", randomIntBetween(0, 10));
                }})
                .setPipeline("simple_regression_pipeline")
                .get();

            assertThat(client().admin().cluster().prepareDeletePipeline("simple_regression_pipeline").get().isAcknowledged(),
                is(true));
        }

        assertThat(client().admin().cluster().preparePutPipeline("simple_classification_pipeline",
            new BytesArray(CLASSIFICATION_PIPELINE.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON).get().isAcknowledged(), is(true));

        assertThat(client().admin().cluster().preparePutPipeline("simple_regression_pipeline",
            new BytesArray(REGRESSION_PIPELINE.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON).get().isAcknowledged(), is(true));

        for (int i = 0; i < 10; i++) {
            client().prepareIndex("index_for_inference_test")
                .setSource(generateSourceDoc())
                .setPipeline("simple_classification_pipeline")
                .get();

            client().prepareIndex("index_for_inference_test")
                .setSource(generateSourceDoc())
                .setPipeline("simple_regression_pipeline")
                .get();
        }

        assertThat(client().admin().cluster().prepareDeletePipeline("simple_classification_pipeline").get().isAcknowledged(),
            is(true));

        assertThat(client().admin().cluster().prepareDeletePipeline("simple_regression_pipeline").get().isAcknowledged(),
            is(true));

        client().admin().indices().refresh(new RefreshRequest("index_for_inference_test")).get();

        assertThat(client().search(new SearchRequest().indices("index_for_inference_test")
                .source(new SearchSourceBuilder()
                    .size(0)
                    .trackTotalHits(true)
                    .query(QueryBuilders.boolQuery()
                        .filter(
                            QueryBuilders.existsQuery("ml.inference.regression.predicted_value"))))).get().getHits().getTotalHits().value,
            equalTo(20L));

        assertThat(client().search(new SearchRequest().indices("index_for_inference_test")
                .source(new SearchSourceBuilder()
                    .size(0)
                    .trackTotalHits(true)
                    .query(QueryBuilders.boolQuery()
                        .filter(
                            QueryBuilders.existsQuery("ml.inference.classification.predicted_value")))))
                .get()
                .getHits()
                .getTotalHits()
                .value,
            equalTo(20L));

    }

    public void testSimulate() {
        String source = "{\n" +
            "  \"pipeline\": {\n" +
            "    \"processors\": [\n" +
            "      {\n" +
            "        \"inference\": {\n" +
            "          \"target_field\": \"ml.classification\",\n" +
            "          \"inference_config\": {\"classification\": " +
            "                {\"num_top_classes\":2, \"top_classes_results_field\": \"result_class_prob\"}},\n" +
            "          \"model_id\": \"test_classification\",\n" +
            "          \"field_mappings\": {\n" +
            "            \"col1\": \"col1\",\n" +
            "            \"col2\": \"col2\",\n" +
            "            \"col3\": \"col3\",\n" +
            "            \"col4\": \"col4\"\n" +
            "          }\n" +
            "        }\n" +
            "      },\n" +
            "      {\n" +
            "        \"inference\": {\n" +
            "          \"target_field\": \"ml.regression\",\n" +
            "          \"model_id\": \"test_regression\",\n" +
            "          \"inference_config\": {\"regression\":{}},\n" +
            "          \"field_mappings\": {\n" +
            "            \"col1\": \"col1\",\n" +
            "            \"col2\": \"col2\",\n" +
            "            \"col3\": \"col3\",\n" +
            "            \"col4\": \"col4\"\n" +
            "          }\n" +
            "        }\n" +
            "      }\n" +
            "    ]\n" +
            "  },\n" +
            "  \"docs\": [\n" +
            "    {\"_source\": {\n" +
            "      \"col1\": \"female\",\n" +
            "      \"col2\": \"M\",\n" +
            "      \"col3\": \"none\",\n" +
            "      \"col4\": 10\n" +
            "    }}]\n" +
            "}";

        SimulatePipelineResponse response = client().admin().cluster()
            .prepareSimulatePipeline(new BytesArray(source.getBytes(StandardCharsets.UTF_8)),
                XContentType.JSON).get();
        SimulateDocumentBaseResult baseResult = (SimulateDocumentBaseResult)response.getResults().get(0);
        assertThat(baseResult.getIngestDocument().getFieldValue("ml.regression.predicted_value", Double.class), equalTo(1.0));
        assertThat(baseResult.getIngestDocument().getFieldValue("ml.classification.predicted_value", String.class),
            equalTo("second"));
        assertThat(baseResult.getIngestDocument().getFieldValue("ml.classification.result_class_prob", List.class).size(),
            equalTo(2));

        String sourceWithMissingModel = "{\n" +
            "  \"pipeline\": {\n" +
            "    \"processors\": [\n" +
            "      {\n" +
            "        \"inference\": {\n" +
            "          \"model_id\": \"test_classification_missing\",\n" +
            "          \"inference_config\": {\"classification\":{}},\n" +
            "          \"field_mappings\": {\n" +
            "            \"col1\": \"col1\",\n" +
            "            \"col2\": \"col2\",\n" +
            "            \"col3\": \"col3\",\n" +
            "            \"col4\": \"col4\"\n" +
            "          }\n" +
            "        }\n" +
            "      }\n" +
            "    ]\n" +
            "  },\n" +
            "  \"docs\": [\n" +
            "    {\"_source\": {\n" +
            "      \"col1\": \"female\",\n" +
            "      \"col2\": \"M\",\n" +
            "      \"col3\": \"none\",\n" +
            "      \"col4\": 10\n" +
            "    }}]\n" +
            "}";

        response = client().admin().cluster()
            .prepareSimulatePipeline(new BytesArray(sourceWithMissingModel.getBytes(StandardCharsets.UTF_8)),
                XContentType.JSON).get();

        assertThat(((SimulateDocumentBaseResult) response.getResults().get(0)).getFailure().getMessage(),
            containsString("Could not find trained model [test_classification_missing]"));
    }

    public void testSimulateLangIdent() {
        String source = "{\n" +
            "  \"pipeline\": {\n" +
            "    \"processors\": [\n" +
            "      {\n" +
            "        \"inference\": {\n" +
            "          \"inference_config\": {\"classification\":{}},\n" +
            "          \"model_id\": \"lang_ident_model_1\",\n" +
            "          \"field_mappings\": {}\n" +
            "        }\n" +
            "      }\n" +
            "    ]\n" +
            "  },\n" +
            "  \"docs\": [\n" +
            "    {\"_source\": {\n" +
            "      \"text\": \"this is some plain text.\"\n" +
            "    }}]\n" +
            "}";

        SimulatePipelineResponse response = client().admin().cluster()
            .prepareSimulatePipeline(new BytesArray(source.getBytes(StandardCharsets.UTF_8)),
                XContentType.JSON).get();
        SimulateDocumentBaseResult baseResult = (SimulateDocumentBaseResult)response.getResults().get(0);
        assertThat(baseResult.getIngestDocument().getFieldValue("ml.inference.predicted_value", String.class), equalTo("en"));
    }

    private Map<String, Object> generateSourceDoc() {
        return new HashMap<>(){{
            put("col1", randomFrom("female", "male"));
            put("col2", randomFrom("S", "M", "L", "XL"));
            put("col3", randomFrom("true", "false", "none", "other"));
            put("col4", randomIntBetween(0, 10));
        }};
    }

    private static final String REGRESSION_DEFINITION = "{" +
        "  \"preprocessors\": [\n" +
        "    {\n" +
        "      \"one_hot_encoding\": {\n" +
        "        \"field\": \"col1\",\n" +
        "        \"hot_map\": {\n" +
        "          \"male\": \"col1_male\",\n" +
        "          \"female\": \"col1_female\"\n" +
        "        }\n" +
        "      }\n" +
        "    },\n" +
        "    {\n" +
        "      \"target_mean_encoding\": {\n" +
        "        \"field\": \"col2\",\n" +
        "        \"feature_name\": \"col2_encoded\",\n" +
        "        \"target_map\": {\n" +
        "          \"S\": 5.0,\n" +
        "          \"M\": 10.0,\n" +
        "          \"L\": 20\n" +
        "        },\n" +
        "        \"default_value\": 5.0\n" +
        "      }\n" +
        "    },\n" +
        "    {\n" +
        "      \"frequency_encoding\": {\n" +
        "        \"field\": \"col3\",\n" +
        "        \"feature_name\": \"col3_encoded\",\n" +
        "        \"frequency_map\": {\n" +
        "          \"none\": 0.75,\n" +
        "          \"true\": 0.10,\n" +
        "          \"false\": 0.15\n" +
        "        }\n" +
        "      }\n" +
        "    }\n" +
        "  ],\n" +
        "  \"trained_model\": {\n" +
        "    \"ensemble\": {\n" +
        "      \"feature_names\": [\n" +
        "        \"col1_male\",\n" +
        "        \"col1_female\",\n" +
        "        \"col2_encoded\",\n" +
        "        \"col3_encoded\",\n" +
        "        \"col4\"\n" +
        "      ],\n" +
        "      \"aggregate_output\": {\n" +
        "        \"weighted_sum\": {\n" +
        "          \"weights\": [\n" +
        "            0.5,\n" +
        "            0.5\n" +
        "          ]\n" +
        "        }\n" +
        "      },\n" +
        "      \"target_type\": \"regression\",\n" +
        "      \"trained_models\": [\n" +
        "        {\n" +
        "          \"tree\": {\n" +
        "            \"feature_names\": [\n" +
        "              \"col1_male\",\n" +
        "              \"col1_female\",\n" +
        "              \"col4\"\n" +
        "            ],\n" +
        "            \"tree_structure\": [\n" +
        "              {\n" +
        "                \"node_index\": 0,\n" +
        "                \"split_feature\": 0,\n" +
        "                \"split_gain\": 12.0,\n" +
        "                \"threshold\": 10.0,\n" +
        "                \"decision_type\": \"lte\",\n" +
        "                \"default_left\": true,\n" +
        "                \"left_child\": 1,\n" +
        "                \"right_child\": 2\n" +
        "              },\n" +
        "              {\n" +
        "                \"node_index\": 1,\n" +
        "                \"leaf_value\": 1\n" +
        "              },\n" +
        "              {\n" +
        "                \"node_index\": 2,\n" +
        "                \"leaf_value\": 2\n" +
        "              }\n" +
        "            ],\n" +
        "            \"target_type\": \"regression\"\n" +
        "          }\n" +
        "        },\n" +
        "        {\n" +
        "          \"tree\": {\n" +
        "            \"feature_names\": [\n" +
        "              \"col2_encoded\",\n" +
        "              \"col3_encoded\",\n" +
        "              \"col4\"\n" +
        "            ],\n" +
        "            \"tree_structure\": [\n" +
        "              {\n" +
        "                \"node_index\": 0,\n" +
        "                \"split_feature\": 0,\n" +
        "                \"split_gain\": 12.0,\n" +
        "                \"threshold\": 10.0,\n" +
        "                \"decision_type\": \"lte\",\n" +
        "                \"default_left\": true,\n" +
        "                \"left_child\": 1,\n" +
        "                \"right_child\": 2\n" +
        "              },\n" +
        "              {\n" +
        "                \"node_index\": 1,\n" +
        "                \"leaf_value\": 1\n" +
        "              },\n" +
        "              {\n" +
        "                \"node_index\": 2,\n" +
        "                \"leaf_value\": 2\n" +
        "              }\n" +
        "            ],\n" +
        "            \"target_type\": \"regression\"\n" +
        "          }\n" +
        "        }\n" +
        "      ]\n" +
        "    }\n" +
        "  }\n" +
        "}";

    private static final String REGRESSION_CONFIG = "{" +
        "  \"model_id\": \"test_regression\",\n" +
        "  \"input\":{\"field_names\":[\"col1\",\"col2\",\"col3\",\"col4\"]}," +
        "  \"description\": \"test model for regression\",\n" +
        "  \"version\": \"8.0.0\",\n" +
        "  \"definition\": " + REGRESSION_DEFINITION + ","+
        "  \"license_level\": \"platinum\",\n" +
        "  \"created_by\": \"ml_test\",\n" +
        "  \"estimated_heap_memory_usage_bytes\": 0," +
        "  \"estimated_operations\": 0," +
        "  \"created_time\": 0" +
        "}";

    private static final String CLASSIFICATION_DEFINITION = "{" +
        "  \"preprocessors\": [\n" +
        "    {\n" +
        "      \"one_hot_encoding\": {\n" +
        "        \"field\": \"col1\",\n" +
        "        \"hot_map\": {\n" +
        "          \"male\": \"col1_male\",\n" +
        "          \"female\": \"col1_female\"\n" +
        "        }\n" +
        "      }\n" +
        "    },\n" +
        "    {\n" +
        "      \"target_mean_encoding\": {\n" +
        "        \"field\": \"col2\",\n" +
        "        \"feature_name\": \"col2_encoded\",\n" +
        "        \"target_map\": {\n" +
        "          \"S\": 5.0,\n" +
        "          \"M\": 10.0,\n" +
        "          \"L\": 20\n" +
        "        },\n" +
        "        \"default_value\": 5.0\n" +
        "      }\n" +
        "    },\n" +
        "    {\n" +
        "      \"frequency_encoding\": {\n" +
        "        \"field\": \"col3\",\n" +
        "        \"feature_name\": \"col3_encoded\",\n" +
        "        \"frequency_map\": {\n" +
        "          \"none\": 0.75,\n" +
        "          \"true\": 0.10,\n" +
        "          \"false\": 0.15\n" +
        "        }\n" +
        "      }\n" +
        "    }\n" +
        "  ],\n" +
        "  \"trained_model\": {\n" +
        "    \"ensemble\": {\n" +
        "      \"feature_names\": [\n" +
        "        \"col1_male\",\n" +
        "        \"col1_female\",\n" +
        "        \"col2_encoded\",\n" +
        "        \"col3_encoded\",\n" +
        "        \"col4\"\n" +
        "      ],\n" +
        "      \"aggregate_output\": {\n" +
        "        \"weighted_mode\": {\n" +
        "          \"weights\": [\n" +
        "            0.5,\n" +
        "            0.5\n" +
        "          ]\n" +
        "        }\n" +
        "      },\n" +
        "      \"target_type\": \"classification\",\n" +
        "      \"classification_labels\": [\"first\", \"second\"],\n" +
        "      \"trained_models\": [\n" +
        "        {\n" +
        "          \"tree\": {\n" +
        "            \"feature_names\": [\n" +
        "              \"col1_male\",\n" +
        "              \"col1_female\",\n" +
        "              \"col4\"\n" +
        "            ],\n" +
        "            \"tree_structure\": [\n" +
        "              {\n" +
        "                \"node_index\": 0,\n" +
        "                \"split_feature\": 0,\n" +
        "                \"split_gain\": 12.0,\n" +
        "                \"threshold\": 10.0,\n" +
        "                \"decision_type\": \"lte\",\n" +
        "                \"default_left\": true,\n" +
        "                \"left_child\": 1,\n" +
        "                \"right_child\": 2\n" +
        "              },\n" +
        "              {\n" +
        "                \"node_index\": 1,\n" +
        "                \"leaf_value\": 1\n" +
        "              },\n" +
        "              {\n" +
        "                \"node_index\": 2,\n" +
        "                \"leaf_value\": 0\n" +
        "              }\n" +
        "            ],\n" +
        "            \"target_type\": \"regression\"\n" +
        "          }\n" +
        "        },\n" +
        "        {\n" +
        "          \"tree\": {\n" +
        "            \"feature_names\": [\n" +
        "              \"col2_encoded\",\n" +
        "              \"col3_encoded\",\n" +
        "              \"col4\"\n" +
        "            ],\n" +
        "            \"tree_structure\": [\n" +
        "              {\n" +
        "                \"node_index\": 0,\n" +
        "                \"split_feature\": 0,\n" +
        "                \"split_gain\": 12.0,\n" +
        "                \"threshold\": 10.0,\n" +
        "                \"decision_type\": \"lte\",\n" +
        "                \"default_left\": true,\n" +
        "                \"left_child\": 1,\n" +
        "                \"right_child\": 2\n" +
        "              },\n" +
        "              {\n" +
        "                \"node_index\": 1,\n" +
        "                \"leaf_value\": 1\n" +
        "              },\n" +
        "              {\n" +
        "                \"node_index\": 2,\n" +
        "                \"leaf_value\": 0\n" +
        "              }\n" +
        "            ],\n" +
        "            \"target_type\": \"regression\"\n" +
        "          }\n" +
        "        }\n" +
        "      ]\n" +
        "    }\n" +
        "  }\n" +
        "}";

    private TrainedModelConfig buildClassificationModel() throws IOException {
        try (XContentParser parser = XContentHelper.createParser(xContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            new BytesArray(CLASSIFICATION_CONFIG),
            XContentType.JSON)) {
            return TrainedModelConfig.LENIENT_PARSER.apply(parser, null).build();
        }
    }

    private TrainedModelConfig buildRegressionModel() throws IOException {
        try (XContentParser parser = XContentHelper.createParser(xContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            new BytesArray(REGRESSION_CONFIG),
            XContentType.JSON)) {
            return TrainedModelConfig.LENIENT_PARSER.apply(parser, null).build();
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
    }

    private static final String CLASSIFICATION_CONFIG = "" +
        "{\n" +
        "  \"model_id\": \"test_classification\",\n" +
        "  \"input\":{\"field_names\":[\"col1\",\"col2\",\"col3\",\"col4\"]}," +
        "  \"description\": \"test model for classification\",\n" +
        "  \"definition\": " + CLASSIFICATION_DEFINITION + ","+
        "  \"version\": \"8.0.0\",\n" +
        "  \"license_level\": \"platinum\",\n" +
        "  \"created_by\": \"es_test\",\n" +
        "  \"estimated_heap_memory_usage_bytes\": 0," +
        "  \"estimated_operations\": 0," +
        "  \"created_time\": 0\n" +
        "}";

    private static final String CLASSIFICATION_PIPELINE = "{" +
        "    \"processors\": [\n" +
        "      {\n" +
        "        \"inference\": {\n" +
        "          \"model_id\": \"test_classification\",\n" +
        "          \"tag\": \"classification\",\n" +
        "          \"inference_config\": {\"classification\": {}},\n" +
        "          \"field_mappings\": {\n" +
        "            \"col1\": \"col1\",\n" +
        "            \"col2\": \"col2\",\n" +
        "            \"col3\": \"col3\",\n" +
        "            \"col4\": \"col4\"\n" +
        "          }\n" +
        "        }\n" +
        "      }]}\n";

    private static final String REGRESSION_PIPELINE = "{" +
        "    \"processors\": [\n" +
        "      {\n" +
        "        \"inference\": {\n" +
        "          \"model_id\": \"test_regression\",\n" +
        "          \"tag\": \"regression\",\n" +
        "          \"inference_config\": {\"regression\": {}},\n" +
        "          \"field_mappings\": {\n" +
        "            \"col1\": \"col1\",\n" +
        "            \"col2\": \"col2\",\n" +
        "            \"col3\": \"col3\",\n" +
        "            \"col4\": \"col4\"\n" +
        "          }\n" +
        "        }\n" +
        "      }]}\n";

}
