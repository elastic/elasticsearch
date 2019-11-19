/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.FrequencyEncodingTests;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.OneHotEncodingTests;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.TargetMeanEncodingTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.Ensemble;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.EnsembleTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.Tree;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.core.ml.utils.ToXContentParams.FOR_INTERNAL_STORAGE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;


public class TrainedModelDefinitionTests extends AbstractSerializingTestCase<TrainedModelDefinition> {

    @Override
    protected TrainedModelDefinition doParseInstance(XContentParser parser) throws IOException {
        return TrainedModelDefinition.fromXContent(parser, true).build();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> !field.isEmpty();
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return new ToXContent.MapParams(Collections.singletonMap(FOR_INTERNAL_STORAGE, "true"));
    }

    @Override
    protected boolean assertToXContentEquivalence() {
        return false;
    }

    public static TrainedModelDefinition.Builder createRandomBuilder(String modelId) {
        int numberOfProcessors = randomIntBetween(1, 10);
        return new TrainedModelDefinition.Builder()
            .setModelId(modelId)
            .setPreProcessors(
                randomBoolean() ? null :
                    Stream.generate(() -> randomFrom(FrequencyEncodingTests.createRandom(),
                        OneHotEncodingTests.createRandom(),
                        TargetMeanEncodingTests.createRandom()))
                        .limit(numberOfProcessors)
                        .collect(Collectors.toList()))
            .setTrainedModel(randomFrom(TreeTests.createRandom(), EnsembleTests.createRandom()));
    }

    private static final String ENSEMBLE_MODEL = "" +
        "{\n" +
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
    private static final String TREE_MODEL = "" +
        "{\n" +
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
        "    \"tree\": {\n" +
        "      \"feature_names\": [\n" +
        "        \"col1_male\",\n" +
        "        \"col1_female\",\n" +
        "        \"col4\"\n" +
        "      ],\n" +
        "      \"tree_structure\": [\n" +
        "        {\n" +
        "          \"node_index\": 0,\n" +
        "          \"split_feature\": 0,\n" +
        "          \"split_gain\": 12.0,\n" +
        "          \"threshold\": 10.0,\n" +
        "          \"decision_type\": \"lte\",\n" +
        "          \"default_left\": true,\n" +
        "          \"left_child\": 1,\n" +
        "          \"right_child\": 2\n" +
        "        },\n" +
        "        {\n" +
        "          \"node_index\": 1,\n" +
        "          \"leaf_value\": 1\n" +
        "        },\n" +
        "        {\n" +
        "          \"node_index\": 2,\n" +
        "          \"leaf_value\": 2\n" +
        "        }\n" +
        "      ],\n" +
        "      \"target_type\": \"regression\"\n" +
        "    }\n" +
        "  }\n" +
        "}";

    public void testEnsembleSchemaDeserialization() throws IOException {
        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(xContentRegistry(), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, ENSEMBLE_MODEL);
        TrainedModelDefinition definition = TrainedModelDefinition.fromXContent(parser, false).build();
        assertThat(definition.getTrainedModel().getClass(), equalTo(Ensemble.class));
    }

    public void testTreeSchemaDeserialization() throws IOException {
        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(xContentRegistry(), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, TREE_MODEL);
        TrainedModelDefinition definition = TrainedModelDefinition.fromXContent(parser, false).build();
        assertThat(definition.getTrainedModel().getClass(), equalTo(Tree.class));
    }

    public void testStrictParser() throws IOException {
        TrainedModelDefinition.Builder builder = createRandomBuilder("asdf");
        BytesReference reference = XContentHelper.toXContent(builder.build(),
            XContentType.JSON,
            new ToXContent.MapParams(Collections.singletonMap(FOR_INTERNAL_STORAGE, "true")),
            false);

        XContentParser parser = XContentHelper.createParser(xContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            reference,
            XContentType.JSON);

        XContentParseException exception = expectThrows(XContentParseException.class,
            () -> TrainedModelDefinition.fromXContent(parser, false));

        assertThat(exception.getMessage(), containsString("[trained_model_definition] unknown field [doc_type]"));
    }

    @Override
    protected TrainedModelDefinition createTestInstance() {
        return createRandomBuilder(randomAlphaOfLength(10)).build();
    }

    @Override
    protected Writeable.Reader<TrainedModelDefinition> instanceReader() {
        return TrainedModelDefinition::new;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    public void testRamUsageEstimation() {
        TrainedModelDefinition test = createTestInstance();
        assertThat(test.ramBytesUsed(), greaterThan(0L));
    }

}
