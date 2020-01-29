/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.FrequencyEncodingTests;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.OneHotEncodingTests;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.TargetMeanEncodingTests;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.Ensemble;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.EnsembleTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.Tree;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    protected boolean assertToXContentEquivalence() {
        return false;
    }

    public static TrainedModelDefinition.Builder createRandomBuilder() {
        int numberOfProcessors = randomIntBetween(1, 10);
        return new TrainedModelDefinition.Builder()
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

    @Override
    protected TrainedModelDefinition createTestInstance() {
        return createRandomBuilder().build();
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

    public void testMultiClassIrisInference() throws IOException {
        // Fairly simple, random forest classification model built to fit in our format
        // Trained on the well known Iris dataset
        String compressedDef = "H4sIAMHqMV4C/+1b7Y6bOhB9lVV+b5HHX9h9g/sMVYVo4iRIbBIBaXtV9d0vJr23uzGKJwwsu" +
            "73kx0qwgI+xZ+bMmeHHqqny4uA22dNx48rVx4cfK3eo3dOX0nUHTV7tXJM1f5/88Wpd5nVdbIt13hTHw+rxYbV1eXOuX" +
            "HbIn1zdXvJpVbtTXmalO+yavb/icvyt2FwOT6558e/L8eXfnx+vh8jK/IsrLw/+qyrqD7VrjnXub+wOv7qqLtbH8lj9P" +
            "lVUu+LQ3t897sX8uue0k6rcXLPzQ2d1U53X/rkXOIcWWlYcNu57e8zaizZuXdR+8v8CKxvnH1a6bZOt90W5aU9Ce6Iqd" +
            "vvfZ7iHcyqLJvsFuz0n/Jj7ytX7Y3cNSwzrfgCWM8vtz8eHKwRwE0G+zb7m5dmfZOG9HIteBOiB9cDnV/BlYpRHLwxIb" +
            "VOehhAEFoIMICgUglSFg0rsO4PwXoUFrAPAKWLFIVG2+zGpBVfayBCCJsBPsfBNAN/2wGdX8FVipUdv2s2qFYieFTcE9" +
            "BZ7L+8xFLythsYKHDF5fb12PSCAMgO0vUKPwUrU7uszFxCEJQOJn/HNgMKS9n2D/8MT9vlnN9ISGt5gaNCojaa7yMCsE" +
            "jqVAvQMoSHqqqaLE7cNXS8mc6/JGFRwkrEVBywCHiAQiD3HEdGdE6yWYDIp4gWSTWb70kTjfgOCFygvfkOm2oi0z20og" +
            "tFqgtGmhHEN9q1ZXFwf4Oss2vZYiAFQez/u8iMc7fbmB7TxQmi9IHD0iD3ffcrcTdIiZElQ6CHa9iA0Ps4RS6gS6LaRB" +
            "qME46noAYHOjyBMkDgM56gaPW4ajstw7L5nWHQ+BWFCBRaXEHbv3Cremgw3sm/lDWXbWMKG5YxyMyUZ4nwhSWPkFUMoy" +
            "lyak0QxBBtjCASWZMdRvQgsyYzD00aWsQbwtOlUrNuOYzr9KUKSxDhEbXwdC4YwNaDQHE6JV7NxJNBjcURKjgKLOnB34" +
            "NModUB1ux4sS4U0WsygDkSd5kzigMJJepeNz4WRbYJg3ouetn0Z2hAOE5eSEEotqIRaJSy2YSm6AkOJ0H0Tt5QknpJQT" +
            "FldWfzqYL/62qrrgFLJ4ldf26+ywdX7kUvYQxJgQ9guFLmFxv3NcHWNJK2mIxUEafVvjdL4oiEV0OYOob2DGieu31NUn" +
            "y/7uFctjYxslvg8jeAXzTrm0vuGc29CTO7l3teOIk1YrNVnZI2Pz67xDXHZb1Pkoyc+mKrMhM1q86VBIwRZRI/g9EG2N" +
            "R/2fPNySpAFSpDli8S3hLr30zMXs5qlZ24xmfm+QJicHVpUAifkdORQvwY5pLTJ3dR9zDjF47dZAJ7yAwQ5TufB+E1y1" +
            "6snrlMjOVsBePkc4Q/urX4/Kv/bbK0OQ6VQkTrD/621mg+uUbxCNzWiTW/CbmpSg09ERDBTfak2QDcY1Bs1oTp/u4EaU" +
            "px2Ga0vUHQ7oJAzQNsrWFwT+xD5CW2+nOGa2AcQ9D+wtdpfnO92ldvljcuO5+Z0bjq+9M35V/jru/9nZ7qnwH9Pwf3xozzl37MO1jOo/vcP8wTvX0JBAAA=";

        TrainedModelDefinition definition = InferenceToXContentCompressor.inflate(compressedDef,
            parser -> TrainedModelDefinition.fromXContent(parser, true).build(),
            xContentRegistry());

        Map<String, Object> fields = new HashMap<>(){{
            put("sepal_length", 5.1);
            put("sepal_width", 3.5);
            put("petal_length", 1.4);
            put("petal_width", 0.2);
        }};

        assertThat(
            ((ClassificationInferenceResults)definition.getTrainedModel()
                .infer(fields, ClassificationConfig.EMPTY_PARAMS))
                .getClassificationLabel(),
            equalTo("Iris-setosa"));

        fields = new HashMap<>(){{
            put("sepal_length", 7.0);
            put("sepal_width", 3.2);
            put("petal_length", 4.7);
            put("petal_width", 1.4);
        }};
        assertThat(
            ((ClassificationInferenceResults)definition.getTrainedModel()
                .infer(fields, ClassificationConfig.EMPTY_PARAMS))
                .getClassificationLabel(),
            equalTo("Iris-versicolor"));

        fields = new HashMap<>(){{
            put("sepal_length", 6.5);
            put("sepal_width", 3.0);
            put("petal_length", 5.2);
            put("petal_width", 2.0);
        }};
        assertThat(
            ((ClassificationInferenceResults)definition.getTrainedModel()
                .infer(fields, ClassificationConfig.EMPTY_PARAMS))
                .getClassificationLabel(),
            equalTo("Iris-virginica"));
    }

}
