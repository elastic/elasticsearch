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
        String compressedDef = "H4sIAPbiMl4C/+1b246bMBD9lVWet8jjG3b/oN9QVYgmToLEkghIL6r23wukl90" +
            "YxRMGlt2WPKwEC/gYe2bOnBl+rOoyzQq3SR4OG5ev3t/9WLmicg+fc9cd1Gm5c3VSfz+2x6t1nlZVts3Wa" +
            "Z0ditX93Wrr0vpUuqRIH1zVXPJxVbljmie5K3b1vr3ifPw125wPj65+9u/z8fnfn+4vh0jy9LPLzw/+UGb" +
            "Vu8rVhyptb+wOv7iyytaH/FD+PZWVu6xo7u8e92x+3XOaSZVurtm1QydVXZ7W7XPPcIoGWpIVG/etOWbNR" +
            "Ru3zqp28r+B5bVrH5a7bZ2s91m+aU5Cc6LMdvu/Z3gL55hndfILdnNOtGPuS1ftD901LDKs+wFYziy3j/d" +
            "3FwjgKoJ0m3xJ81N7kvn3cix64aEH1gOfX8CXkVEtemFAahvz2IcgsBCkB0GhEMTKH1Ri3xn49yosYO0Bj" +
            "hErDpGy3Y9JLbjSRvoQNAF+jIVvPPi2Bz67gK8iK1v0ptmsWoHoWXFDQG+x9/IeQ8Hbqm+swBGT15dr1wM" +
            "CKDNA2yv0GKxE7b4+cwFBWDKQ+BlfDSgsat43tH94xD49diMtoeEVhgaN2mi6iwzMKqFjKUDPEBqCrmq6O" +
            "HHd0PViMreajEEFJxlaccAi4B4CgdhzHBHdOcFqCSYTI14g2WS2z0007DfAe4Hy7DdkrI2I+9yGIhitJhh" +
            "tTBjXYN+axcX1Ab7Oom2P+RgAtffDLj/A0a5vfkAbL/jWCwJHj9jT3afMzSQtQJYEhR6ibQ984+McsYQqg" +
            "m4baTBKMB6LHhDo/Aj8BInDcI6q0ePG/rgMx+57hkXnU+AnVGBxCWH3zq3ijclwI/tW3lC2jSVsWM4oN1O" +
            "SIc4XkjRGXjGEosylOUkUQ7AhhkBgSXYc1YvAksw4PG1kGWsAT5tOxbruOKbTnwIkSYxD1MbXsWAIUwMKz" +
            "eGUeDUbRwI9Fkek5CiwqAM3Bz6NUgdUt+vBslhIo8UM6kDQac4kDiicpHfe+FwY2SQI5q3oadvnoQ3hMHE" +
            "pCaHUgkqoVcRCG5aiKzCUCN03cUtJ4ikJxZTVlcWvDvarL626DiiVLH71pf0qG1y9H7mEPSQBNoTtQpFba" +
            "NzfDFfXSNJqPFJBkFb/1iiNLxhSAW3u4Ns7qHHi+i1F9fmyj1vV0sDIZonP0wh+waxjLr1vOPcmxORe7n3" +
            "pKOKIhVp9Rtb4+Owa3xCX/TpFPnrig6nKTNisNl8aNEKQRfQITh9kG/NhTzcvpwRZoARZvkh8S6h7Oz1zI" +
            "atZeuYWk5nvC4TJ2aFFJXBCTkcO9UuQQ0qb3FXdx4xTPH6dBeApP0CQ43QejN8kd7l64jI1krMVgJfPEf7" +
            "h3uq3o/K/ztZqP1QKFagz/G+t1XxwjeIFuqkRbXoTdlOTGnwCIoKZ6ku1AbrBoN6oCdX56w3UEOO0y2B9g" +
            "aLbAYWcAdpeweKa2IfIT2jz5QzXxD6AoP+DrdXtxeluV7pdWrvkcKqPp7rjS19d+wp/fff/5Ez3FPjzFNy" +
            "fdpTi9JB0sDp2JR7b309mn5HuPkEAAA==";

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
