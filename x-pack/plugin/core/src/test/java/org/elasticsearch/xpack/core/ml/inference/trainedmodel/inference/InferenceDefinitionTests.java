/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference;

import com.unboundid.util.Base64;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.inference.InferenceToXContentCompressor;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationFeatureImportance;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearningToRankConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.QueryExtractorBuilderTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.Tree;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeNode;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinitionTests.ENSEMBLE_MODEL;
import static org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinitionTests.TREE_MODEL;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.InferenceModelTestUtils.deserializeFromTrainedModel;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class InferenceDefinitionTests extends ESTestCase {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);
    }

    public void testEnsembleSchemaDeserialization() throws IOException {
        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry()), ENSEMBLE_MODEL);
        InferenceDefinition definition = InferenceDefinition.fromXContent(parser);
        assertThat(definition.getTrainedModel().getClass(), equalTo(EnsembleInferenceModel.class));
    }

    public void testTreeSchemaDeserialization() throws IOException {
        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry()), TREE_MODEL);
        InferenceDefinition definition = InferenceDefinition.fromXContent(parser);
        assertThat(definition.getTrainedModel().getClass(), equalTo(TreeInferenceModel.class));
    }

    public void testMultiClassIrisInference() throws IOException, ParseException {
        // Fairly simple, random forest classification model built to fit in our format
        // Trained on the well known Iris dataset
        String compressedDef = "H4sIAPbiMl4C/+1b246bMBD9lVWet8jjG3b/oN9QVYgmToLEkghIL6r23wukl90"
            + "YxRMGlt2WPKwEC/gYe2bOnBl+rOoyzQq3SR4OG5ev3t/9WLmicg+fc9cd1Gm5c3VSfz+2x6t1nlZVts3Wa"
            + "Z0ditX93Wrr0vpUuqRIH1zVXPJxVbljmie5K3b1vr3ifPw125wPj65+9u/z8fnfn+4vh0jy9LPLzw/+UGb"
            + "Vu8rVhyptb+wOv7iyytaH/FD+PZWVu6xo7u8e92x+3XOaSZVurtm1QydVXZ7W7XPPcIoGWpIVG/etOWbNR"
            + "Ru3zqp28r+B5bVrH5a7bZ2s91m+aU5Cc6LMdvu/Z3gL55hndfILdnNOtGPuS1ftD901LDKs+wFYziy3j/d"
            + "3FwjgKoJ0m3xJ81N7kvn3cix64aEH1gOfX8CXkVEtemFAahvz2IcgsBCkB0GhEMTKH1Ri3xn49yosYO0Bj"
            + "hErDpGy3Y9JLbjSRvoQNAF+jIVvPPi2Bz67gK8iK1v0ptmsWoHoWXFDQG+x9/IeQ8Hbqm+swBGT15dr1wM"
            + "CKDNA2yv0GKxE7b4+cwFBWDKQ+BlfDSgsat43tH94xD49diMtoeEVhgaN2mi6iwzMKqFjKUDPEBqCrmq6O"
            + "HHd0PViMreajEEFJxlaccAi4B4CgdhzHBHdOcFqCSYTI14g2WS2z0007DfAe4Hy7DdkrI2I+9yGIhitJhh"
            + "tTBjXYN+axcX1Ab7Oom2P+RgAtffDLj/A0a5vfkAbL/jWCwJHj9jT3afMzSQtQJYEhR6ibQ984+McsYQqg"
            + "m4baTBKMB6LHhDo/Aj8BInDcI6q0ePG/rgMx+57hkXnU+AnVGBxCWH3zq3ijclwI/tW3lC2jSVsWM4oN1O"
            + "SIc4XkjRGXjGEosylOUkUQ7AhhkBgSXYc1YvAksw4PG1kGWsAT5tOxbruOKbTnwIkSYxD1MbXsWAIUwMKz"
            + "eGUeDUbRwI9Fkek5CiwqAM3Bz6NUgdUt+vBslhIo8UM6kDQac4kDiicpHfe+FwY2SQI5q3oadvnoQ3hMHE"
            + "pCaHUgkqoVcRCG5aiKzCUCN03cUtJ4ikJxZTVlcWvDvarL626DiiVLH71pf0qG1y9H7mEPSQBNoTtQpFba"
            + "NzfDFfXSNJqPFJBkFb/1iiNLxhSAW3u4Ns7qHHi+i1F9fmyj1vV0sDIZonP0wh+waxjLr1vOPcmxORe7n3"
            + "pKOKIhVp9Rtb4+Owa3xCX/TpFPnrig6nKTNisNl8aNEKQRfQITh9kG/NhTzcvpwRZoARZvkh8S6h7Oz1zI"
            + "atZeuYWk5nvC4TJ2aFFJXBCTkcO9UuQQ0qb3FXdx4xTPH6dBeApP0CQ43QejN8kd7l64jI1krMVgJfPEf7"
            + "h3uq3o/K/ztZqP1QKFagz/G+t1XxwjeIFuqkRbXoTdlOTGnwCIoKZ6ku1AbrBoN6oCdX56w3UEOO0y2B9g"
            + "aLbAYWcAdpeweKa2IfIT2jz5QzXxD6AoP+DrdXtxeluV7pdWrvkcKqPp7rjS19d+wp/fff/5Ez3FPjzFNy"
            + "fdpTi9JB0sDp2JR7b309mn5HuPkEAAA==";

        byte[] bytes = Base64.decode(compressedDef);
        InferenceDefinition definition = InferenceToXContentCompressor.inflate(
            new BytesArray(bytes),
            InferenceDefinition::fromXContent,
            xContentRegistry()
        );

        Map<String, Object> fields = Map.of("sepal_length", 5.1, "sepal_width", 3.5, "petal_length", 1.4, "petal_width", 0.2);

        assertThat(
            ((ClassificationInferenceResults) definition.infer(fields, ClassificationConfig.EMPTY_PARAMS)).getClassificationLabel(),
            equalTo("Iris-setosa")
        );

        fields = Map.of("sepal_length", 7.0, "sepal_width", 3.2, "petal_length", 4.7, "petal_width", 1.4);
        assertThat(
            ((ClassificationInferenceResults) definition.infer(fields, ClassificationConfig.EMPTY_PARAMS)).getClassificationLabel(),
            equalTo("Iris-versicolor")
        );

        fields = Map.of("sepal_length", 6.5, "sepal_width", 3.0, "petal_length", 5.2, "petal_width", 2.0);
        assertThat(
            ((ClassificationInferenceResults) definition.infer(fields, ClassificationConfig.EMPTY_PARAMS)).getClassificationLabel(),
            equalTo("Iris-virginica")
        );
    }

    public void testComplexInferenceDefinitionInfer() throws IOException {
        XContentParser parser = XContentHelper.createParser(
            XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry()),
            new BytesArray(getClassificationDefinition(false)),
            XContentType.JSON
        );
        InferenceDefinition inferenceDefinition = InferenceDefinition.fromXContent(parser);

        ClassificationConfig config = new ClassificationConfig(2, null, null, 2, null);
        Map<String, Object> featureMap = new HashMap<>();
        featureMap.put("col1", "female");
        featureMap.put("col2", "M");
        featureMap.put("col3", "none");
        featureMap.put("col4", 10);

        ClassificationInferenceResults results = (ClassificationInferenceResults) inferenceDefinition.infer(featureMap, config);
        assertThat(results.valueAsString(), equalTo("second"));
        assertThat(results.getFeatureImportance().get(0).getFeatureName(), equalTo("col2"));
        assertThat(results.getFeatureImportance().get(0).getTotalImportance(), closeTo(0.944, 0.001));
        assertThat(results.getFeatureImportance().get(1).getFeatureName(), equalTo("col1"));
        assertThat(results.getFeatureImportance().get(1).getTotalImportance(), closeTo(0.199, 0.001));
    }

    public void testComplexInferenceDefinitionInferWithCustomPreProcessor() throws IOException {
        XContentParser parser = XContentHelper.createParser(
            XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry()),
            new BytesArray(getClassificationDefinition(true)),
            XContentType.JSON
        );
        InferenceDefinition inferenceDefinition = InferenceDefinition.fromXContent(parser);

        ClassificationConfig config = new ClassificationConfig(2, null, null, 2, null);
        Map<String, Object> featureMap = new HashMap<>();
        featureMap.put("col1", "female");
        featureMap.put("col2", "M");
        featureMap.put("col3", "none");
        featureMap.put("col4", 10);

        ClassificationInferenceResults results = (ClassificationInferenceResults) inferenceDefinition.infer(featureMap, config);
        assertThat(results.valueAsString(), equalTo("second"));
        ClassificationFeatureImportance featureImportance1 = results.getFeatureImportance().get(0);
        assertThat(featureImportance1.getFeatureName(), equalTo("col2"));
        assertThat(featureImportance1.getTotalImportance(), closeTo(0.944, 0.001));
        for (ClassificationFeatureImportance.ClassImportance classImportance : featureImportance1.getClassImportance()) {
            if (classImportance.getClassName().equals("second")) {
                assertThat(classImportance.getImportance(), closeTo(0.944, 0.001));
            } else {
                assertThat(classImportance.getImportance(), closeTo(-0.944, 0.001));
            }
        }
        ClassificationFeatureImportance featureImportance2 = results.getFeatureImportance().get(1);
        assertThat(featureImportance2.getFeatureName(), equalTo("col1_male"));
        assertThat(featureImportance2.getTotalImportance(), closeTo(0.199, 0.001));
        for (ClassificationFeatureImportance.ClassImportance classImportance : featureImportance2.getClassImportance()) {
            if (classImportance.getClassName().equals("second")) {
                assertThat(classImportance.getImportance(), closeTo(0.199, 0.001));
            } else {
                assertThat(classImportance.getImportance(), closeTo(-0.199, 0.001));
            }
        }
    }

    public void testWithLearningToRankConfiguration() throws IOException {
        Tree.Builder builder = Tree.builder().setTargetType(TargetType.REGRESSION);
        TreeNode.Builder rootNode = builder.addJunction(0, 0, true, 0.5);
        builder.addLeaf(rootNode.getRightChild(), -2.0);
        TreeNode.Builder leftChildNode = builder.addJunction(rootNode.getLeftChild(), 1, true, 0.8);
        builder.addLeaf(leftChildNode.getLeftChild(), 0.2);
        builder.addLeaf(leftChildNode.getRightChild(), 1.5);

        List<String> featureNames = Arrays.asList("foo", "bar");
        Tree treeObject = builder.setFeatureNames(featureNames).build();
        TreeInferenceModel tree = deserializeFromTrainedModel(treeObject, xContentRegistry(), TreeInferenceModel::fromXContent);
        tree.rewriteFeatureIndices(Collections.emptyMap());

        BoundedWindowInferenceModel testModel = new BoundedWindowInferenceModel(tree);

        InferenceDefinition definition = new InferenceDefinition(testModel, null);
        LearningToRankConfig config = new LearningToRankConfig(
            randomBoolean() ? null : randomIntBetween(0, 10),
            randomBoolean()
                ? null
                : Stream.generate(QueryExtractorBuilderTests::randomInstance).limit(randomInt(5)).collect(Collectors.toList()),
            randomBoolean() ? null : randomMap(0, 10, () -> Tuple.tuple(randomIdentifier(), randomIdentifier()))
        );

        InferenceResults results = definition.infer(Map.of("foo", 1.0, "bar", 0.0), config);

        assertThat(results.predictedValue(), equalTo(2.0));
    }

    public static String getClassificationDefinition(boolean customPreprocessor) {
        return Strings.format("""
            {
                "preprocessors": [
                    {
                        "one_hot_encoding": {
                            "field": "col1",
                            "custom": %s,
                            "hot_map": {
                                "male": "col1_male",
                                "female": "col1_female"
                            }
                        }
                    },
                    {
                        "target_mean_encoding": {
                            "field": "col2",
                            "feature_name": "col2_encoded",
                            "target_map": {
                                "S": 5.0,
                                "M": 10.0,
                                "L": 20
                            },
                            "default_value": 5.0
                        }
                    },
                    {
                        "frequency_encoding": {
                            "field": "col3",
                            "feature_name": "col3_encoded",
                            "frequency_map": {
                                "none": 0.75,
                                "true": 0.10,
                                "false": 0.15
                            }
                        }
                    }
                ],
                "trained_model": {
                    "ensemble": {
                        "feature_names": [
                            "col1_male",
                            "col1_female",
                            "col2_encoded",
                            "col3_encoded",
                            "col4"
                        ],
                        "aggregate_output": {
                            "weighted_mode": {
                                "num_classes": "2",
                                "weights": [
                                    0.5,
                                    0.5
                                ]
                            }
                        },
                        "target_type": "classification",
                        "classification_labels": [
                            "first",
                            "second"
                        ],
                        "trained_models": [
                            {
                                "tree": {
                                    "feature_names": [
                                        "col1_male",
                                        "col1_female",
                                        "col4"
                                    ],
                                    "tree_structure": [
                                        {
                                            "node_index": 0,
                                            "split_feature": 0,
                                            "number_samples": 100,
                                            "split_gain": 12.0,
                                            "threshold": 10.0,
                                            "decision_type": "lte",
                                            "default_left": true,
                                            "left_child": 1,
                                            "right_child": 2
                                        },
                                        {
                                            "node_index": 1,
                                            "number_samples": 80,
                                            "leaf_value": 1
                                        },
                                        {
                                            "node_index": 2,
                                            "number_samples": 20,
                                            "leaf_value": 0
                                        }
                                    ],
                                    "target_type": "regression"
                                }
                            },
                            {
                                "tree": {
                                    "feature_names": [
                                        "col2_encoded",
                                        "col3_encoded",
                                        "col4"
                                    ],
                                    "tree_structure": [
                                        {
                                            "node_index": 0,
                                            "split_feature": 0,
                                            "split_gain": 12.0,
                                            "number_samples": 180,
                                            "threshold": 10.0,
                                            "decision_type": "lte",
                                            "default_left": true,
                                            "left_child": 1,
                                            "right_child": 2
                                        },
                                        {
                                            "node_index": 1,
                                            "number_samples": 10,
                                            "leaf_value": 1
                                        },
                                        {
                                            "node_index": 2,
                                            "number_samples": 170,
                                            "leaf_value": 0
                                        }
                                    ],
                                    "target_type": "regression"
                                }
                            }
                        ]
                    }
                }
            }""", customPreprocessor);
    }
}
