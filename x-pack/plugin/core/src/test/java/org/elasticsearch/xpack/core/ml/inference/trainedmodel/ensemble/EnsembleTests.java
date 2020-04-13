/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.SingleValueInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.Tree;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeNode;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeTests;
import org.elasticsearch.xpack.core.ml.job.config.Operator;
import org.junit.Before;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class EnsembleTests extends AbstractSerializingTestCase<Ensemble> {
    private final double eps = 1.0E-8;

    private boolean lenient;

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> !field.isEmpty();
    }

    @Override
    protected Ensemble doParseInstance(XContentParser parser) throws IOException {
        return lenient ? Ensemble.fromXContentLenient(parser) : Ensemble.fromXContentStrict(parser);
    }

    public static Ensemble createRandom() {
        return createRandom(randomFrom(TargetType.values()));
    }

    public static Ensemble createRandom(TargetType targetType) {
        int numberOfFeatures = randomIntBetween(1, 10);
        List<String> featureNames = Stream.generate(() -> randomAlphaOfLength(10)).limit(numberOfFeatures).collect(Collectors.toList());
        int numberOfModels = randomIntBetween(1, 10);
        List<TrainedModel> models = Stream.generate(() -> TreeTests.buildRandomTree(featureNames, 6))
            .limit(numberOfModels)
            .collect(Collectors.toList());
        double[] weights = randomBoolean() ?
            null :
            Stream.generate(ESTestCase::randomDouble).limit(numberOfModels).mapToDouble(Double::valueOf).toArray();
        List<String> categoryLabels = null;
        if (randomBoolean() && targetType == TargetType.CLASSIFICATION) {
            categoryLabels = randomList(2, randomIntBetween(3, 10), () -> randomAlphaOfLength(10));
        }

        OutputAggregator outputAggregator = targetType == TargetType.REGRESSION ? new WeightedSum(weights) :
            randomFrom(
                new WeightedMode(
                    weights,
                    categoryLabels != null ? categoryLabels.size() : randomIntBetween(2, 10)),
                new LogisticRegression(weights));

        double[] thresholds = randomBoolean() && targetType == TargetType.CLASSIFICATION ?
            Stream.generate(ESTestCase::randomDouble)
                .limit(categoryLabels == null ? randomIntBetween(1, 10) : categoryLabels.size())
                .mapToDouble(Double::valueOf)
                .toArray() :
            null;

        return new Ensemble(randomBoolean() ? featureNames : Collections.emptyList(),
            models,
            outputAggregator,
            targetType,
            categoryLabels,
            thresholds);
    }

    @Override
    protected Ensemble createTestInstance() {
        return createRandom();
    }

    @Override
    protected Writeable.Reader<Ensemble> instanceReader() {
        return Ensemble::new;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new MlInferenceNamedXContentProvider().getNamedWriteables());
    }

    public void testEnsembleWithAggregatedOutputDifferingFromTrainedModels() {
        List<String> featureNames = Arrays.asList("foo", "bar");
        int numberOfModels = 5;
        double[] weights = new double[numberOfModels + 2];
        for (int i = 0; i < numberOfModels + 2; i++) {
            weights[i] = randomDouble();
        }
        OutputAggregator outputAggregator = new WeightedSum(weights);

        List<TrainedModel> models = new ArrayList<>(numberOfModels);
        for (int i = 0; i < numberOfModels; i++) {
            models.add(TreeTests.buildRandomTree(featureNames, 6));
        }
        ElasticsearchException ex = expectThrows(ElasticsearchException.class, () -> {
            Ensemble.builder()
                .setTrainedModels(models)
                .setOutputAggregator(outputAggregator)
                .setFeatureNames(featureNames)
                .build()
                .validate();
        });
        assertThat(ex.getMessage(), equalTo("[aggregate_output] expects value array of size [7] but number of models is [5]"));
    }

    public void testEnsembleWithInvalidModel() {
        List<String> featureNames = Arrays.asList("foo", "bar");
        expectThrows(ElasticsearchException.class, () -> {
            Ensemble.builder()
                .setFeatureNames(featureNames)
                .setTrainedModels(Arrays.asList(
                // Tree with loop
                Tree.builder()
                    .setNodes(TreeNode.builder(0)
                    .setLeftChild(1)
                    .setSplitFeature(1)
                    .setThreshold(randomDouble()),
                TreeNode.builder(0)
                    .setLeftChild(0)
                    .setSplitFeature(1)
                    .setThreshold(randomDouble()))
                    .setFeatureNames(featureNames)
                    .build()))
                .build()
                .validate();
        });
    }

    public void testEnsembleWithAggregatorOutputNotSupportingTargetType() {
        List<String> featureNames = Arrays.asList("foo", "bar");
        ElasticsearchException ex = expectThrows(ElasticsearchException.class, () -> {
            Ensemble.builder()
                .setFeatureNames(featureNames)
                .setTrainedModels(Arrays.asList(
                    Tree.builder()
                        .setNodes(TreeNode.builder(0)
                            .setLeftChild(1)
                            .setSplitFeature(1)
                            .setThreshold(randomDouble()))
                        .setFeatureNames(featureNames)
                        .build()))
                .setClassificationLabels(Arrays.asList("label1", "label2"))
                .setTargetType(TargetType.CLASSIFICATION)
                .setOutputAggregator(new WeightedSum())
                .build()
                .validate();
        });
    }

    public void testEnsembleWithTargetTypeAndLabelsMismatch() {
        List<String> featureNames = Arrays.asList("foo", "bar");
        String msg = "[target_type] should be [classification] if " +
            "[classification_labels] or [classification_weights] are provided";
        ElasticsearchException ex = expectThrows(ElasticsearchException.class, () -> {
            Ensemble.builder()
                .setFeatureNames(featureNames)
                .setTrainedModels(Arrays.asList(
                    Tree.builder()
                        .setNodes(TreeNode.builder(0)
                                .setLeafValue(randomDouble()))
                        .setFeatureNames(featureNames)
                        .build()))
                .setClassificationLabels(Arrays.asList("label1", "label2"))
                .build()
                .validate();
        });
        assertThat(ex.getMessage(), equalTo(msg));
    }

    public void testEnsembleWithEmptyModels() {
        List<String> featureNames = Arrays.asList("foo", "bar");
        ElasticsearchException ex = expectThrows(ElasticsearchException.class, () -> {
            Ensemble.builder().setTrainedModels(Collections.emptyList()).setFeatureNames(featureNames).build().validate();
        });
        assertThat(ex.getMessage(), equalTo("[trained_models] must not be empty"));
    }

    public void testClassificationProbability() {
        List<String> featureNames = Arrays.asList("foo", "bar");
        Tree tree1 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0)
                .setLeftChild(1)
                .setRightChild(2)
                .setSplitFeature(0)
                .setThreshold(0.5))
            .addNode(TreeNode.builder(1).setLeafValue(1.0))
            .addNode(TreeNode.builder(2)
                .setThreshold(0.8)
                .setSplitFeature(1)
                .setLeftChild(3)
                .setRightChild(4))
            .addNode(TreeNode.builder(3).setLeafValue(0.0))
            .addNode(TreeNode.builder(4).setLeafValue(1.0)).build();
        Tree tree2 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0)
                .setLeftChild(1)
                .setRightChild(2)
                .setSplitFeature(0)
                .setThreshold(0.5))
            .addNode(TreeNode.builder(1).setLeafValue(0.0))
            .addNode(TreeNode.builder(2).setLeafValue(1.0))
            .build();
        Tree tree3 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0)
                .setLeftChild(1)
                .setRightChild(2)
                .setSplitFeature(1)
                .setThreshold(1.0))
            .addNode(TreeNode.builder(1).setLeafValue(1.0))
            .addNode(TreeNode.builder(2).setLeafValue(0.0))
            .build();
        Ensemble ensemble = Ensemble.builder()
            .setTargetType(TargetType.CLASSIFICATION)
            .setFeatureNames(featureNames)
            .setTrainedModels(Arrays.asList(tree1, tree2, tree3))
            .setOutputAggregator(new WeightedMode(new double[]{0.7, 0.5, 1.0}, 2))
            .setClassificationWeights(Arrays.asList(0.7, 0.3))
            .build();

        List<Double> featureVector = Arrays.asList(0.4, 0.0);
        Map<String, Object> featureMap = zipObjMap(featureNames, featureVector);
        List<Double> expected = Arrays.asList(0.768524783, 0.231475216);
        List<Double> scores   = Arrays.asList(0.230557435, 0.162032651);
        double eps = 0.000001;
        List<ClassificationInferenceResults.TopClassEntry> probabilities =
            ((ClassificationInferenceResults)ensemble.infer(featureMap, new ClassificationConfig(2), Collections.emptyMap()))
                .getTopClasses();
        for(int i = 0; i < expected.size(); i++) {
            assertThat(probabilities.get(i).getProbability(), closeTo(expected.get(i), eps));
            assertThat(probabilities.get(i).getScore(), closeTo(scores.get(i), eps));
        }

        featureVector = Arrays.asList(2.0, 0.7);
        featureMap = zipObjMap(featureNames, featureVector);
        expected = Arrays.asList(0.310025518, 0.6899744811);
        scores   = Arrays.asList(0.217017863, 0.2069923443);
        probabilities =
            ((ClassificationInferenceResults)ensemble.infer(featureMap, new ClassificationConfig(2), Collections.emptyMap()))
                .getTopClasses();
        for(int i = 0; i < expected.size(); i++) {
            assertThat(probabilities.get(i).getProbability(), closeTo(expected.get(i), eps));
            assertThat(probabilities.get(i).getScore(), closeTo(scores.get(i), eps));
        }

        featureVector = Arrays.asList(0.0, 1.0);
        featureMap = zipObjMap(featureNames, featureVector);
        expected = Arrays.asList(0.768524783, 0.231475216);
        scores   = Arrays.asList(0.230557435, 0.162032651);
        probabilities =
            ((ClassificationInferenceResults)ensemble.infer(featureMap, new ClassificationConfig(2), Collections.emptyMap()))
                .getTopClasses();
        for(int i = 0; i < expected.size(); i++) {
            assertThat(probabilities.get(i).getProbability(), closeTo(expected.get(i), eps));
            assertThat(probabilities.get(i).getScore(), closeTo(scores.get(i), eps));
        }

        // This should handle missing values and take the default_left path
        featureMap = new HashMap<>(2) {{
            put("foo", 0.3);
            put("bar", null);
        }};
        expected = Arrays.asList(0.6899744811, 0.3100255188);
        scores   = Arrays.asList(0.482982136, 0.0930076556);
        probabilities =
            ((ClassificationInferenceResults)ensemble.infer(featureMap, new ClassificationConfig(2), Collections.emptyMap()))
                .getTopClasses();
        for(int i = 0; i < expected.size(); i++) {
            assertThat(probabilities.get(i).getProbability(), closeTo(expected.get(i), eps));
            assertThat(probabilities.get(i).getScore(), closeTo(scores.get(i), eps));
        }
    }

    public void testClassificationInference() {
        List<String> featureNames = Arrays.asList("foo", "bar");
        Tree tree1 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0)
                .setLeftChild(1)
                .setRightChild(2)
                .setSplitFeature(0)
                .setThreshold(0.5))
            .addNode(TreeNode.builder(1).setLeafValue(1.0))
            .addNode(TreeNode.builder(2)
                .setThreshold(0.8)
                .setSplitFeature(1)
                .setLeftChild(3)
                .setRightChild(4))
            .addNode(TreeNode.builder(3).setLeafValue(0.0))
            .addNode(TreeNode.builder(4).setLeafValue(1.0))
            .setTargetType(randomFrom(TargetType.CLASSIFICATION, TargetType.REGRESSION))
            .build();
        Tree tree2 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0)
                .setLeftChild(1)
                .setRightChild(2)
                .setSplitFeature(0)
                .setThreshold(0.5))
            .addNode(TreeNode.builder(1).setLeafValue(0.0))
            .addNode(TreeNode.builder(2).setLeafValue(1.0))
            .setTargetType(randomFrom(TargetType.CLASSIFICATION, TargetType.REGRESSION))
            .build();
        Tree tree3 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0)
                .setLeftChild(1)
                .setRightChild(2)
                .setSplitFeature(1)
                .setThreshold(1.0))
            .addNode(TreeNode.builder(1).setLeafValue(1.0))
            .addNode(TreeNode.builder(2).setLeafValue(0.0))
            .setTargetType(randomFrom(TargetType.CLASSIFICATION, TargetType.REGRESSION))
            .build();
        Ensemble ensemble = Ensemble.builder()
            .setTargetType(TargetType.CLASSIFICATION)
            .setFeatureNames(featureNames)
            .setTrainedModels(Arrays.asList(tree1, tree2, tree3))
            .setOutputAggregator(new WeightedMode(new double[]{0.7, 0.5, 1.0}, 2))
            .build();

        List<Double> featureVector = Arrays.asList(0.4, 0.0);
        Map<String, Object> featureMap = zipObjMap(featureNames, featureVector);
        assertThat(1.0,
            closeTo(((SingleValueInferenceResults)ensemble.infer(featureMap, new ClassificationConfig(0), Collections.emptyMap())).value(),
                0.00001));

        featureVector = Arrays.asList(2.0, 0.7);
        featureMap = zipObjMap(featureNames, featureVector);
        assertThat(1.0,
            closeTo(((SingleValueInferenceResults)ensemble.infer(featureMap, new ClassificationConfig(0), Collections.emptyMap())).value(),
                0.00001));

        featureVector = Arrays.asList(0.0, 1.0);
        featureMap = zipObjMap(featureNames, featureVector);
        assertThat(1.0,
            closeTo(((SingleValueInferenceResults)ensemble.infer(featureMap, new ClassificationConfig(0), Collections.emptyMap())).value(),
                0.00001));

        featureMap = new HashMap<>(2) {{
            put("foo", 0.3);
            put("bar", null);
        }};
        assertThat(0.0,
            closeTo(((SingleValueInferenceResults)ensemble.infer(featureMap, new ClassificationConfig(0), Collections.emptyMap())).value(),
                0.00001));
    }

    public void testMultiClassClassificationInference() {
        List<String> featureNames = Arrays.asList("foo", "bar");
        Tree tree1 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0)
                .setLeftChild(1)
                .setRightChild(2)
                .setSplitFeature(0)
                .setThreshold(0.5))
            .addNode(TreeNode.builder(1).setLeafValue(2.0))
            .addNode(TreeNode.builder(2)
                .setThreshold(0.8)
                .setSplitFeature(1)
                .setLeftChild(3)
                .setRightChild(4))
            .addNode(TreeNode.builder(3).setLeafValue(0.0))
            .addNode(TreeNode.builder(4).setLeafValue(1.0))
            .setTargetType(randomFrom(TargetType.CLASSIFICATION, TargetType.REGRESSION))
            .build();
        Tree tree2 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0)
                .setLeftChild(1)
                .setRightChild(2)
                .setSplitFeature(1)
                .setThreshold(0.5))
            .addNode(TreeNode.builder(1).setLeafValue(2.0))
            .addNode(TreeNode.builder(2).setLeafValue(1.0))
            .setTargetType(randomFrom(TargetType.CLASSIFICATION, TargetType.REGRESSION))
            .build();
        Tree tree3 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0)
                .setLeftChild(1)
                .setRightChild(2)
                .setSplitFeature(1)
                .setThreshold(2.0))
            .addNode(TreeNode.builder(1).setLeafValue(1.0))
            .addNode(TreeNode.builder(2).setLeafValue(0.0))
            .setTargetType(randomFrom(TargetType.CLASSIFICATION, TargetType.REGRESSION))
            .build();
        Ensemble ensemble = Ensemble.builder()
            .setTargetType(TargetType.CLASSIFICATION)
            .setFeatureNames(featureNames)
            .setTrainedModels(Arrays.asList(tree1, tree2, tree3))
            .setOutputAggregator(new WeightedMode(new double[]{0.7, 0.5, 1.0}, 3))
            .build();

        List<Double> featureVector = Arrays.asList(0.4, 0.0);
        Map<String, Object> featureMap = zipObjMap(featureNames, featureVector);
        assertThat(2.0,
            closeTo(((SingleValueInferenceResults)ensemble.infer(featureMap, new ClassificationConfig(0), Collections.emptyMap())).value(),
                0.00001));

        featureVector = Arrays.asList(2.0, 0.7);
        featureMap = zipObjMap(featureNames, featureVector);
        assertThat(1.0,
            closeTo(((SingleValueInferenceResults)ensemble.infer(featureMap, new ClassificationConfig(0), Collections.emptyMap())).value(),
                0.00001));

        featureVector = Arrays.asList(0.0, 1.0);
        featureMap = zipObjMap(featureNames, featureVector);
        assertThat(1.0,
            closeTo(((SingleValueInferenceResults)ensemble.infer(featureMap, new ClassificationConfig(0), Collections.emptyMap())).value(),
                0.00001));

        featureMap = new HashMap<>(2) {{
            put("foo", 0.6);
            put("bar", null);
        }};
        assertThat(1.0,
            closeTo(((SingleValueInferenceResults)ensemble.infer(featureMap, new ClassificationConfig(0), Collections.emptyMap())).value(),
                0.00001));
    }

    public void testRegressionInference() {
        List<String> featureNames = Arrays.asList("foo", "bar");
        Tree tree1 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0)
                .setLeftChild(1)
                .setRightChild(2)
                .setSplitFeature(0)
                .setThreshold(0.5))
            .addNode(TreeNode.builder(1).setLeafValue(0.3))
            .addNode(TreeNode.builder(2)
                .setThreshold(0.8)
                .setSplitFeature(1)
                .setLeftChild(3)
                .setRightChild(4))
            .addNode(TreeNode.builder(3).setLeafValue(0.1))
            .addNode(TreeNode.builder(4).setLeafValue(0.2)).build();
        Tree tree2 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0)
                .setLeftChild(1)
                .setRightChild(2)
                .setSplitFeature(0)
                .setThreshold(0.5))
            .addNode(TreeNode.builder(1).setLeafValue(1.5))
            .addNode(TreeNode.builder(2).setLeafValue(0.9))
            .build();
        Ensemble ensemble = Ensemble.builder()
            .setTargetType(TargetType.REGRESSION)
            .setFeatureNames(featureNames)
            .setTrainedModels(Arrays.asList(tree1, tree2))
            .setOutputAggregator(new WeightedSum(new double[]{0.5, 0.5}))
            .build();

        List<Double> featureVector = Arrays.asList(0.4, 0.0);
        Map<String, Object> featureMap = zipObjMap(featureNames, featureVector);
        assertThat(0.9,
            closeTo(((SingleValueInferenceResults)ensemble.infer(featureMap, RegressionConfig.EMPTY_PARAMS, Collections.emptyMap()))
                .value(),
                0.00001));

        featureVector = Arrays.asList(2.0, 0.7);
        featureMap = zipObjMap(featureNames, featureVector);
        assertThat(0.5,
            closeTo(((SingleValueInferenceResults)ensemble.infer(featureMap, RegressionConfig.EMPTY_PARAMS, Collections.emptyMap()))
                .value(),
                0.00001));

        // Test with NO aggregator supplied, verifies default behavior of non-weighted sum
        ensemble = Ensemble.builder()
            .setTargetType(TargetType.REGRESSION)
            .setFeatureNames(featureNames)
            .setTrainedModels(Arrays.asList(tree1, tree2))
            .build();

        featureVector = Arrays.asList(0.4, 0.0);
        featureMap = zipObjMap(featureNames, featureVector);
        assertThat(1.8,
            closeTo(((SingleValueInferenceResults)ensemble.infer(featureMap, RegressionConfig.EMPTY_PARAMS, Collections.emptyMap()))
                .value(),
                0.00001));

        featureVector = Arrays.asList(2.0, 0.7);
        featureMap = zipObjMap(featureNames, featureVector);
        assertThat(1.0,
            closeTo(((SingleValueInferenceResults)ensemble.infer(featureMap, RegressionConfig.EMPTY_PARAMS, Collections.emptyMap()))
                .value(),
                0.00001));

        featureMap = new HashMap<>(2) {{
            put("foo", 0.3);
            put("bar", null);
        }};
        assertThat(1.8,
            closeTo(((SingleValueInferenceResults)ensemble.infer(featureMap, RegressionConfig.EMPTY_PARAMS, Collections.emptyMap()))
                .value(),
                0.00001));
    }

    public void testInferNestedFields() {
        List<String> featureNames = Arrays.asList("foo.baz", "bar.biz");
        Tree tree1 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0)
                .setLeftChild(1)
                .setRightChild(2)
                .setSplitFeature(0)
                .setThreshold(0.5))
            .addNode(TreeNode.builder(1).setLeafValue(0.3))
            .addNode(TreeNode.builder(2)
                .setThreshold(0.8)
                .setSplitFeature(1)
                .setLeftChild(3)
                .setRightChild(4))
            .addNode(TreeNode.builder(3).setLeafValue(0.1))
            .addNode(TreeNode.builder(4).setLeafValue(0.2)).build();
        Tree tree2 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0)
                .setLeftChild(1)
                .setRightChild(2)
                .setSplitFeature(0)
                .setThreshold(0.5))
            .addNode(TreeNode.builder(1).setLeafValue(1.5))
            .addNode(TreeNode.builder(2).setLeafValue(0.9))
            .build();
        Ensemble ensemble = Ensemble.builder()
            .setTargetType(TargetType.REGRESSION)
            .setFeatureNames(featureNames)
            .setTrainedModels(Arrays.asList(tree1, tree2))
            .setOutputAggregator(new WeightedSum(new double[]{0.5, 0.5}))
            .build();

        Map<String, Object> featureMap = new HashMap<>() {{
            put("foo", new HashMap<>(){{
                put("baz", 0.4);
            }});
            put("bar", new HashMap<>(){{
                put("biz", 0.0);
            }});
        }};
        assertThat(0.9,
            closeTo(((SingleValueInferenceResults)ensemble.infer(featureMap, RegressionConfig.EMPTY_PARAMS, Collections.emptyMap()))
                .value(),
                0.00001));

        featureMap = new HashMap<>() {{
            put("foo", new HashMap<>(){{
                put("baz", 2.0);
            }});
            put("bar", new HashMap<>(){{
                put("biz", 0.7);
            }});
        }};
        assertThat(0.5,
            closeTo(((SingleValueInferenceResults)ensemble.infer(featureMap, RegressionConfig.EMPTY_PARAMS, Collections.emptyMap()))
                .value(),
                0.00001));
    }

    public void testOperationsEstimations() {
        Tree tree1 = TreeTests.buildRandomTree(Arrays.asList("foo", "bar"), 2);
        Tree tree2 = TreeTests.buildRandomTree(Arrays.asList("foo", "bar", "baz"), 5);
        Tree tree3 = TreeTests.buildRandomTree(Arrays.asList("foo", "baz"), 3);
        Ensemble ensemble = Ensemble.builder().setTrainedModels(Arrays.asList(tree1, tree2, tree3))
            .setTargetType(TargetType.CLASSIFICATION)
            .setFeatureNames(Arrays.asList("foo", "bar", "baz"))
            .setOutputAggregator(new LogisticRegression(new double[]{0.1, 0.4, 1.0}))
            .build();
        assertThat(ensemble.estimatedNumOperations(), equalTo(9L));
    }

    public void testFeatureImportance() {
        List<String> featureNames = Arrays.asList("foo", "bar");
        Tree tree1 = Tree.builder()
            .setFeatureNames(featureNames)
            .setNodes(
                TreeNode.builder(0)
                    .setSplitFeature(0)
                    .setOperator(Operator.LT)
                    .setLeftChild(1)
                    .setRightChild(2)
                    .setThreshold(0.55)
                    .setNumberSamples(10L),
                TreeNode.builder(1)
                    .setSplitFeature(0)
                    .setLeftChild(3)
                    .setRightChild(4)
                    .setOperator(Operator.LT)
                    .setThreshold(0.41)
                    .setNumberSamples(6L),
                TreeNode.builder(2)
                    .setSplitFeature(1)
                    .setLeftChild(5)
                    .setRightChild(6)
                    .setOperator(Operator.LT)
                    .setThreshold(0.25)
                    .setNumberSamples(4L),
                TreeNode.builder(3).setLeafValue(1.18230136).setNumberSamples(5L),
                TreeNode.builder(4).setLeafValue(1.98006658).setNumberSamples(1L),
                TreeNode.builder(5).setLeafValue(3.25350885).setNumberSamples(3L),
                TreeNode.builder(6).setLeafValue(2.42384369).setNumberSamples(1L)).build();

        Tree tree2 = Tree.builder()
            .setFeatureNames(featureNames)
            .setNodes(
                TreeNode.builder(0)
                    .setSplitFeature(0)
                    .setOperator(Operator.LT)
                    .setLeftChild(1)
                    .setRightChild(2)
                    .setThreshold(0.45)
                    .setNumberSamples(10L),
                TreeNode.builder(1)
                    .setSplitFeature(0)
                    .setLeftChild(3)
                    .setRightChild(4)
                    .setOperator(Operator.LT)
                    .setThreshold(0.25)
                    .setNumberSamples(5L),
                TreeNode.builder(2)
                    .setSplitFeature(0)
                    .setLeftChild(5)
                    .setRightChild(6)
                    .setOperator(Operator.LT)
                    .setThreshold(0.59)
                    .setNumberSamples(5L),
                TreeNode.builder(3).setLeafValue(1.04476388).setNumberSamples(3L),
                TreeNode.builder(4).setLeafValue(1.52799228).setNumberSamples(2L),
                TreeNode.builder(5).setLeafValue(1.98006658).setNumberSamples(1L),
                TreeNode.builder(6).setLeafValue(2.950216).setNumberSamples(4L)).build();

        Ensemble ensemble = Ensemble.builder().setOutputAggregator(new WeightedSum())
            .setTrainedModels(Arrays.asList(tree1, tree2))
            .setFeatureNames(featureNames)
            .build();


        Map<String, double[]> featureImportance = ensemble.featureImportance(zipObjMap(featureNames, Arrays.asList(0.0, 0.9)));
        assertThat(featureImportance.get("foo")[0], closeTo(-1.653200025, eps));
        assertThat(featureImportance.get("bar")[0], closeTo( -0.12444978, eps));

        featureImportance = ensemble.featureImportance(zipObjMap(featureNames, Arrays.asList(0.1, 0.8)));
        assertThat(featureImportance.get("foo")[0], closeTo(-1.653200025, eps));
        assertThat(featureImportance.get("bar")[0], closeTo( -0.12444978, eps));

        featureImportance = ensemble.featureImportance(zipObjMap(featureNames, Arrays.asList(0.2, 0.7)));
        assertThat(featureImportance.get("foo")[0], closeTo(-1.653200025, eps));
        assertThat(featureImportance.get("bar")[0], closeTo( -0.12444978, eps));

        featureImportance = ensemble.featureImportance(zipObjMap(featureNames, Arrays.asList(0.3, 0.6)));
        assertThat(featureImportance.get("foo")[0], closeTo(-1.16997162, eps));
        assertThat(featureImportance.get("bar")[0], closeTo( -0.12444978, eps));

        featureImportance = ensemble.featureImportance(zipObjMap(featureNames, Arrays.asList(0.4, 0.5)));
        assertThat(featureImportance.get("foo")[0], closeTo(-1.16997162, eps));
        assertThat(featureImportance.get("bar")[0], closeTo( -0.12444978, eps));

        featureImportance = ensemble.featureImportance(zipObjMap(featureNames, Arrays.asList(0.5, 0.4)));
        assertThat(featureImportance.get("foo")[0], closeTo(0.0798679, eps));
        assertThat(featureImportance.get("bar")[0], closeTo( -0.12444978, eps));

        featureImportance = ensemble.featureImportance(zipObjMap(featureNames, Arrays.asList(0.6, 0.3)));
        assertThat(featureImportance.get("foo")[0], closeTo(1.80491886, eps));
        assertThat(featureImportance.get("bar")[0], closeTo(-0.4355742, eps));

        featureImportance = ensemble.featureImportance(zipObjMap(featureNames, Arrays.asList(0.7, 0.2)));
        assertThat(featureImportance.get("foo")[0], closeTo(2.0538184, eps));
        assertThat(featureImportance.get("bar")[0], closeTo(0.1451914, eps));

        featureImportance = ensemble.featureImportance(zipObjMap(featureNames, Arrays.asList(0.8, 0.1)));
        assertThat(featureImportance.get("foo")[0], closeTo(2.0538184, eps));
        assertThat(featureImportance.get("bar")[0], closeTo(0.1451914, eps));

        featureImportance = ensemble.featureImportance(zipObjMap(featureNames, Arrays.asList(0.9, 0.0)));
        assertThat(featureImportance.get("foo")[0], closeTo(2.0538184, eps));
        assertThat(featureImportance.get("bar")[0], closeTo(0.1451914, eps));
    }


    private static Map<String, Object> zipObjMap(List<String> keys, List<Double> values) {
        return IntStream.range(0, keys.size()).boxed().collect(Collectors.toMap(keys::get, values::get));
    }
}
