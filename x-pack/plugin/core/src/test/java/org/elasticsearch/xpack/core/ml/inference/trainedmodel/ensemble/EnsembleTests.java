/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.Tree;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeNode;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeTests;
import org.junit.Before;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class EnsembleTests extends AbstractSerializingTestCase<Ensemble> {

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
        return field -> field.isEmpty() == false;
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
        return createRandom(targetType, featureNames);
    }

    public static Ensemble createRandom(TargetType targetType, List<String> featureNames) {
        int numberOfModels = randomIntBetween(1, 10);
        List<String> treeFeatureNames = featureNames.isEmpty() ?
            Stream.generate(() -> randomAlphaOfLength(10)).limit(5).collect(Collectors.toList()) :
            featureNames;
        List<TrainedModel> models = Stream.generate(() -> TreeTests.buildRandomTree(treeFeatureNames, 6))
            .limit(numberOfModels)
            .collect(Collectors.toList());
        double[] weights = randomBoolean() ?
            null :
            Stream.generate(ESTestCase::randomDouble).limit(numberOfModels).mapToDouble(Double::valueOf).toArray();
        List<String> categoryLabels = null;
        if (randomBoolean() && targetType == TargetType.CLASSIFICATION) {
            categoryLabels = randomList(2, randomIntBetween(3, 10), () -> randomAlphaOfLength(10));
        }

        OutputAggregator outputAggregator = targetType == TargetType.REGRESSION ?
            randomFrom(new WeightedSum(weights), new Exponent(weights)) :
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

        return new Ensemble(featureNames,
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

}
