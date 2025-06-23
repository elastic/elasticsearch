/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class BoostedTreeParamsTests extends AbstractBWCSerializationTestCase<BoostedTreeParams> {

    @Override
    protected BoostedTreeParams doParseInstance(XContentParser parser) throws IOException {
        ConstructingObjectParser<BoostedTreeParams, Void> objParser = new ConstructingObjectParser<>(
            BoostedTreeParams.NAME,
            true,
            a -> new BoostedTreeParams(
                (Double) a[0],
                (Double) a[1],
                (Double) a[2],
                (Integer) a[3],
                (Double) a[4],
                (Integer) a[5],
                (Double) a[6],
                (Double) a[7],
                (Double) a[8],
                (Double) a[9],
                (Double) a[10],
                (Integer) a[11]
            )
        );
        BoostedTreeParams.declareFields(objParser);
        return objParser.apply(parser, null);
    }

    @Override
    protected BoostedTreeParams createTestInstance() {
        return createRandom();
    }

    @Override
    protected BoostedTreeParams mutateInstance(BoostedTreeParams instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public static BoostedTreeParams createRandom() {
        return BoostedTreeParams.builder()
            .setLambda(randomBoolean() ? null : randomDoubleBetween(0.0, Double.MAX_VALUE, true))
            .setGamma(randomBoolean() ? null : randomDoubleBetween(0.0, Double.MAX_VALUE, true))
            .setEta(randomBoolean() ? null : randomDoubleBetween(0.001, 1.0, true))
            .setMaxTrees(randomBoolean() ? null : randomIntBetween(1, 2000))
            .setFeatureBagFraction(randomBoolean() ? null : randomDoubleBetween(0.0, 1.0, false))
            .setNumTopFeatureImportanceValues(randomBoolean() ? null : randomIntBetween(0, Integer.MAX_VALUE))
            .setAlpha(randomBoolean() ? null : randomDoubleBetween(0.0, Double.MAX_VALUE, true))
            .setEtaGrowthRatePerTree(randomBoolean() ? null : randomDoubleBetween(0.5, 2.0, true))
            .setSoftTreeDepthLimit(randomBoolean() ? null : randomDoubleBetween(0.0, Double.MAX_VALUE, true))
            .setSoftTreeDepthTolerance(randomBoolean() ? null : randomDoubleBetween(0.01, Double.MAX_VALUE, true))
            .setDownsampleFactor(randomBoolean() ? null : randomDoubleBetween(0.0, 1.0, false))
            .setMaxOptimizationRoundsPerHyperparameter(randomBoolean() ? null : randomIntBetween(0, 20))
            .build();
    }

    public static BoostedTreeParams mutateForVersion(BoostedTreeParams instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected Writeable.Reader<BoostedTreeParams> instanceReader() {
        return BoostedTreeParams::new;
    }

    public void testConstructor_GivenNegativeLambda() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> BoostedTreeParams.builder().setLambda(-0.00001).build()
        );

        assertThat(e.getMessage(), equalTo("[lambda] must be a non-negative double"));
    }

    public void testConstructor_GivenNegativeGamma() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> BoostedTreeParams.builder().setGamma(-0.00001).build()
        );

        assertThat(e.getMessage(), equalTo("[gamma] must be a non-negative double"));
    }

    public void testConstructor_GivenEtaIsZero() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> BoostedTreeParams.builder().setEta(0.0).build()
        );

        assertThat(e.getMessage(), equalTo("[eta] must be a double in [0.001, 1]"));
    }

    public void testConstructor_GivenEtaIsGreaterThanOne() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> BoostedTreeParams.builder().setEta(1.00001).build()
        );

        assertThat(e.getMessage(), equalTo("[eta] must be a double in [0.001, 1]"));
    }

    public void testConstructor_GivenMaximumNumberTreesIsZero() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> BoostedTreeParams.builder().setMaxTrees(0).build()
        );

        assertThat(e.getMessage(), equalTo("[max_trees] must be an integer in [1, 2000]"));
    }

    public void testConstructor_GivenMaximumNumberTreesIsGreaterThan2k() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> BoostedTreeParams.builder().setMaxTrees(2001).build()
        );

        assertThat(e.getMessage(), equalTo("[max_trees] must be an integer in [1, 2000]"));
    }

    public void testConstructor_GivenFeatureBagFractionIsLessThanZero() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> BoostedTreeParams.builder().setFeatureBagFraction(-0.00001).build()
        );

        assertThat(e.getMessage(), equalTo("[feature_bag_fraction] must be a double in (0, 1]"));
    }

    public void testConstructor_GivenFeatureBagFractionIsGreaterThanOne() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> BoostedTreeParams.builder().setFeatureBagFraction(1.00001).build()
        );

        assertThat(e.getMessage(), equalTo("[feature_bag_fraction] must be a double in (0, 1]"));
    }

    public void testConstructor_GivenTopFeatureImportanceValuesIsNegative() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> BoostedTreeParams.builder().setNumTopFeatureImportanceValues(-1).build()
        );

        assertThat(e.getMessage(), equalTo("[num_top_feature_importance_values] must be a non-negative integer"));
    }

    public void testConstructor_GivenAlphaIsNegative() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> BoostedTreeParams.builder().setAlpha(-0.001).build()
        );

        assertThat(e.getMessage(), equalTo("[alpha] must be a non-negative double"));
    }

    public void testConstructor_GivenAlphaIsZero() {
        assertThat(BoostedTreeParams.builder().setAlpha(0.0).build().getAlpha(), equalTo(0.0));
    }

    public void testConstructor_GivenEtaGrowthRatePerTreeIsOnRangeLimit() {
        assertThat(BoostedTreeParams.builder().setEtaGrowthRatePerTree(0.5).build().getEtaGrowthRatePerTree(), equalTo(0.5));
        assertThat(BoostedTreeParams.builder().setEtaGrowthRatePerTree(2.0).build().getEtaGrowthRatePerTree(), equalTo(2.0));
    }

    public void testConstructor_GivenEtaGrowthRatePerTreeIsLessThanMin() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> BoostedTreeParams.builder().setEtaGrowthRatePerTree(0.49999).build()
        );

        assertThat(e.getMessage(), equalTo("[eta_growth_rate_per_tree] must be a double in [0.5, 2.0]"));
    }

    public void testConstructor_GivenEtaGrowthRatePerTreeIsGreaterThanMax() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> BoostedTreeParams.builder().setEtaGrowthRatePerTree(2.00001).build()
        );

        assertThat(e.getMessage(), equalTo("[eta_growth_rate_per_tree] must be a double in [0.5, 2.0]"));
    }

    public void testConstructor_GivenSoftTreeDepthLimitIsNegative() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> BoostedTreeParams.builder().setSoftTreeDepthLimit(-0.001).build()
        );

        assertThat(e.getMessage(), equalTo("[soft_tree_depth_limit] must be a non-negative double"));
    }

    public void testConstructor_GivenSoftTreeDepthLimitIsZero() {
        assertThat(BoostedTreeParams.builder().setSoftTreeDepthLimit(0.0).build().getSoftTreeDepthLimit(), equalTo(0.0));
    }

    public void testConstructor_GivenSoftTreeDepthToleranceIsLessThanMin() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> BoostedTreeParams.builder().setSoftTreeDepthTolerance(0.001).build()
        );

        assertThat(e.getMessage(), equalTo("[soft_tree_depth_tolerance] must be a double greater than or equal to 0.01"));
    }

    public void testConstructor_GivenSoftTreeDepthToleranceIsMin() {
        assertThat(BoostedTreeParams.builder().setSoftTreeDepthTolerance(0.01).build().getSoftTreeDepthTolerance(), equalTo(0.01));
    }

    public void testConstructor_GivenDownsampleFactorIsZero() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> BoostedTreeParams.builder().setDownsampleFactor(0.0).build()
        );

        assertThat(e.getMessage(), equalTo("[downsample_factor] must be a double in (0, 1]"));
    }

    public void testConstructor_GivenDownsampleFactorIsNegative() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> BoostedTreeParams.builder().setDownsampleFactor(-42.0).build()
        );

        assertThat(e.getMessage(), equalTo("[downsample_factor] must be a double in (0, 1]"));
    }

    public void testConstructor_GivenDownsampleFactorIsOne() {
        assertThat(BoostedTreeParams.builder().setDownsampleFactor(1.0).build().getDownsampleFactor(), equalTo(1.0));
    }

    public void testConstructor_GivenDownsampleFactorIsGreaterThanOne() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> BoostedTreeParams.builder().setDownsampleFactor(1.00001).build()
        );

        assertThat(e.getMessage(), equalTo("[downsample_factor] must be a double in (0, 1]"));
    }

    public void testConstructor_GivenMaxOptimizationRoundsPerHyperparameterIsZero() {
        assertThat(
            BoostedTreeParams.builder().setMaxOptimizationRoundsPerHyperparameter(0).build().getMaxOptimizationRoundsPerHyperparameter(),
            equalTo(0)
        );
    }

    public void testConstructor_GivenMaxOptimizationRoundsPerHyperparameterIsNegative() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> BoostedTreeParams.builder().setMaxOptimizationRoundsPerHyperparameter(-1).build()
        );

        assertThat(e.getMessage(), equalTo("[max_optimization_rounds_per_hyperparameter] must be an integer in [0, 20]"));
    }

    public void testConstructor_GivenMaxOptimizationRoundsPerHyperparameterIsMax() {
        assertThat(
            BoostedTreeParams.builder().setMaxOptimizationRoundsPerHyperparameter(20).build().getMaxOptimizationRoundsPerHyperparameter(),
            equalTo(20)
        );
    }

    public void testConstructor_GivenMaxOptimizationRoundsPerHyperparameterIsGreaterThanMax() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> BoostedTreeParams.builder().setMaxOptimizationRoundsPerHyperparameter(21).build()
        );

        assertThat(e.getMessage(), equalTo("[max_optimization_rounds_per_hyperparameter] must be an integer in [0, 20]"));
    }

    public void testGetParams_GivenEmpty() {
        assertThat(BoostedTreeParams.builder().build().getParams(), is(anEmptyMap()));
    }

    public void testGetParams_GivenAllParams() {
        BoostedTreeParams boostedTreeParams = BoostedTreeParams.builder()
            .setLambda(randomDoubleBetween(0.0, Double.MAX_VALUE, true))
            .setGamma(randomDoubleBetween(0.0, Double.MAX_VALUE, true))
            .setEta(randomDoubleBetween(0.001, 1.0, true))
            .setMaxTrees(randomIntBetween(1, 2000))
            .setFeatureBagFraction(randomDoubleBetween(0.0, 1.0, false))
            .setNumTopFeatureImportanceValues(randomIntBetween(0, Integer.MAX_VALUE))
            .setAlpha(randomDoubleBetween(0.0, Double.MAX_VALUE, true))
            .setEtaGrowthRatePerTree(randomDoubleBetween(0.5, 2.0, true))
            .setSoftTreeDepthLimit(randomDoubleBetween(0.0, Double.MAX_VALUE, true))
            .setSoftTreeDepthTolerance(randomDoubleBetween(0.01, Double.MAX_VALUE, true))
            .setDownsampleFactor(randomDoubleBetween(0.0, 1.0, false))
            .setMaxOptimizationRoundsPerHyperparameter(randomIntBetween(0, 20))
            .build();

        Map<String, Object> expectedParams = new HashMap<>();
        expectedParams.put("lambda", boostedTreeParams.getLambda());
        expectedParams.put("gamma", boostedTreeParams.getGamma());
        expectedParams.put("eta", boostedTreeParams.getEta());
        expectedParams.put("max_trees", boostedTreeParams.getMaxTrees());
        expectedParams.put("feature_bag_fraction", boostedTreeParams.getFeatureBagFraction());
        expectedParams.put("num_top_feature_importance_values", boostedTreeParams.getNumTopFeatureImportanceValues());
        expectedParams.put("alpha", boostedTreeParams.getAlpha());
        expectedParams.put("eta_growth_rate_per_tree", boostedTreeParams.getEtaGrowthRatePerTree());
        expectedParams.put("soft_tree_depth_limit", boostedTreeParams.getSoftTreeDepthLimit());
        expectedParams.put("soft_tree_depth_tolerance", boostedTreeParams.getSoftTreeDepthTolerance());
        expectedParams.put("downsample_factor", boostedTreeParams.getDownsampleFactor());
        expectedParams.put("max_optimization_rounds_per_hyperparameter", boostedTreeParams.getMaxOptimizationRoundsPerHyperparameter());

        assertThat(boostedTreeParams.getParams(), equalTo(expectedParams));
    }

    @Override
    protected BoostedTreeParams mutateInstanceForVersion(BoostedTreeParams instance, TransportVersion version) {
        return mutateForVersion(instance, version);
    }
}
