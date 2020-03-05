/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class BoostedTreeParamsTests extends AbstractBWCSerializationTestCase<BoostedTreeParams> {

    @Override
    protected BoostedTreeParams doParseInstance(XContentParser parser) throws IOException {
        ConstructingObjectParser<BoostedTreeParams, Void> objParser =
            new ConstructingObjectParser<>(
                BoostedTreeParams.NAME,
                true,
                a -> new BoostedTreeParams((Double) a[0], (Double) a[1], (Double) a[2], (Integer) a[3], (Double) a[4], (Integer) a[5]));
        BoostedTreeParams.declareFields(objParser);
        return objParser.apply(parser, null);
    }

    @Override
    protected BoostedTreeParams createTestInstance() {
        return createRandom();
    }

    public static BoostedTreeParams createRandom() {
        return BoostedTreeParams.builder()
            .setLambda(randomBoolean() ? null : randomDoubleBetween(0.0, Double.MAX_VALUE, true))
            .setGamma(randomBoolean() ? null : randomDoubleBetween(0.0, Double.MAX_VALUE, true))
            .setEta(randomBoolean() ? null : randomDoubleBetween(0.001, 1.0, true))
            .setMaximumNumberTrees(randomBoolean() ? null : randomIntBetween(1, 2000))
            .setFeatureBagFraction(randomBoolean() ? null : randomDoubleBetween(0.0, 1.0, false))
            .setNumTopFeatureImportanceValues(randomBoolean() ? null : randomIntBetween(0, Integer.MAX_VALUE))
            .build();
    }

    public static BoostedTreeParams mutateForVersion(BoostedTreeParams instance, Version version) {
        BoostedTreeParams.Builder builder = new BoostedTreeParams.Builder(instance);
        if (version.before(Version.V_7_6_0)) {
            builder.setNumTopFeatureImportanceValues(null);
        }
        return builder.build();
    }

    @Override
    protected Writeable.Reader<BoostedTreeParams> instanceReader() {
        return BoostedTreeParams::new;
    }

    public void testConstructor_GivenNegativeLambda() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> BoostedTreeParams.builder().setLambda(-0.00001).build());

        assertThat(e.getMessage(), equalTo("[lambda] must be a non-negative double"));
    }

    public void testConstructor_GivenNegativeGamma() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> BoostedTreeParams.builder().setGamma(-0.00001).build());

        assertThat(e.getMessage(), equalTo("[gamma] must be a non-negative double"));
    }

    public void testConstructor_GivenEtaIsZero() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> BoostedTreeParams.builder().setEta(0.0).build());

        assertThat(e.getMessage(), equalTo("[eta] must be a double in [0.001, 1]"));
    }

    public void testConstructor_GivenEtaIsGreaterThanOne() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> BoostedTreeParams.builder().setEta(1.00001).build());

        assertThat(e.getMessage(), equalTo("[eta] must be a double in [0.001, 1]"));
    }

    public void testConstructor_GivenMaximumNumberTreesIsZero() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> BoostedTreeParams.builder().setMaximumNumberTrees(0).build());

        assertThat(e.getMessage(), equalTo("[maximum_number_trees] must be an integer in [1, 2000]"));
    }

    public void testConstructor_GivenMaximumNumberTreesIsGreaterThan2k() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> BoostedTreeParams.builder().setMaximumNumberTrees(2001).build());

        assertThat(e.getMessage(), equalTo("[maximum_number_trees] must be an integer in [1, 2000]"));
    }

    public void testConstructor_GivenFeatureBagFractionIsLessThanZero() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> BoostedTreeParams.builder().setFeatureBagFraction(-0.00001).build());

        assertThat(e.getMessage(), equalTo("[feature_bag_fraction] must be a double in (0, 1]"));
    }

    public void testConstructor_GivenFeatureBagFractionIsGreaterThanOne() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> BoostedTreeParams.builder().setFeatureBagFraction(1.00001).build());

        assertThat(e.getMessage(), equalTo("[feature_bag_fraction] must be a double in (0, 1]"));
    }

    public void testConstructor_GivenTopFeatureImportanceValuesIsNegative() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> BoostedTreeParams.builder().setNumTopFeatureImportanceValues(-1).build());

        assertThat(e.getMessage(), equalTo("[num_top_feature_importance_values] must be a non-negative integer"));
    }

    @Override
    protected BoostedTreeParams mutateInstanceForVersion(BoostedTreeParams instance, Version version) {
        return mutateForVersion(instance, version);
    }
}
