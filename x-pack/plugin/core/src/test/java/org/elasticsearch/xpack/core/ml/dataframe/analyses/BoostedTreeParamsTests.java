/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class BoostedTreeParamsTests extends AbstractSerializingTestCase<BoostedTreeParams> {

    @Override
    protected BoostedTreeParams doParseInstance(XContentParser parser) throws IOException {
        ConstructingObjectParser<BoostedTreeParams, Void> objParser =
            new ConstructingObjectParser<>(
                BoostedTreeParams.NAME,
                true,
                a -> new BoostedTreeParams((Double) a[0], (Double) a[1], (Double) a[2], (Integer) a[3], (Double) a[4]));
        BoostedTreeParams.declareFields(objParser);
        return objParser.apply(parser, null);
    }

    @Override
    protected BoostedTreeParams createTestInstance() {
        return createRandom();
    }

    public static BoostedTreeParams createRandom() {
        Double lambda = randomBoolean() ? null : randomDoubleBetween(0.0, Double.MAX_VALUE, true);
        Double gamma = randomBoolean() ? null : randomDoubleBetween(0.0, Double.MAX_VALUE, true);
        Double eta = randomBoolean() ? null : randomDoubleBetween(0.001, 1.0, true);
        Integer maximumNumberTrees = randomBoolean() ? null : randomIntBetween(1, 2000);
        Double featureBagFraction = randomBoolean() ? null : randomDoubleBetween(0.0, 1.0, false);
        return new BoostedTreeParams(lambda, gamma, eta, maximumNumberTrees, featureBagFraction);
    }

    @Override
    protected Writeable.Reader<BoostedTreeParams> instanceReader() {
        return BoostedTreeParams::new;
    }

    public void testConstructor_GivenNegativeLambda() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new BoostedTreeParams(-0.00001, 0.0, 0.5, 500, 0.3));

        assertThat(e.getMessage(), equalTo("[lambda] must be a non-negative double"));
    }

    public void testConstructor_GivenNegativeGamma() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new BoostedTreeParams(0.0, -0.00001, 0.5, 500, 0.3));

        assertThat(e.getMessage(), equalTo("[gamma] must be a non-negative double"));
    }

    public void testConstructor_GivenEtaIsZero() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new BoostedTreeParams(0.0, 0.0, 0.0, 500, 0.3));

        assertThat(e.getMessage(), equalTo("[eta] must be a double in [0.001, 1]"));
    }

    public void testConstructor_GivenEtaIsGreaterThanOne() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new BoostedTreeParams(0.0, 0.0, 1.00001, 500, 0.3));

        assertThat(e.getMessage(), equalTo("[eta] must be a double in [0.001, 1]"));
    }

    public void testConstructor_GivenMaximumNumberTreesIsZero() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new BoostedTreeParams(0.0, 0.0, 0.5, 0, 0.3));

        assertThat(e.getMessage(), equalTo("[maximum_number_trees] must be an integer in [1, 2000]"));
    }

    public void testConstructor_GivenMaximumNumberTreesIsGreaterThan2k() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new BoostedTreeParams(0.0, 0.0, 0.5, 2001, 0.3));

        assertThat(e.getMessage(), equalTo("[maximum_number_trees] must be an integer in [1, 2000]"));
    }

    public void testConstructor_GivenFeatureBagFractionIsLessThanZero() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new BoostedTreeParams(0.0, 0.0, 0.5, 500, -0.00001));

        assertThat(e.getMessage(), equalTo("[feature_bag_fraction] must be a double in (0, 1]"));
    }

    public void testConstructor_GivenFeatureBagFractionIsGreaterThanOne() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new BoostedTreeParams(0.0, 0.0, 0.5, 500, 1.00001));

        assertThat(e.getMessage(), equalTo("[feature_bag_fraction] must be a double in (0, 1]"));
    }
}
