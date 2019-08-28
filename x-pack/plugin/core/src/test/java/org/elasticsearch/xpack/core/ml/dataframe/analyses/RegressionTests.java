/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class RegressionTests extends AbstractSerializingTestCase<Regression> {

    @Override
    protected Regression doParseInstance(XContentParser parser) throws IOException {
        return Regression.fromXContent(parser, false);
    }

    @Override
    protected Regression createTestInstance() {
        return createRandom();
    }

    public static Regression createRandom() {
        Double lambda = randomBoolean() ? null : randomDoubleBetween(0.0, Double.MAX_VALUE, true);
        Double gamma = randomBoolean() ? null : randomDoubleBetween(0.0, Double.MAX_VALUE, true);
        Double eta = randomBoolean() ? null : randomDoubleBetween(0.001, 1.0, true);
        Integer maximumNumberTrees = randomBoolean() ? null : randomIntBetween(1, 2000);
        Double featureBagFraction = randomBoolean() ? null : randomDoubleBetween(0.0, 1.0, false);
        String predictionFieldName = randomBoolean() ? null : randomAlphaOfLength(10);
        Double trainingPercent = randomBoolean() ? null : randomDoubleBetween(1.0, 100.0, true);
        return new Regression(randomAlphaOfLength(10), lambda, gamma, eta, maximumNumberTrees, featureBagFraction,
            predictionFieldName, trainingPercent);
    }

    @Override
    protected Writeable.Reader<Regression> instanceReader() {
        return Regression::new;
    }

    public void testRegression_GivenNegativeLambda() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Regression("foo", -0.00001, 0.0, 0.5, 500, 0.3, "result", 100.0));

        assertThat(e.getMessage(), equalTo("[lambda] must be a non-negative double"));
    }

    public void testRegression_GivenNegativeGamma() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Regression("foo", 0.0, -0.00001, 0.5, 500, 0.3, "result", 100.0));

        assertThat(e.getMessage(), equalTo("[gamma] must be a non-negative double"));
    }

    public void testRegression_GivenEtaIsZero() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Regression("foo", 0.0, 0.0, 0.0, 500, 0.3, "result", 100.0));

        assertThat(e.getMessage(), equalTo("[eta] must be a double in [0.001, 1]"));
    }

    public void testRegression_GivenEtaIsGreaterThanOne() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Regression("foo", 0.0, 0.0, 1.00001, 500, 0.3, "result", 100.0));

        assertThat(e.getMessage(), equalTo("[eta] must be a double in [0.001, 1]"));
    }

    public void testRegression_GivenMaximumNumberTreesIsZero() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Regression("foo", 0.0, 0.0, 0.5, 0, 0.3, "result", 100.0));

        assertThat(e.getMessage(), equalTo("[maximum_number_trees] must be an integer in [1, 2000]"));
    }

    public void testRegression_GivenMaximumNumberTreesIsGreaterThan2k() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Regression("foo", 0.0, 0.0, 0.5, 2001, 0.3, "result", 100.0));

        assertThat(e.getMessage(), equalTo("[maximum_number_trees] must be an integer in [1, 2000]"));
    }

    public void testRegression_GivenFeatureBagFractionIsLessThanZero() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Regression("foo", 0.0, 0.0, 0.5, 500, -0.00001, "result", 100.0));

        assertThat(e.getMessage(), equalTo("[feature_bag_fraction] must be a double in (0, 1]"));
    }

    public void testRegression_GivenFeatureBagFractionIsGreaterThanOne() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Regression("foo", 0.0, 0.0, 0.5, 500, 1.00001, "result", 100.0));

        assertThat(e.getMessage(), equalTo("[feature_bag_fraction] must be a double in (0, 1]"));
    }

    public void testRegression_GivenTrainingPercentIsNull() {
        Regression regression = new Regression("foo", 0.0, 0.0, 0.5, 500, 1.0, "result", null);
        assertThat(regression.getTrainingPercent(), equalTo(100.0));
    }

    public void testRegression_GivenTrainingPercentIsBoundary() {
        Regression regression = new Regression("foo", 0.0, 0.0, 0.5, 500, 1.0, "result", 1.0);
        assertThat(regression.getTrainingPercent(), equalTo(1.0));
        regression = new Regression("foo", 0.0, 0.0, 0.5, 500, 1.0, "result", 100.0);
        assertThat(regression.getTrainingPercent(), equalTo(100.0));
    }

    public void testRegression_GivenTrainingPercentIsLessThanOne() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Regression("foo", 0.0, 0.0, 0.5, 500, 1.0, "result", 0.999));

        assertThat(e.getMessage(), equalTo("[training_percent] must be a double in [1, 100]"));
    }

    public void testRegression_GivenTrainingPercentIsGreaterThan100() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Regression("foo", 0.0, 0.0, 0.5, 500, 1.0, "result", 100.0001));

        assertThat(e.getMessage(), equalTo("[training_percent] must be a double in [1, 100]"));
    }
}
