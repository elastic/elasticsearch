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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class ClassificationTests extends AbstractSerializingTestCase<Classification> {

    private static final BoostedTreeParams BOOSTED_TREE_PARAMS = new BoostedTreeParams(0.0, 0.0, 0.5, 500, 1.0);

    @Override
    protected Classification doParseInstance(XContentParser parser) throws IOException {
        return Classification.fromXContent(parser, false);
    }

    @Override
    protected Classification createTestInstance() {
        return createRandom();
    }

    public static Classification createRandom() {
        String dependentVariableName = randomAlphaOfLength(10);
        BoostedTreeParams boostedTreeParams = BoostedTreeParamsTests.createRandom();
        String predictionFieldName = randomBoolean() ? null : randomAlphaOfLength(10);
        Integer numTopClasses = randomBoolean() ? null : randomIntBetween(0, 1000);
        Double trainingPercent = randomBoolean() ? null : randomDoubleBetween(1.0, 100.0, true);
        return new Classification(dependentVariableName, boostedTreeParams, predictionFieldName, numTopClasses, trainingPercent);
    }

    @Override
    protected Writeable.Reader<Classification> instanceReader() {
        return Classification::new;
    }

    public void testConstructor_GivenTrainingPercentIsLessThanOne() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Classification("foo", BOOSTED_TREE_PARAMS, "result", 3, 0.999));

        assertThat(e.getMessage(), equalTo("[training_percent] must be a double in [1, 100]"));
    }

    public void testConstructor_GivenTrainingPercentIsGreaterThan100() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Classification("foo", BOOSTED_TREE_PARAMS, "result", 3, 100.0001));

        assertThat(e.getMessage(), equalTo("[training_percent] must be a double in [1, 100]"));
    }

    public void testConstructor_GivenNumTopClassesIsLessThanZero() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Classification("foo", BOOSTED_TREE_PARAMS, "result", -1, 1.0));

        assertThat(e.getMessage(), equalTo("[num_top_classes] must be an integer in [0, 1000]"));
    }

    public void testConstructor_GivenNumTopClassesIsGreaterThan1000() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Classification("foo", BOOSTED_TREE_PARAMS, "result", 1001, 1.0));

        assertThat(e.getMessage(), equalTo("[num_top_classes] must be an integer in [0, 1000]"));
    }

    public void testGetNumTopClasses() {
        Classification classification = new Classification("foo", BOOSTED_TREE_PARAMS, "result", 7, 1.0);
        assertThat(classification.getNumTopClasses(), equalTo(7));

        // Boundary condition: num_top_classes == 0
        classification = new Classification("foo", BOOSTED_TREE_PARAMS, "result", 0, 1.0);
        assertThat(classification.getNumTopClasses(), equalTo(0));

        // Boundary condition: num_top_classes == 1000
        classification = new Classification("foo", BOOSTED_TREE_PARAMS, "result", 1000, 1.0);
        assertThat(classification.getNumTopClasses(), equalTo(1000));

        // num_top_classes == null, default applied
        classification = new Classification("foo", BOOSTED_TREE_PARAMS, "result", null, 1.0);
        assertThat(classification.getNumTopClasses(), equalTo(2));
    }

    public void testGetTrainingPercent() {
        Classification classification = new Classification("foo", BOOSTED_TREE_PARAMS, "result", 3, 50.0);
        assertThat(classification.getTrainingPercent(), equalTo(50.0));

        // Boundary condition: training_percent == 1.0
        classification = new Classification("foo", BOOSTED_TREE_PARAMS, "result", 3, 1.0);
        assertThat(classification.getTrainingPercent(), equalTo(1.0));

        // Boundary condition: training_percent == 100.0
        classification = new Classification("foo", BOOSTED_TREE_PARAMS, "result", 3, 100.0);
        assertThat(classification.getTrainingPercent(), equalTo(100.0));

        // training_percent == null, default applied
        classification = new Classification("foo", BOOSTED_TREE_PARAMS, "result", 3, null);
        assertThat(classification.getTrainingPercent(), equalTo(100.0));
    }

    public void testFieldCardinalityLimitsIsNonNull() {
        assertThat(createTestInstance().getFieldCardinalityLimits(), is(not(nullValue())));
    }
}
