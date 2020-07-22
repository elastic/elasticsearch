/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.equalTo;

public class RawInferenceResultsTests extends ESTestCase {

    public static RawInferenceResults createRandomResults() {
        int n = randomIntBetween(1, 10);
        double[] results = new double[n];
        for (int i = 0; i < n; i++) {
            results[i] = randomDouble();
        }
        return new RawInferenceResults(results, randomBoolean() ? new double[0][] : new double[][]{{1.08}} );
    }

    public void testEqualityAndHashcode() {
        int n = randomIntBetween(1, 10);
        double[] results = new double[n];
        for (int i = 0; i < n; i++) {
            results[i] = randomDouble();
        }
        double[][] importance = randomBoolean() ?
            new double[0][] :
            new double[][]{{1.08, 42.0}};
        RawInferenceResults lft = new RawInferenceResults(results, importance);
        RawInferenceResults rgt = new RawInferenceResults(Arrays.copyOf(results, n), importance);
        assertThat(lft, equalTo(rgt));
        assertThat(lft.hashCode(), equalTo(rgt.hashCode()));
    }

}
