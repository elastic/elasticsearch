/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

public class RawInferenceResultsTests extends ESTestCase {

    public static RawInferenceResults createRandomResults() {
        int n = randomIntBetween(1, 10);
        double[] results = new double[n];
        for (int i = 0; i < n; i++) {
            results[i] = randomDouble();
        }
        return new RawInferenceResults(results,
            randomBoolean() ? Collections.emptyMap() : Collections.singletonMap("foo", new double[]{1.08}));
    }

    public void testEqualityAndHashcode() {
        int n = randomIntBetween(1, 10);
        double[] results = new double[n];
        for (int i = 0; i < n; i++) {
            results[i] = randomDouble();
        }
        Map<String, double[]> importance = randomBoolean() ?
            Collections.emptyMap() :
            Collections.singletonMap("foo", new double[]{1.08, 42.0});
        RawInferenceResults lft = new RawInferenceResults(results, new HashMap<>(importance));
        RawInferenceResults rgt = new RawInferenceResults(Arrays.copyOf(results, n), new HashMap<>(importance));
        assertThat(lft, equalTo(rgt));
        assertThat(lft.hashCode(), equalTo(rgt.hashCode()));
    }

}
