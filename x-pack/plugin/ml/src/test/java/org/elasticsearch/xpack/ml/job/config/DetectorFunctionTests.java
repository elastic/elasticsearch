/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.DetectorFunction;

import static org.hamcrest.Matchers.equalTo;

public class DetectorFunctionTests extends ESTestCase {

    public void testShortcuts() {
        assertThat(DetectorFunction.fromString("nzc").getFullName(), equalTo("non_zero_count"));
        assertThat(DetectorFunction.fromString("low_nzc").getFullName(), equalTo("low_non_zero_count"));
        assertThat(DetectorFunction.fromString("high_nzc").getFullName(), equalTo("high_non_zero_count"));
        assertThat(DetectorFunction.fromString("dc").getFullName(), equalTo("distinct_count"));
        assertThat(DetectorFunction.fromString("low_dc").getFullName(), equalTo("low_distinct_count"));
        assertThat(DetectorFunction.fromString("high_dc").getFullName(), equalTo("high_distinct_count"));
    }

    public void testFromString_GivenInvalidFunction() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DetectorFunction.fromString("invalid"));
        assertThat(e.getMessage(), equalTo("Unknown function 'invalid'"));
    }
}
