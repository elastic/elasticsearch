/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class InferenceHelpersTests extends ESTestCase {

    public void testToDoubleFromNumbers() {
        assertThat(InferenceHelpers.toDouble(0.5), equalTo(0.5));
        assertThat(InferenceHelpers.toDouble(5L), equalTo(5.0));
        assertThat(InferenceHelpers.toDouble(5), equalTo(5.0));
        assertThat(InferenceHelpers.toDouble(0.5f), equalTo(0.5));
    }

    public void testToDoubleFromString() {
        assertThat(InferenceHelpers.toDouble(""), is(nullValue()));
        assertThat(InferenceHelpers.toDouble("0.5"), equalTo(0.5));
        assertThat(InferenceHelpers.toDouble("-0.5"), equalTo(-0.5));
        assertThat(InferenceHelpers.toDouble("5"), equalTo(5.0));
        assertThat(InferenceHelpers.toDouble("-5"), equalTo(-5.0));

        // if ae are turned off, then we should get a null value
        // otherwise, we should expect an assertion failure telling us that the string is improperly formatted
        try {
            assertThat(InferenceHelpers.toDouble(""), is(nullValue()));
        } catch (AssertionError ae) {
            assertThat(ae.getMessage(), equalTo("value is not properly formatted double []"));
        }
        try {
            assertThat(InferenceHelpers.toDouble("notADouble"), is(nullValue()));
        } catch (AssertionError ae) {
            assertThat(ae.getMessage(), equalTo("value is not properly formatted double [notADouble]"));
        }
    }

    public void testToDoubleFromNull() {
        assertThat(InferenceHelpers.toDouble(null), is(nullValue()));
    }

    public void testDoubleFromUnknownObj() {
        assertThat(InferenceHelpers.toDouble(new HashMap<>()), is(nullValue()));
    }

}
