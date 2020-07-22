/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.nullValue;

public class PredictionFieldTypeTests extends ESTestCase {

    public void testTransformPredictedValueBoolean() {
        assertThat(PredictionFieldType.BOOLEAN.transformPredictedValue(null, randomBoolean() ? null : randomAlphaOfLength(10)),
            is(nullValue()));
        assertThat(PredictionFieldType.BOOLEAN.transformPredictedValue(1.0, randomBoolean() ? null : randomAlphaOfLength(10)),
            is(true));
        assertThat(PredictionFieldType.BOOLEAN.transformPredictedValue(0.0, randomBoolean() ? null : randomAlphaOfLength(10)),
            is(false));
        expectThrows(IllegalArgumentException.class,
            () -> PredictionFieldType.BOOLEAN.transformPredictedValue(0.1, randomBoolean() ? null : randomAlphaOfLength(10)));
        expectThrows(IllegalArgumentException.class,
            () -> PredictionFieldType.BOOLEAN.transformPredictedValue(1.1, randomBoolean() ? null : randomAlphaOfLength(10)));
    }

    public void testTransformPredictedValueString() {
        assertThat(PredictionFieldType.STRING.transformPredictedValue(null, randomBoolean() ? null : randomAlphaOfLength(10)),
            is(nullValue()));
        assertThat(PredictionFieldType.STRING.transformPredictedValue(1.0, "foo"), equalTo("foo"));
        assertThat(PredictionFieldType.STRING.transformPredictedValue(1.0, null), equalTo("1.0"));
    }

    public void testTransformPredictedValueNumber() {
        assertThat(PredictionFieldType.NUMBER.transformPredictedValue(null, randomBoolean() ? null : randomAlphaOfLength(10)),
            is(nullValue()));
        assertThat(PredictionFieldType.NUMBER.transformPredictedValue(1.0, "foo"), equalTo(1.0));
        assertThat(PredictionFieldType.NUMBER.transformPredictedValue(1.0, null), equalTo(1.0));
        assertThat(PredictionFieldType.NUMBER.transformPredictedValue(1.0, ""), equalTo(1.0));
        long expected = randomLong();
        assertThat(PredictionFieldType.NUMBER.transformPredictedValue(1.0, String.valueOf(expected)), equalTo(expected));
    }

}
