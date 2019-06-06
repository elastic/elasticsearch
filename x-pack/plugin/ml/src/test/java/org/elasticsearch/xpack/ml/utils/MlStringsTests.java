/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.utils;


import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.utils.MlStrings;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class MlStringsTests extends ESTestCase {
    public void testDoubleQuoteIfNotAlphaNumeric() {
        assertEquals("foo2", MlStrings.doubleQuoteIfNotAlphaNumeric("foo2"));
        assertEquals("\"fo o\"", MlStrings.doubleQuoteIfNotAlphaNumeric("fo o"));
        assertEquals("\" \"", MlStrings.doubleQuoteIfNotAlphaNumeric(" "));
        assertEquals("\"ba\\\"r\\\"\"", MlStrings.doubleQuoteIfNotAlphaNumeric("ba\"r\""));
    }

    public void testIsValidId() {
        assertThat(MlStrings.isValidId("1_-.a"), is(true));
        assertThat(MlStrings.isValidId("b.-_3"), is(true));
        assertThat(MlStrings.isValidId("a-b.c_d"), is(true));

        assertThat(MlStrings.isValidId("a1_-."), is(false));
        assertThat(MlStrings.isValidId("-.a1_"), is(false));
        assertThat(MlStrings.isValidId(".a1_-"), is(false));
        assertThat(MlStrings.isValidId("_-.a1"), is(false));
        assertThat(MlStrings.isValidId("A"), is(false));
        assertThat(MlStrings.isValidId("!afafd"), is(false));
        assertThat(MlStrings.isValidId("_all"), is(false));
    }

    public void testGetParentField() {
        assertThat(MlStrings.getParentField(null), is(nullValue()));
        assertThat(MlStrings.getParentField("foo"), equalTo("foo"));
        assertThat(MlStrings.getParentField("foo.bar"), equalTo("foo"));
        assertThat(MlStrings.getParentField("x.y.z"), equalTo("x.y"));
    }

    public void testHasValidLengthForId() {
        assertThat(MlStrings.hasValidLengthForId(randomAlphaOfLength(64)), is(true));
        assertThat(MlStrings.hasValidLengthForId(randomAlphaOfLength(65)), is(false));
    }
}
