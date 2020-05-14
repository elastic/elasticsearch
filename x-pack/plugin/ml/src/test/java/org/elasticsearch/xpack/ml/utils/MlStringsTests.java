/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.utils;


import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.utils.MlStrings;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
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

    public void testFindMatching_GivenEmptyItems() {
        assertThat(MlStrings.findMatching(new String[0], Collections.emptySet()), is(empty()));
    }

    public void testFindMatching_GivenAllPattern() {
        assertThat(MlStrings.findMatching(new String[] {"_all"}, new HashSet<>(Arrays.asList("a", "b"))), contains("a", "b"));
    }

    public void testFindMatching_GivenWildcardPattern() {
        assertThat(MlStrings.findMatching(new String[] {"*"}, new HashSet<>(Arrays.asList("a", "b"))), contains("a", "b"));
    }

    public void testFindMatching_GivenMixedPatterns() {
        assertThat(MlStrings.findMatching(new String[] {"concrete", "wild-*"}, new HashSet<>(
            Arrays.asList("a", "concrete", "con*", "wild-1", "wild-2"))), contains("concrete", "wild-1", "wild-2"));
    }

    public void testFindMatching_GivenItemMatchedByTwoPatterns() {
        Set<String> matching = MlStrings.findMatching(new String[]{"a*", "ab*"}, new HashSet<>(Collections.singletonList("abc")));
        assertThat(matching, contains("abc"));
    }
}
