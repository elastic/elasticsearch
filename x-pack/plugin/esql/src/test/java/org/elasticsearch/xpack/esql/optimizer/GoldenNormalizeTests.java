/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class GoldenNormalizeTests extends ESTestCase {
    public void testPlainTextIsUnchanged() {
        assertThat(GoldenTestCase.normalizeString("plain text"), equalTo("plain text"));
    }

    public void testSyntheticNameWithMultipleRepeatingSyntheticIds() {
        assertThat(
            GoldenTestCase.normalizeString("$$alias$1$2#3 $$alias$4$5#6 $$alias$1$2#3"),
            equalTo("$$alias$0#0 $$alias$1#1 $$alias$0#0")
        );
    }

    public void testSyntheticNameWithMultipleNumericSegments() {
        assertThat(GoldenTestCase.normalizeString("$$alias$1$2#3"), equalTo("$$alias$0#0"));
    }

    public void testSyntheticNameWithMixedTextAndNumericSegmentsMultipleSegments() {
        assertThat(GoldenTestCase.normalizeString("$$last_name$LENGTH$241149320$123$456{f$}#6"), equalTo("$$last_name$LENGTH$0{f$}#0"));
    }

    public void testSyntheticFollowedByBrace() {
        assertThat(GoldenTestCase.normalizeString("$$x$foo$99$123{body}"), equalTo("$$x$foo$0{body}"));
    }

    public void testMixedSegmentsMultipleTextParts() {
        assertThat(GoldenTestCase.normalizeString("$$a$B$C$42#7"), equalTo("$$a$B$C$0#0"));
    }
}
