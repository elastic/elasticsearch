/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class StatusHeuristicTests extends ESTestCase {

    private static final String BETA_STR = "beta";
    private static final String BETA_CAPS_STR = "BETA";
    private static final String PREVIEW_STR = "preview";
    private static final String GA_STR = "ga";
    private static final String DEPRECATED_STR = "deprecated";
    private static final String WHITESPACE = "  ";
    private static final String TAB = "\t";
    private static final String NEWLINE = "\n";
    private static final String INVALID_VALUE = "invalid";
    private static final String EMPTY_STRING = "";
    private static final String WHITESPACE_ONLY = "   ";
    private static final String PARTIAL_VALUE = "bet";

    public void testToString() {
        assertThat(StatusHeuristic.BETA.toString(), equalTo(BETA_STR));
        assertThat(StatusHeuristic.PREVIEW.toString(), equalTo(PREVIEW_STR));
        assertThat(StatusHeuristic.GA.toString(), equalTo(GA_STR));
        assertThat(StatusHeuristic.DEPRECATED.toString(), equalTo(DEPRECATED_STR));
    }

    public void testFromStringLowercase() {
        assertThat(StatusHeuristic.fromString(BETA_STR), is(StatusHeuristic.BETA));
        assertThat(StatusHeuristic.fromString(PREVIEW_STR), is(StatusHeuristic.PREVIEW));
        assertThat(StatusHeuristic.fromString(GA_STR), is(StatusHeuristic.GA));
        assertThat(StatusHeuristic.fromString(DEPRECATED_STR), is(StatusHeuristic.DEPRECATED));
    }

    public void testFromStringMixedCase() {
        assertThat(StatusHeuristic.fromString("Beta"), is(StatusHeuristic.BETA));
        assertThat(StatusHeuristic.fromString("Preview"), is(StatusHeuristic.PREVIEW));
        assertThat(StatusHeuristic.fromString("Ga"), is(StatusHeuristic.GA));
        assertThat(StatusHeuristic.fromString("Deprecated"), is(StatusHeuristic.DEPRECATED));
        assertThat(StatusHeuristic.fromString("bEtA"), is(StatusHeuristic.BETA));
        assertThat(StatusHeuristic.fromString("PrEvIeW"), is(StatusHeuristic.PREVIEW));
    }

    public void testFromStringWithWhitespace() {
        assertThat(StatusHeuristic.fromString(WHITESPACE + BETA_STR + WHITESPACE), is(StatusHeuristic.BETA));
        assertThat(StatusHeuristic.fromString(TAB + PREVIEW_STR + TAB), is(StatusHeuristic.PREVIEW));
        assertThat(StatusHeuristic.fromString(NEWLINE + WHITESPACE + GA_STR + WHITESPACE + NEWLINE), is(StatusHeuristic.GA));
        assertThat(StatusHeuristic.fromString(WHITESPACE + DEPRECATED_STR + WHITESPACE), is(StatusHeuristic.DEPRECATED));
        assertThat(StatusHeuristic.fromString(WHITESPACE + BETA_CAPS_STR + WHITESPACE), is(StatusHeuristic.BETA));
    }

    public void testFromStringInvalidValue() {
        // fromString() should throw IllegalArgumentException for invalid values
        expectThrows(IllegalArgumentException.class, () -> StatusHeuristic.fromString(INVALID_VALUE));
        expectThrows(IllegalArgumentException.class, () -> StatusHeuristic.fromString(EMPTY_STRING));
        expectThrows(IllegalArgumentException.class, () -> StatusHeuristic.fromString(WHITESPACE_ONLY));
        expectThrows(IllegalArgumentException.class, () -> StatusHeuristic.fromString(PARTIAL_VALUE));
    }

    public void testRoundTrip() {
        for (var status : StatusHeuristic.values()) {
            var stringValue = status.toString();
            var parsed = StatusHeuristic.fromString(stringValue);
            assertThat(parsed, is(status));
        }
    }

    public void testRoundTripWithWhitespace() {
        for (var status : StatusHeuristic.values()) {
            var stringValue = WHITESPACE + status.toString() + WHITESPACE;
            var parsed = StatusHeuristic.fromString(stringValue);
            assertThat(parsed, is(status));
        }
    }
}
