/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;

public class ErrorExcerptsTests extends ESTestCase {

    public void testShortValuePassesThrough() {
        assertEquals("hello", ErrorExcerpts.summarize("hello"));
        assertEquals("null", ErrorExcerpts.summarize(null));
        String atCap = "x".repeat(ErrorExcerpts.MAX_EXCERPT_CHARS);
        assertSame(atCap, ErrorExcerpts.summarize(atCap));
    }

    public void testLongValueIsCappedKeepingBothEnds() {
        String huge = "H" + "x".repeat(5_000) + "T";
        String summarized = ErrorExcerpts.summarize(huge);
        assertThat(summarized.length(), lessThanOrEqualTo(ErrorExcerpts.MAX_EXCERPT_CHARS));
        assertThat(summarized, containsString("(truncated, 5002 chars total)"));
        assertThat(summarized, startsWith("H"));
        assertThat(summarized, endsWith("T"));
    }

    public void testOneCharOverCapIsCapped() {
        String overCap = "x".repeat(ErrorExcerpts.MAX_EXCERPT_CHARS + 1);
        String summarized = ErrorExcerpts.summarize(overCap);
        assertThat(summarized.length(), lessThanOrEqualTo(ErrorExcerpts.MAX_EXCERPT_CHARS));
        assertThat(summarized, containsString("(truncated, " + overCap.length() + " chars total)"));
    }
}
