/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.containsString;

public class AssertWarningsTests extends ESTestCase {

    /**
     * The context is the whole query response. Rendering it for every allowed warning ran the generative tests out of heap
     * (https://github.com/elastic/elasticsearch/issues/158881), so it may only be rendered for an unexpected warning.
     */
    public void testAllowedRegexesDoesNotRenderContextForAllowedWarnings() {
        Object context = new Object() {
            @Override
            public String toString() {
                throw new AssertionError("context rendered for allowed warnings");
            }
        };
        new AssertWarnings.AllowedRegexes(List.of(Pattern.compile(".*"))).assertWarnings(List.of("a", "b"), context);
    }

    public void testAllowedRegexesReportsUnexpectedWarningWithContext() {
        AssertionError e = expectThrows(
            AssertionError.class,
            () -> new AssertWarnings.AllowedRegexes(List.of(Pattern.compile("a"))).assertWarnings(List.of("a", "b"), "the response")
        );
        assertThat(e.getMessage(), containsString("Context: the response"));
        assertThat(e.getMessage(), containsString("Unexpected warning: b"));
    }
}
