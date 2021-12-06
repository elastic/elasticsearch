/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;

public class SuggestingErrorOnUnknownTests extends ESTestCase {
    private String errorMessage(String unknownField, String... candidates) {
        return new SuggestingErrorOnUnknown().errorMessage("test", unknownField, Arrays.asList(candidates));
    }

    public void testNoCandidates() {
        assertThat(errorMessage("foo"), equalTo("[test] unknown field [foo]"));
    }

    public void testBadCandidates() {
        assertThat(errorMessage("foo", "bar", "baz"), equalTo("[test] unknown field [foo]"));
    }

    public void testOneCandidate() {
        assertThat(errorMessage("foo", "bar", "fop"), equalTo("[test] unknown field [foo] did you mean [fop]?"));
    }

    public void testManyCandidate() {
        assertThat(errorMessage("foo", "bar", "fop", "fou", "baz"), equalTo("[test] unknown field [foo] did you mean any of [fop, fou]?"));
    }
}
