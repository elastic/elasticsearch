/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.process.autodetect.state;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class CategorizerStateTests extends ESTestCase {

    public void testExtractJobId_GivenValidDocId() {
        assertThat(CategorizerState.extractJobId("foo_categorizer_state#1"), equalTo("foo"));
        assertThat(CategorizerState.extractJobId("bar_categorizer_state#2"), equalTo("bar"));
        assertThat(CategorizerState.extractJobId("foo_bar_categorizer_state#3"), equalTo("foo_bar"));
        assertThat(CategorizerState.extractJobId("_categorizer_state_categorizer_state#3"), equalTo("_categorizer_state"));
    }

    public void testExtractJobId_GivenInvalidDocId() {
        assertThat(CategorizerState.extractJobId(""), is(nullValue()));
        assertThat(CategorizerState.extractJobId("foo"), is(nullValue()));
        assertThat(CategorizerState.extractJobId("_categorizer_state"), is(nullValue()));
        assertThat(CategorizerState.extractJobId("foo_model_state_3141341341"), is(nullValue()));
    }
}