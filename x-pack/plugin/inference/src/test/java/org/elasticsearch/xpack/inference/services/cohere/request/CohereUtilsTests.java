/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.request;

import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.MatcherAssert;

import static org.hamcrest.Matchers.is;

public class CohereUtilsTests extends ESTestCase {

    public void testCreateRequestSourceHeader() {
        var requestSourceHeader = CohereUtils.createRequestSourceHeader();

        assertThat(requestSourceHeader.getName(), is("Request-Source"));
        assertThat(requestSourceHeader.getValue(), is("unspecified:elasticsearch"));
    }

    public void testInputTypeToString() {
        assertThat(CohereUtils.inputTypeToString(InputType.INGEST), is("search_document"));
        assertThat(CohereUtils.inputTypeToString(InputType.INTERNAL_INGEST), is("search_document"));
        assertThat(CohereUtils.inputTypeToString(InputType.SEARCH), is("search_query"));
        assertThat(CohereUtils.inputTypeToString(InputType.INTERNAL_SEARCH), is("search_query"));
        assertThat(CohereUtils.inputTypeToString(InputType.CLASSIFICATION), is("classification"));
        assertThat(CohereUtils.inputTypeToString(InputType.CLUSTERING), is("clustering"));
        assertThat(InputType.values().length, is(7)); // includes unspecified. Fail if new values are added
    }

    public void testInputTypeToString_ThrowsAssertionFailure_WhenInputTypeIsUnspecified() {
        var thrownException = expectThrows(AssertionError.class, () -> CohereUtils.inputTypeToString(InputType.UNSPECIFIED));
        MatcherAssert.assertThat(thrownException.getMessage(), is("received invalid input type value [unspecified]"));
    }
}
