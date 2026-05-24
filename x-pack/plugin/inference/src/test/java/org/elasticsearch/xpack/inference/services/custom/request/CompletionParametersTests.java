/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.request;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.custom.request.RequestParameters.INPUT;
import static org.hamcrest.Matchers.is;

public class CompletionParametersTests extends ESTestCase {

    public void testJsonParameters_SingleValue() {
        var parameters = CompletionParameters.of(new ChatCompletionInput(List.of("hello")));
        assertThat(parameters.jsonParameters(), is(Map.of(INPUT, "\"hello\"")));
    }

    public void testJsonParameters_RetrievesFirstEntryFromList() {
        var parameters = CompletionParameters.of(new ChatCompletionInput(List.of("hello", "hi")));
        assertThat(parameters.jsonParameters(), is(Map.of(INPUT, "\"hello\"")));
    }

    public void testJsonParameters_EmptyList() {
        var parameters = CompletionParameters.of(new ChatCompletionInput(List.of()));
        assertThat(parameters.jsonParameters(), is(Map.of(INPUT, "\"\"")));
    }
}
