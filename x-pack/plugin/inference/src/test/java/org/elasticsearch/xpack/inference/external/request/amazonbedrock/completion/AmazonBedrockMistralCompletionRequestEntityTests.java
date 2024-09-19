/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion.AmazonBedrockConverseRequestUtils.doesConverseRequestHasMessage;
import static org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion.AmazonBedrockConverseRequestUtils.doesConverseRequestHaveAnyMaxTokensInput;
import static org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion.AmazonBedrockConverseRequestUtils.doesConverseRequestHaveAnyTemperatureInput;
import static org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion.AmazonBedrockConverseRequestUtils.doesConverseRequestHaveAnyTopKInput;
import static org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion.AmazonBedrockConverseRequestUtils.doesConverseRequestHaveAnyTopPInput;
import static org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion.AmazonBedrockConverseRequestUtils.doesConverseRequestHaveMaxTokensInput;
import static org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion.AmazonBedrockConverseRequestUtils.doesConverseRequestHaveTemperatureInput;
import static org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion.AmazonBedrockConverseRequestUtils.doesConverseRequestHaveTopKInput;
import static org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion.AmazonBedrockConverseRequestUtils.doesConverseRequestHaveTopPInput;
import static org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion.AmazonBedrockConverseRequestUtils.getConverseRequest;
import static org.hamcrest.Matchers.is;

public class AmazonBedrockMistralCompletionRequestEntityTests extends ESTestCase {
    public void testRequestEntity_CreatesProperRequest() {
        var request = new AmazonBedrockMistralCompletionRequestEntity(List.of("test message"), null, null, null, null);
        var builtRequest = getConverseRequest("testmodel", request);
        assertThat(builtRequest.getModelId(), is("testmodel"));
        assertThat(doesConverseRequestHasMessage(builtRequest, "test message"), is(true));
        assertThat(builtRequest.getModelId(), is("testmodel"));
        assertFalse(doesConverseRequestHaveAnyTemperatureInput(builtRequest));
        assertFalse(doesConverseRequestHaveAnyTopPInput(builtRequest));
        assertFalse(doesConverseRequestHaveAnyTopKInput(builtRequest));
        assertFalse(doesConverseRequestHaveAnyMaxTokensInput(builtRequest));
    }

    public void testRequestEntity_CreatesProperRequest_WithTemperature() {
        var request = new AmazonBedrockMistralCompletionRequestEntity(List.of("test message"), 1.0, null, null, null);
        var builtRequest = getConverseRequest("testmodel", request);
        assertThat(builtRequest.getModelId(), is("testmodel"));
        assertThat(doesConverseRequestHasMessage(builtRequest, "test message"), is(true));
        assertTrue(doesConverseRequestHaveTemperatureInput(builtRequest, 1.0));
        assertFalse(doesConverseRequestHaveAnyTopPInput(builtRequest));
        assertFalse(doesConverseRequestHaveAnyTopKInput(builtRequest));
        assertFalse(doesConverseRequestHaveAnyMaxTokensInput(builtRequest));
    }

    public void testRequestEntity_CreatesProperRequest_WithTopP() {
        var request = new AmazonBedrockMistralCompletionRequestEntity(List.of("test message"), null, 1.0, null, null);
        var builtRequest = getConverseRequest("testmodel", request);
        assertThat(builtRequest.getModelId(), is("testmodel"));
        assertThat(doesConverseRequestHasMessage(builtRequest, "test message"), is(true));
        assertFalse(doesConverseRequestHaveAnyTemperatureInput(builtRequest));
        assertTrue(doesConverseRequestHaveTopPInput(builtRequest, 1.0));
        assertFalse(doesConverseRequestHaveAnyTopKInput(builtRequest));
        assertFalse(doesConverseRequestHaveAnyMaxTokensInput(builtRequest));
    }

    public void testRequestEntity_CreatesProperRequest_WithMaxTokens() {
        var request = new AmazonBedrockMistralCompletionRequestEntity(List.of("test message"), null, null, null, 128);
        var builtRequest = getConverseRequest("testmodel", request);
        assertThat(builtRequest.getModelId(), is("testmodel"));
        assertThat(doesConverseRequestHasMessage(builtRequest, "test message"), is(true));
        assertFalse(doesConverseRequestHaveAnyTemperatureInput(builtRequest));
        assertFalse(doesConverseRequestHaveAnyTopPInput(builtRequest));
        assertFalse(doesConverseRequestHaveAnyTopKInput(builtRequest));
        assertTrue(doesConverseRequestHaveMaxTokensInput(builtRequest, 128));
    }

    public void testRequestEntity_CreatesProperRequest_WithTopK() {
        var request = new AmazonBedrockMistralCompletionRequestEntity(List.of("test message"), null, null, 1.0, null);
        var builtRequest = getConverseRequest("testmodel", request);
        assertThat(builtRequest.getModelId(), is("testmodel"));
        assertThat(doesConverseRequestHasMessage(builtRequest, "test message"), is(true));
        assertFalse(doesConverseRequestHaveAnyTemperatureInput(builtRequest));
        assertFalse(doesConverseRequestHaveAnyTopPInput(builtRequest));
        assertTrue(doesConverseRequestHaveTopKInput(builtRequest, 1.0));
        assertFalse(doesConverseRequestHaveAnyMaxTokensInput(builtRequest));
    }
}
