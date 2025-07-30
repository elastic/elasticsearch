/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioEndpointType;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;

public class AzureAiStudioChatCompletionRequestEntityTests extends ESTestCase {

    public void testToXContent_WhenTokenEndpoint_NoParameters() throws IOException {
        var entity = new AzureAiStudioChatCompletionRequestEntity(
            List.of("abc"),
            AzureAiStudioEndpointType.TOKEN,
            null,
            null,
            null,
            null,
            false
        );
        var request = getXContentAsString(entity);
        var expectedRequest = getExpectedTokenEndpointRequest(List.of("abc"), null, null, null, null);
        assertThat(request, is(expectedRequest));
    }

    public void testToXContent_WhenTokenEndpoint_WithTemperatureParam() throws IOException {
        var entity = new AzureAiStudioChatCompletionRequestEntity(
            List.of("abc"),
            AzureAiStudioEndpointType.TOKEN,
            1.0,
            null,
            null,
            null,
            false
        );
        var request = getXContentAsString(entity);
        var expectedRequest = getExpectedTokenEndpointRequest(List.of("abc"), 1.0, null, null, null);
        assertThat(request, is(expectedRequest));
    }

    public void testToXContent_WhenTokenEndpoint_WithTopPParam() throws IOException {
        var entity = new AzureAiStudioChatCompletionRequestEntity(
            List.of("abc"),
            AzureAiStudioEndpointType.TOKEN,
            null,
            2.0,
            null,
            null,
            false
        );
        var request = getXContentAsString(entity);
        var expectedRequest = getExpectedTokenEndpointRequest(List.of("abc"), null, 2.0, null, null);
        assertThat(request, is(expectedRequest));
    }

    public void testToXContent_WhenTokenEndpoint_WithDoSampleParam() throws IOException {
        var entity = new AzureAiStudioChatCompletionRequestEntity(
            List.of("abc"),
            AzureAiStudioEndpointType.TOKEN,
            null,
            null,
            true,
            null,
            false
        );
        var request = getXContentAsString(entity);
        var expectedRequest = getExpectedTokenEndpointRequest(List.of("abc"), null, null, true, null);
        assertThat(request, is(expectedRequest));
    }

    public void testToXContent_WhenTokenEndpoint_WithMaxNewTokensParam() throws IOException {
        var entity = new AzureAiStudioChatCompletionRequestEntity(
            List.of("abc"),
            AzureAiStudioEndpointType.TOKEN,
            null,
            null,
            null,
            512,
            false
        );
        var request = getXContentAsString(entity);
        var expectedRequest = getExpectedTokenEndpointRequest(List.of("abc"), null, null, null, 512);
        assertThat(request, is(expectedRequest));
    }

    public void testToXContent_WhenRealtimeEndpoint_NoParameters() throws IOException {
        var entity = new AzureAiStudioChatCompletionRequestEntity(
            List.of("abc"),
            AzureAiStudioEndpointType.REALTIME,
            null,
            null,
            null,
            null,
            false
        );
        var request = getXContentAsString(entity);
        var expectedRequest = getExpectedRealtimeEndpointRequest(List.of("abc"), null, null, null, null);
        assertThat(request, is(expectedRequest));
    }

    public void testToXContent_WhenRealtimeEndpoint_WithTemperatureParam() throws IOException {
        var entity = new AzureAiStudioChatCompletionRequestEntity(
            List.of("abc"),
            AzureAiStudioEndpointType.REALTIME,
            1.0,
            null,
            null,
            null,
            false
        );
        var request = getXContentAsString(entity);
        var expectedRequest = getExpectedRealtimeEndpointRequest(List.of("abc"), 1.0, null, null, null);
        assertThat(request, is(expectedRequest));
    }

    public void testToXContent_WhenRealtimeEndpoint_WithTopPParam() throws IOException {
        var entity = new AzureAiStudioChatCompletionRequestEntity(
            List.of("abc"),
            AzureAiStudioEndpointType.REALTIME,
            null,
            2.0,
            null,
            null,
            false
        );
        var request = getXContentAsString(entity);
        var expectedRequest = getExpectedRealtimeEndpointRequest(List.of("abc"), null, 2.0, null, null);
        assertThat(request, is(expectedRequest));
    }

    public void testToXContent_WhenRealtimeEndpoint_WithDoSampleParam() throws IOException {
        var entity = new AzureAiStudioChatCompletionRequestEntity(
            List.of("abc"),
            AzureAiStudioEndpointType.REALTIME,
            null,
            null,
            true,
            null,
            false
        );
        var request = getXContentAsString(entity);
        var expectedRequest = getExpectedRealtimeEndpointRequest(List.of("abc"), null, null, true, null);
        assertThat(request, is(expectedRequest));
    }

    public void testToXContent_WhenRealtimeEndpoint_WithMaxNewTokensParam() throws IOException {
        var entity = new AzureAiStudioChatCompletionRequestEntity(
            List.of("abc"),
            AzureAiStudioEndpointType.REALTIME,
            null,
            null,
            null,
            512,
            false
        );
        var request = getXContentAsString(entity);
        var expectedRequest = getExpectedRealtimeEndpointRequest(List.of("abc"), null, null, null, 512);
        assertThat(request, is(expectedRequest));
    }

    private String getXContentAsString(AzureAiStudioChatCompletionRequestEntity entity) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        return Strings.toString(builder);
    }

    private String getExpectedTokenEndpointRequest(
        List<String> inputs,
        @Nullable Double temperature,
        @Nullable Double topP,
        @Nullable Boolean doSample,
        @Nullable Integer maxNewTokens
    ) {
        String expected = "{";

        expected = addMessageInputs("messages", expected, inputs);
        expected = addParameters(expected, temperature, topP, doSample, maxNewTokens);

        expected += "}";
        return expected;
    }

    private String getExpectedRealtimeEndpointRequest(
        List<String> inputs,
        @Nullable Double temperature,
        @Nullable Double topP,
        @Nullable Boolean doSample,
        @Nullable Integer maxNewTokens
    ) {
        String expected = "{\"input_data\":{";

        expected = addMessageInputs("input_string", expected, inputs);
        expected = addParameters(expected, temperature, topP, doSample, maxNewTokens);

        expected += "}}";
        return expected;
    }

    private String addMessageInputs(String fieldName, String expected, List<String> inputs) {
        StringBuilder messages = new StringBuilder(Strings.format("\"%s\":[", fieldName));
        var hasOne = false;
        for (String input : inputs) {
            if (hasOne) {
                messages.append(",");
            }
            messages.append(getMessageString(input));
            hasOne = true;
        }
        messages.append("]");

        return expected + messages;
    }

    private String getMessageString(String input) {
        return Strings.format("{\"content\":\"%s\",\"role\":\"user\"}", input);
    }

    private String addParameters(String expected, Double temperature, Double topP, Boolean doSample, Integer maxNewTokens) {
        if (temperature == null && topP == null && doSample == null && maxNewTokens == null) {
            return expected;
        }

        StringBuilder parameters = new StringBuilder(",\"parameters\":{");

        var hasOne = false;
        if (temperature != null) {
            parameters.append(Strings.format("\"temperature\":%.1f", temperature));
            hasOne = true;
        }

        if (topP != null) {
            if (hasOne) {
                parameters.append(",");
            }
            parameters.append(Strings.format("\"top_p\":%.1f", topP));
            hasOne = true;
        }

        if (doSample != null) {
            if (hasOne) {
                parameters.append(",");
            }
            parameters.append(Strings.format("\"do_sample\":%s", doSample.equals(Boolean.TRUE)));
            hasOne = true;
        }

        if (maxNewTokens != null) {
            if (hasOne) {
                parameters.append(",");
            }
            parameters.append(Strings.format("\"max_new_tokens\":%d", maxNewTokens));
        }

        parameters.append("}");

        return expected + parameters;
    }
}
