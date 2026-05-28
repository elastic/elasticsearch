/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.InputStreamReader;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class RestUpdateActionIT extends ESIntegTestCase {
    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    public void testUpdateByDocWithSourceOnErrorDisabled() throws Exception {
        var updateRequest = "{\"doc\":{\"field\": \"value}}";
        var sourceEscaped = "{\\\"field\\\": \\\"value}";

        var request = new Request("POST", "/test_index/_update/1");
        request.setJsonEntity(updateRequest);

        var exception = assertThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        String response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(response, containsString(sourceEscaped));

        // disable source on error
        request.addParameter(RestUtils.INCLUDE_SOURCE_ON_ERROR_PARAMETER, "false");
        exception = assertThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(
            response,
            both(not(containsString(sourceEscaped))).and(
                containsString("REDACTED (`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION` disabled)")
            )
        );
    }

    public void testUpdateSliceValidationAndRequirement() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request create = new Request("PUT", "/slice-update-it");
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true
              }
            }""");
        getRestClient().performRequest(create);

        Request seedDoc = new Request("POST", "/slice-update-it/_doc/1");
        seedDoc.addParameter("_slice", "s1");
        seedDoc.setJsonEntity("""
            {
              "field": "value"
            }""");
        getRestClient().performRequest(seedDoc);

        Request missingSlice = new Request("POST", "/slice-update-it/_update/1");
        missingSlice.setJsonEntity("""
            {
              "doc": {
                "field": "updated"
              }
            }""");
        ResponseException missingSliceException = expectThrows(ResponseException.class, () -> getRestClient().performRequest(missingSlice));
        String missingSliceBody = Streams.copyToString(
            new InputStreamReader(missingSliceException.getResponse().getEntity().getContent(), UTF_8)
        );
        assertThat(missingSliceBody, containsString("[_slice] is required when [index.slice.enabled] is true"));

        Request invalidSlice = new Request("POST", "/slice-update-it/_update/1");
        invalidSlice.addParameter("_slice", "_all");
        invalidSlice.setJsonEntity("""
            {
              "doc": {
                "field": "updated"
              }
            }""");
        ResponseException invalidSliceException = expectThrows(ResponseException.class, () -> getRestClient().performRequest(invalidSlice));
        String invalidSliceBody = Streams.copyToString(
            new InputStreamReader(invalidSliceException.getResponse().getEntity().getContent(), UTF_8)
        );
        assertThat(invalidSliceBody, containsString("invalid [_slice] value"));

        Request routingProvided = new Request("POST", "/slice-update-it/_update/1");
        routingProvided.addParameter("routing", "s1");
        routingProvided.setJsonEntity("""
            {
              "doc": {
                "field": "updated"
              }
            }""");
        ResponseException routingProvidedException = expectThrows(
            ResponseException.class,
            () -> getRestClient().performRequest(routingProvided)
        );
        String routingProvidedBody = Streams.copyToString(
            new InputStreamReader(routingProvidedException.getResponse().getEntity().getContent(), UTF_8)
        );
        assertThat(routingProvidedBody, containsString("[routing] is not allowed when [index.slice.enabled] is true"));

        Request validSlice = new Request("POST", "/slice-update-it/_update/1");
        validSlice.addParameter("_slice", "s1");
        validSlice.setJsonEntity("""
            {
              "doc": {
                "field": "updated"
              }
            }""");
        Response response = getRestClient().performRequest(validSlice);
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        assertThat(objectPath.evaluate("result"), equalTo("updated"));
    }

    public void testUpdateSliceRejectedWhenSettingDisabled() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request create = new Request("PUT", "/slice-update-disabled");
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": false
              }
            }""");
        getRestClient().performRequest(create);

        Request seedDoc = new Request("POST", "/slice-update-disabled/_doc/1");
        seedDoc.setJsonEntity("""
            {
              "field": "value"
            }""");
        getRestClient().performRequest(seedDoc);

        Request request = new Request("POST", "/slice-update-disabled/_update/1");
        request.addParameter("_slice", "s1");
        request.setJsonEntity("""
            {
              "doc": {
                "field": "updated"
              }
            }""");
        ResponseException exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        String response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(response, containsString("[_slice] is not allowed when [index.slice.enabled] is false"));
    }

    public void testUpdateSliceParamRejectedWhenFeatureFlagDisabled() throws Exception {
        assumeFalse("slice indexing feature flag must be disabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request request = new Request("POST", "/test_index/_update/1");
        request.addParameter("_slice", "s1");
        request.setJsonEntity("""
            {
              "doc": {
                "field": "updated"
              }
            }""");
        ResponseException exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        String response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(response, containsString("request does not support [_slice]"));
    }
}
