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
import org.elasticsearch.common.Strings;
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

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2)
public class RestBulkActionIT extends ESIntegTestCase {
    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    public void testBulkIndexWithSourceOnErrorDisabled() throws Exception {
        var source = "{\"field\": \"index\",}";
        var sourceEscaped = "{\\\"field\\\": \\\"index\\\",}";

        var request = new Request("PUT", "/test_index/_bulk");
        request.setJsonEntity(Strings.format("{\"index\":{\"_id\":\"1\"}}\n%s\n", source));

        Response response = getRestClient().performRequest(request);
        String responseContent = Streams.copyToString(new InputStreamReader(response.getEntity().getContent(), UTF_8));
        assertThat(responseContent, containsString(sourceEscaped));

        request.addParameter(RestUtils.INCLUDE_SOURCE_ON_ERROR_PARAMETER, "false");

        response = getRestClient().performRequest(request);
        responseContent = Streams.copyToString(new InputStreamReader(response.getEntity().getContent(), UTF_8));
        assertThat(
            responseContent,
            both(not(containsString(sourceEscaped))).and(
                containsString("REDACTED (`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION` disabled)")
            )
        );
    }

    public void testBulkUpdateWithSourceOnErrorDisabled() throws Exception {
        var source = "{\"field\": \"index\",}";
        var sourceEscaped = "{\\\"field\\\": \\\"index\\\",}";

        var request = new Request("PUT", "/test_index/_bulk");
        request.addParameter(RestUtils.INCLUDE_SOURCE_ON_ERROR_PARAMETER, "false");
        request.setJsonEntity(Strings.format("{\"update\":{\"_id\":\"1\"}}\n{\"doc\":%s}}\n", source));

        // note: this behavior is not consistent with bulk index actions
        // In case of updates by doc, the source is eagerly parsed and will fail the entire request if it cannot be parsed
        var exception = assertThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        String response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));

        assertThat(
            response,
            both(not(containsString(sourceEscaped))).and(
                containsString("REDACTED (`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION` disabled)")
            )
        );
    }

    public void testBulkSliceRequiredWhenIndexSettingEnabled() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        final String index = "bulk-slice-required";
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true
              }
            }""");
        getRestClient().performRequest(create);

        Request bulk = new Request("POST", "/" + index + "/_bulk");
        bulk.setJsonEntity("""
            {"index":{"_id":"1"}}
            {"field":"value1"}
            """);
        ResponseException exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(bulk));
        String response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(response, containsString("[_slice] is required when [index.slice.enabled] is true"));

        Request bulkWithSlice = new Request("POST", "/" + index + "/_bulk");
        bulkWithSlice.addParameter("_slice", "s1");
        bulkWithSlice.setJsonEntity("""
            {"index":{"_id":"2"}}
            {"field":"value2"}
            """);
        ObjectPath objectPath = ObjectPath.createFromResponse(getRestClient().performRequest(bulkWithSlice));
        assertThat(objectPath.evaluate("errors"), equalTo(false));
    }

    public void testBulkSliceRequiredForIndexUpdateDeleteWhenIndexSettingEnabled() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        final String index = "bulk-slice-required-iud";
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true
              }
            }""");
        getRestClient().performRequest(create);

        Request bulk = new Request("POST", "/" + index + "/_bulk");
        bulk.setJsonEntity("""
            {"index":{"_id":"1"}}
            {"field":"value1"}
            {"update":{"_id":"1"}}
            {"doc":{"field":"value2"}}
            {"delete":{"_id":"1"}}
            """);
        ResponseException exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(bulk));
        String response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(response, containsString("[_slice] is required when [index.slice.enabled] is true"));
    }

    public void testBulkRoutingRejectedWhenSliceSettingEnabled() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        final String index = "bulk-slice-routing-rejected";
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true
              }
            }""");
        getRestClient().performRequest(create);

        Request topLevelRouting = new Request("POST", "/" + index + "/_bulk");
        topLevelRouting.addParameter("routing", "r1");
        topLevelRouting.setJsonEntity("""
            {"index":{"_id":"1"}}
            {"field":"value1"}
            """);
        ResponseException topLevelRoutingException = expectThrows(
            ResponseException.class,
            () -> getRestClient().performRequest(topLevelRouting)
        );
        String topLevelRoutingResponse = Streams.copyToString(
            new InputStreamReader(topLevelRoutingException.getResponse().getEntity().getContent(), UTF_8)
        );
        assertThat(topLevelRoutingResponse, containsString("[routing] is not allowed when [index.slice.enabled] is true"));
        assertThat(topLevelRoutingResponse, containsString("use [_slice] instead"));

        Request itemRouting = new Request("POST", "/" + index + "/_bulk");
        itemRouting.setJsonEntity("""
            {"index":{"_id":"2","routing":"r2"}}
            {"field":"value2"}
            """);
        ResponseException itemRoutingException = expectThrows(ResponseException.class, () -> getRestClient().performRequest(itemRouting));
        String itemRoutingResponse = Streams.copyToString(
            new InputStreamReader(itemRoutingException.getResponse().getEntity().getContent(), UTF_8)
        );
        assertThat(itemRoutingResponse, containsString("[routing] is not allowed when [index.slice.enabled] is true"));
        assertThat(itemRoutingResponse, containsString("use [_slice] instead"));
    }

    public void testBulkSliceRequiredWhenWritingViaAlias() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        final String backingIndex = "bulk-slice-alias-000001";
        final String alias = "bulk-slice-alias";
        Request create = new Request("PUT", "/" + backingIndex);
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true
              }
            }""");
        getRestClient().performRequest(create);

        Request addAlias = new Request("POST", "/_aliases");
        addAlias.setJsonEntity("""
            {
              "actions": [
                {
                  "add": {
                    "index": "bulk-slice-alias-000001",
                    "alias": "bulk-slice-alias",
                    "is_write_index": true
                  }
                }
              ]
            }""");
        getRestClient().performRequest(addAlias);

        Request bulkMissingSlice = new Request("POST", "/" + alias + "/_bulk");
        bulkMissingSlice.setJsonEntity("""
            {"index":{"_id":"1"}}
            {"field":"value1"}
            """);
        ResponseException exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(bulkMissingSlice));
        String response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(response, containsString("[_slice] is required when [index.slice.enabled] is true"));

        Request bulkWithSlice = new Request("POST", "/" + alias + "/_bulk");
        bulkWithSlice.addParameter("_slice", "s1");
        bulkWithSlice.setJsonEntity("""
            {"index":{"_id":"2"}}
            {"field":"value2"}
            """);
        ObjectPath objectPath = ObjectPath.createFromResponse(getRestClient().performRequest(bulkWithSlice));
        assertThat(objectPath.evaluate("errors"), equalTo(false));
    }

    public void testBulkSliceIndexUpdateDeleteFlow() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        final String index = "bulk-slice-iud";
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true
              }
            }""");
        getRestClient().performRequest(create);

        Request bulk = new Request("POST", "/" + index + "/_bulk");
        bulk.addParameter("_slice", "s1");
        bulk.setJsonEntity("""
            {"index":{"_id":"1"}}
            {"field":"value1"}
            {"update":{"_id":"1"}}
            {"doc":{"field":"value2"}}
            {"delete":{"_id":"1"}}
            """);
        ObjectPath bulkResponsePath = ObjectPath.createFromResponse(getRestClient().performRequest(bulk));
        assertThat(bulkResponsePath.evaluate("errors"), equalTo(false));
        assertThat(bulkResponsePath.evaluate("items.0.index.result"), equalTo("created"));
        assertThat(bulkResponsePath.evaluate("items.1.update.result"), equalTo("updated"));
        assertThat(bulkResponsePath.evaluate("items.2.delete.result"), equalTo("deleted"));

        Request getDeletedDoc = new Request("GET", "/" + index + "/_doc/1");
        getDeletedDoc.addParameter("_slice", "s1");
        ResponseException getException = expectThrows(ResponseException.class, () -> getRestClient().performRequest(getDeletedDoc));
        String getResponse = Streams.copyToString(new InputStreamReader(getException.getResponse().getEntity().getContent(), UTF_8));
        assertThat(getResponse, containsString("\"found\":false"));
    }

    public void testBulkSliceParamRejectedWhenFeatureFlagDisabled() throws Exception {
        assumeFalse("slice indexing feature flag must be disabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request bulk = new Request("POST", "/test_index/_bulk");
        bulk.addParameter("_slice", "s1");
        bulk.setJsonEntity("""
            {"index":{"_id":"1"}}
            {"field":"value1"}
            """);
        ResponseException exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(bulk));
        String response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(response, containsString("request does not support [_slice]"));
    }

    public void testBulkSliceRejectedWhenSettingDisabled() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        final String index = "bulk-slice-disabled";
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": false
              }
            }""");
        getRestClient().performRequest(create);

        Request bulk = new Request("POST", "/" + index + "/_bulk");
        bulk.addParameter("_slice", "s1");
        bulk.setJsonEntity("""
            {"index":{"_id":"1"}}
            {"field":"value1"}
            """);
        ResponseException exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(bulk));
        String response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(response, containsString("[_slice] is not allowed when [index.slice.enabled] is false"));
    }
}
