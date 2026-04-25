/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.document;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class RestIndexActionIT extends ESIntegTestCase {
    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    public void testIndexWithSourceOnErrorDisabled() throws Exception {
        var source = "{\"field\": \"value}";
        var sourceEscaped = "{\\\"field\\\": \\\"value}";

        var request = new Request("POST", "/test_index/_doc/1");
        request.setJsonEntity(source);

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

    public void testSliceValidationAndRequirement() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request create = new Request("PUT", "/slice-index-it");
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true
              }
            }""");
        getRestClient().performRequest(create);

        Request missingSlice = new Request("POST", "/slice-index-it/_doc/1");
        missingSlice.setJsonEntity("""
            {
              "field": "value"
            }""");
        ResponseException missingSliceException = expectThrows(ResponseException.class, () -> getRestClient().performRequest(missingSlice));
        String missingSliceBody = Streams.copyToString(
            new InputStreamReader(missingSliceException.getResponse().getEntity().getContent(), UTF_8)
        );
        assertThat(missingSliceBody, containsString("[_slice] is required when [index.slice.enabled] is true"));

        Request invalidSlice = new Request("POST", "/slice-index-it/_doc/2");
        invalidSlice.addParameter("_slice", "_all");
        invalidSlice.setJsonEntity("""
            {
              "field": "value"
            }""");
        ResponseException invalidSliceException = expectThrows(ResponseException.class, () -> getRestClient().performRequest(invalidSlice));
        String invalidSliceBody = Streams.copyToString(
            new InputStreamReader(invalidSliceException.getResponse().getEntity().getContent(), UTF_8)
        );
        assertThat(invalidSliceBody, containsString("invalid [_slice] value"));

        Request validSlice = new Request("POST", "/slice-index-it/_doc/3");
        validSlice.addParameter("_slice", "s1");
        validSlice.setJsonEntity("""
            {
              "field": "value"
            }""");
        Response response = getRestClient().performRequest(validSlice);
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        assertThat(objectPath.evaluate("result"), equalTo("created"));
    }

    public void testSliceRejectedWhenSettingDisabled() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request create = new Request("PUT", "/slice-index-disabled");
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": false
              }
            }""");
        getRestClient().performRequest(create);

        Request request = new Request("POST", "/slice-index-disabled/_doc/1");
        request.addParameter("_slice", "s1");
        request.setJsonEntity("""
            {
              "field": "value"
            }""");
        ResponseException exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        String response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(response, containsString("[_slice] is not allowed when [index.slice.enabled] is false"));
    }

    public void testSliceParamRejectedWhenFeatureFlagDisabled() throws Exception {
        assumeFalse("slice indexing feature flag must be disabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request request = new Request("POST", "/test_index/_doc/1");
        request.addParameter("_slice", "s1");
        request.setJsonEntity("""
            {
              "field": "value"
            }""");
        ResponseException exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        String response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(response, containsString("request does not support [_slice]"));
    }

    public void testSliceRequirementAndValidationForGetAndDelete() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request create = new Request("PUT", "/slice-get-delete-enabled");
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true
              }
            }""");
        getRestClient().performRequest(create);

        Request index = new Request("POST", "/slice-get-delete-enabled/_doc/1");
        index.addParameter("_slice", "s1");
        index.setJsonEntity("""
            {
              "field": "value"
            }""");
        getRestClient().performRequest(index);

        Request getMissingSlice = new Request("GET", "/slice-get-delete-enabled/_doc/1");
        ResponseException missingGetSliceException = expectThrows(
            ResponseException.class,
            () -> getRestClient().performRequest(getMissingSlice)
        );
        String missingGetSliceResponse = Streams.copyToString(
            new InputStreamReader(missingGetSliceException.getResponse().getEntity().getContent(), UTF_8)
        );
        assertThat(missingGetSliceResponse, containsString("[_slice] is required when [index.slice.enabled] is true"));

        Request getWithSlice = new Request("GET", "/slice-get-delete-enabled/_doc/1");
        getWithSlice.addParameter("_slice", "s1");
        Response getResponse = getRestClient().performRequest(getWithSlice);
        ObjectPath getResponsePath = ObjectPath.createFromResponse(getResponse);
        assertThat(getResponsePath.evaluate("found"), equalTo(true));

        Request deleteMissingSlice = new Request("DELETE", "/slice-get-delete-enabled/_doc/1");
        ResponseException missingDeleteSliceException = expectThrows(
            ResponseException.class,
            () -> getRestClient().performRequest(deleteMissingSlice)
        );
        String missingDeleteSliceResponse = Streams.copyToString(
            new InputStreamReader(missingDeleteSliceException.getResponse().getEntity().getContent(), UTF_8)
        );
        assertThat(missingDeleteSliceResponse, containsString("[_slice] is required when [index.slice.enabled] is true"));

        Request deleteWithSlice = new Request("DELETE", "/slice-get-delete-enabled/_doc/1");
        deleteWithSlice.addParameter("_slice", "s1");
        Response deleteResponse = getRestClient().performRequest(deleteWithSlice);
        ObjectPath deleteResponsePath = ObjectPath.createFromResponse(deleteResponse);
        assertThat(deleteResponsePath.evaluate("result"), equalTo("deleted"));
    }

    public void testSliceRejectedForGetAndDeleteWhenSettingDisabled() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request create = new Request("PUT", "/slice-get-delete-disabled");
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": false
              }
            }""");
        getRestClient().performRequest(create);

        Request index = new Request("POST", "/slice-get-delete-disabled/_doc/1");
        index.setJsonEntity("""
            {
              "field": "value"
            }""");
        getRestClient().performRequest(index);

        Request getWithSlice = new Request("GET", "/slice-get-delete-disabled/_doc/1");
        getWithSlice.addParameter("_slice", "s1");
        ResponseException getException = expectThrows(ResponseException.class, () -> getRestClient().performRequest(getWithSlice));
        String getResponse = Streams.copyToString(new InputStreamReader(getException.getResponse().getEntity().getContent(), UTF_8));
        assertThat(getResponse, containsString("[_slice] is not allowed when [index.slice.enabled] is false"));

        Request deleteWithSlice = new Request("DELETE", "/slice-get-delete-disabled/_doc/1");
        deleteWithSlice.addParameter("_slice", "s1");
        ResponseException deleteException = expectThrows(ResponseException.class, () -> getRestClient().performRequest(deleteWithSlice));
        String deleteResponse = Streams.copyToString(new InputStreamReader(deleteException.getResponse().getEntity().getContent(), UTF_8));
        assertThat(deleteResponse, containsString("[_slice] is not allowed when [index.slice.enabled] is false"));
    }

    public void testSliceBehaviorRespectsIndexTemplateSetting() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());

        final String disabledTemplateName = "slice-template-disabled-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        final String disabledPattern = "slice-template-disabled-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT) + "-*";
        final String disabledIndex = disabledPattern.replace("*", "000001");
        Request putDisabledTemplate = new Request("PUT", "/_index_template/" + disabledTemplateName);
        putDisabledTemplate.setJsonEntity(String.format(Locale.ROOT, """
            {
              "index_patterns": ["%s"],
              "template": {
                "settings": {
                  "index.slice.enabled": false
                }
              }
            }""", disabledPattern));
        getRestClient().performRequest(putDisabledTemplate);

        Request disabledSliceWrite = new Request("POST", "/" + disabledIndex + "/_doc/1");
        disabledSliceWrite.addParameter("_slice", "s1");
        disabledSliceWrite.setJsonEntity("""
            {
              "field": "value"
            }""");
        ResponseException disabledException = expectThrows(
            ResponseException.class,
            () -> getRestClient().performRequest(disabledSliceWrite)
        );
        String disabledResponse = Streams.copyToString(
            new InputStreamReader(disabledException.getResponse().getEntity().getContent(), UTF_8)
        );
        assertThat(disabledResponse, containsString("[_slice] is not allowed when [index.slice.enabled] is false"));

        final String enabledTemplateName = "slice-template-enabled-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        final String enabledPattern = "slice-template-enabled-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT) + "-*";
        final String enabledIndex = enabledPattern.replace("*", "000001");
        Request putEnabledTemplate = new Request("PUT", "/_index_template/" + enabledTemplateName);
        putEnabledTemplate.setJsonEntity(String.format(Locale.ROOT, """
            {
              "index_patterns": ["%s"],
              "template": {
                "settings": {
                  "index.slice.enabled": true
                }
              }
            }""", enabledPattern));
        getRestClient().performRequest(putEnabledTemplate);

        Request enabledSliceWrite = new Request("POST", "/" + enabledIndex + "/_doc/1");
        enabledSliceWrite.addParameter("_slice", "s1");
        enabledSliceWrite.setJsonEntity("""
            {
              "field": "value"
            }""");
        Response enabledResponse = getRestClient().performRequest(enabledSliceWrite);
        ObjectPath enabledPath = ObjectPath.createFromResponse(enabledResponse);
        assertThat(enabledPath.evaluate("result"), equalTo("created"));
    }

    public void testSliceProvenanceValidationViaCoordinatingOnlyNode() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        final String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        final RestClient restClient = getRestClient();
        final List<Node> originalNodes = restClient.getNodes();
        try {
            restClient.setNodes(
                Collections.singletonList(
                    new Node(
                        HttpHost.create(
                            internalCluster().getInstance(HttpServerTransport.class, coordinatorNode)
                                .boundAddress()
                                .publishAddress()
                                .toString()
                        )
                    )
                )
            );

            Request createEnabled = new Request("PUT", "/slice-provenance-enabled");
            createEnabled.setJsonEntity("""
                {
                  "settings": {
                    "index.slice.enabled": true
                  }
                }""");
            restClient.performRequest(createEnabled);

            Request missingSlice = new Request("POST", "/slice-provenance-enabled/_doc/1");
            missingSlice.setJsonEntity("""
                {
                  "field": "value"
                }""");
            ResponseException missingSliceException = expectThrows(ResponseException.class, () -> restClient.performRequest(missingSlice));
            String missingSliceBody = Streams.copyToString(
                new InputStreamReader(missingSliceException.getResponse().getEntity().getContent(), UTF_8)
            );
            assertThat(missingSliceBody, containsString("[_slice] is required when [index.slice.enabled] is true"));

            Request validSlice = new Request("POST", "/slice-provenance-enabled/_doc/2");
            validSlice.addParameter("_slice", "s1");
            validSlice.setJsonEntity("""
                {
                  "field": "value"
                }""");
            Response createdResponse = restClient.performRequest(validSlice);
            ObjectPath createdPath = ObjectPath.createFromResponse(createdResponse);
            assertThat(createdPath.evaluate("result"), equalTo("created"));

            Request createDisabled = new Request("PUT", "/slice-provenance-disabled");
            createDisabled.setJsonEntity("""
                {
                  "settings": {
                    "index.slice.enabled": false
                  }
                }""");
            restClient.performRequest(createDisabled);

            Request disabledSlice = new Request("POST", "/slice-provenance-disabled/_doc/1");
            disabledSlice.addParameter("_slice", "s1");
            disabledSlice.setJsonEntity("""
                {
                  "field": "value"
                }""");
            ResponseException disabledSliceException = expectThrows(
                ResponseException.class,
                () -> restClient.performRequest(disabledSlice)
            );
            String disabledSliceBody = Streams.copyToString(
                new InputStreamReader(disabledSliceException.getResponse().getEntity().getContent(), UTF_8)
            );
            assertThat(disabledSliceBody, containsString("[_slice] is not allowed when [index.slice.enabled] is false"));
        } finally {
            restClient.setNodes(originalNodes);
        }
    }
}
