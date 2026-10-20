/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.rest.BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RestPutMappingActionTests extends RestActionTestCase {

    private static final String TYPELESS = """
        {"properties":{"f":{"type":"keyword"}}}""";
    private static final String TYPED = "{\"_doc\":" + TYPELESS + "}";

    private final RestPutMappingAction action = new RestPutMappingAction();

    private FakeRestRequest.Builder requestBuilder(String body, Map<String, String> params) {
        Map<String, String> allParams = new HashMap<>(params);
        allParams.putIfAbsent("index", "index");
        return new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath("/index/_mapping")
            .withParams(allParams)
            .withContent(new BytesArray(body), XContentType.JSON);
    }

    private FakeRestRequest.Builder v7RequestBuilder(String body, Map<String, String> params) {
        String mediaType = compatibleMediaType(XContentType.VND_JSON, RestApiVersion.V_7);
        return requestBuilder(body, params).withHeaders(Map.of("Content-Type", List.of(mediaType), "Accept", List.of(mediaType)));
    }

    /** Runs the request through the handler and returns the transport request that it executes. */
    private PutMappingRequest prepare(RestRequest request) throws Exception {
        AtomicReference<PutMappingRequest> captured = new AtomicReference<>();
        verifyingClient.setExecuteVerifier((actionType, actionRequest) -> {
            assertThat(actionType.name(), equalTo(TransportPutMappingAction.TYPE.name()));
            captured.set((PutMappingRequest) actionRequest);
            return AcknowledgedResponse.TRUE;
        });
        action.handleRequest(request, new FakeRestChannel(request, true), verifyingClient);
        PutMappingRequest result = captured.get();
        assertNotNull("no put mapping request was executed", result);
        return result;
    }

    private void assertRejected(RestRequest request, String message) {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, verifyingClient));
        assertThat(e.getMessage(), containsString(message));
    }

    public void testTypelessMappings() throws Exception {
        PutMappingRequest result = prepare(requestBuilder(TYPELESS, Map.of()).build());
        assertThat(result.source(), equalTo(TYPELESS));
        assertThat(result.indices(), arrayContaining("index"));
    }

    public void testRequestParameters() throws Exception {
        Map<String, String> params = Map.of(
            "index",
            "a,b",
            "write_index_only",
            "true",
            "timeout",
            "7s",
            "master_timeout",
            "8s",
            "expand_wildcards",
            "none"
        );
        PutMappingRequest result = prepare(requestBuilder(TYPELESS, params).build());
        assertThat(result.indices(), arrayContaining("a", "b"));
        assertThat(result.writeIndexOnly(), is(true));
        assertThat(result.ackTimeout(), equalTo(TimeValue.timeValueSeconds(7)));
        assertThat(result.masterNodeTimeout(), equalTo(TimeValue.timeValueSeconds(8)));
        assertThat(result.indicesOptions().expandWildcardsOpen(), is(false));

        PutMappingRequest defaults = prepare(requestBuilder(TYPELESS, Map.of()).build());
        assertThat(defaults.writeIndexOnly(), is(false));
        assertThat(defaults.indicesOptions(), equalTo(new PutMappingRequest().indicesOptions()));
    }

    public void testEmptyMappings() throws Exception {
        assertThat(prepare(requestBuilder("{}", Map.of()).build()).source(), equalTo("{}"));
    }

    public void testSeveralTopLevelFieldsIsNotTyped() throws Exception {
        // _doc is only a type name when it is the only top-level field
        String body = "{\"_doc\":{},\"properties\":{\"f\":{\"type\":\"keyword\"}}}";
        assertThat(prepare(requestBuilder(body, Map.of()).build()).source(), equalTo(body));
    }

    public void testSingleNonTypeTopLevelFieldIsNotTyped() throws Exception {
        assertThat(prepare(requestBuilder(TYPELESS, Map.of()).build()).source(), equalTo(TYPELESS));
        String meta = "{\"_meta\":{\"a\":[1,2,{\"b\":null}]}}";
        assertThat(prepare(requestBuilder(meta, Map.of()).build()).source(), equalTo(meta));
    }

    public void testTypedMappingsAreRejected() {
        assertRejected(requestBuilder(TYPED, Map.of()).build(), "Types cannot be provided in put mapping requests");
    }

    public void testNonObjectBodyIsRejected() {
        assertRejected(requestBuilder("[]", Map.of()).build(), "mapping source must be an object");
        assertRejected(requestBuilder("\"foo\"", Map.of()).build(), "mapping source must be an object");
    }

    public void testMissingBodyIsRejected() {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath("/index/_mapping")
            .withParams(Map.of("index", "index"))
            .build();
        expectThrows(ElasticsearchParseException.class, () -> action.prepareRequest(request, verifyingClient));
    }

    public void testNonJsonContentIsConvertedToJson() throws Exception {
        String yaml = "properties:\n  f:\n    type: keyword\n";
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath("/index/_mapping")
            .withParams(Map.of("index", "index"))
            .withContent(new BytesArray(yaml), XContentType.YAML)
            .build();
        assertThat(prepare(request).source(), equalTo(TYPELESS));
    }

    public void testV7TypelessMappings() throws Exception {
        assertThat(prepare(v7RequestBuilder(TYPELESS, Map.of()).build()).source(), equalTo(TYPELESS));
    }

    public void testV7IncludeTypeNameFalseWithTypedMappingsIsRejected() {
        String message = "Types cannot be provided in put mapping requests, unless the include_type_name parameter is set to true.";
        assertRejected(v7RequestBuilder(TYPED, Map.of()).build(), message);
        assertRejected(v7RequestBuilder(TYPED, Map.of(INCLUDE_TYPE_NAME_PARAMETER, "false")).build(), message);
        assertCriticalWarnings(RestPutMappingAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testV7TypeInPathWithoutIncludeTypeNameIsRejected() {
        String message = "Types cannot be provided in put mapping requests, unless the include_type_name parameter is set to true.";
        assertRejected(v7RequestBuilder(TYPELESS, Map.of("type", "my_type")).build(), message);
    }

    public void testV7IncludeTypeNameFalseWithTypelessMappings() throws Exception {
        PutMappingRequest result = prepare(v7RequestBuilder(TYPELESS, Map.of(INCLUDE_TYPE_NAME_PARAMETER, "false")).build());
        assertThat(result.source(), equalTo(TYPELESS));
        assertCriticalWarnings(RestPutMappingAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testV7IncludeTypeNameUnwrapsTypeName() throws Exception {
        for (String type : List.of("_doc", "my_type")) {
            String body = "{\"" + type + "\":" + TYPELESS + "}";
            PutMappingRequest result = prepare(v7RequestBuilder(body, Map.of(INCLUDE_TYPE_NAME_PARAMETER, "true")).build());
            assertThat(result.source(), equalTo(TYPELESS));
            assertCriticalWarnings(RestPutMappingAction.TYPES_DEPRECATION_MESSAGE);
        }
    }

    public void testV7IncludeTypeNameWithTypeInPath() throws Exception {
        String body = "{\"my_type\":" + TYPELESS + "}";
        Map<String, String> params = Map.of(INCLUDE_TYPE_NAME_PARAMETER, "true", "type", "my_type");
        assertThat(prepare(v7RequestBuilder(body, params).build()).source(), equalTo(TYPELESS));
        assertCriticalWarnings(RestPutMappingAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testV7IncludeTypeNameWithSeveralTopLevelFieldsIsNotUnwrapped() throws Exception {
        String body = "{\"properties\":{\"f\":{\"type\":\"keyword\"}},\"_meta\":{}}";
        PutMappingRequest result = prepare(v7RequestBuilder(body, Map.of(INCLUDE_TYPE_NAME_PARAMETER, "true")).build());
        assertThat(result.source(), equalTo(body));
        assertCriticalWarnings(RestPutMappingAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testV7IncludeTypeNameWithEmptyMappings() throws Exception {
        PutMappingRequest result = prepare(v7RequestBuilder("{}", Map.of(INCLUDE_TYPE_NAME_PARAMETER, "true")).build());
        assertThat(result.source(), equalTo("{}"));
        assertCriticalWarnings(RestPutMappingAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testV7IncludeTypeNameWithEmptyTypeNameIsRejected() {
        assertRejected(
            v7RequestBuilder("{\" \":" + TYPELESS + "}", Map.of(INCLUDE_TYPE_NAME_PARAMETER, "true")).build(),
            "name cannot be empty string"
        );
        assertCriticalWarnings(RestPutMappingAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testV7IncludeTypeNameWithNonObjectTypeValueIsRejected() {
        assertRejected(
            v7RequestBuilder("{\"my_type\":\"foo\"}", Map.of(INCLUDE_TYPE_NAME_PARAMETER, "true")).build(),
            "mappings for type [my_type] must be an object"
        );
        assertCriticalWarnings(RestPutMappingAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testV7NonObjectBodyIsRejected() throws IOException {
        assertRejected(v7RequestBuilder("[]", Map.of()).build(), "mapping source must be an object");
    }
}
