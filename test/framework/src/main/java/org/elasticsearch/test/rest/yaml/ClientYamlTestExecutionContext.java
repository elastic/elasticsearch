/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.rest.yaml;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER;

/**
 * Execution context passed across the REST tests.
 * Holds the REST client used to communicate with elasticsearch.
 * Caches the last obtained test response and allows to stash part of it within variables
 * that can be used as input values in following requests.
 */
public class ClientYamlTestExecutionContext {

    private static final Logger logger = LogManager.getLogger(ClientYamlTestExecutionContext.class);

    private static final XContentType[] STREAMING_CONTENT_TYPES = new XContentType[]{XContentType.JSON, XContentType.SMILE};

    private final Stash stash = new Stash();
    private final ClientYamlTestClient clientYamlTestClient;
    private final ClientYamlTestCandidate clientYamlTestCandidate;

    private ClientYamlTestResponse response;

    private final boolean randomizeContentType;

    ClientYamlTestExecutionContext(
        ClientYamlTestCandidate clientYamlTestCandidate,
        ClientYamlTestClient clientYamlTestClient,
        boolean randomizeContentType) {
        this.clientYamlTestClient = clientYamlTestClient;
        this.clientYamlTestCandidate = clientYamlTestCandidate;
        this.randomizeContentType = randomizeContentType;
    }

    /**
     * Calls an elasticsearch api with the parameters and request body provided as arguments.
     * Saves the obtained response in the execution context.
     */
    public ClientYamlTestResponse callApi(String apiName, Map<String, String> params, List<Map<String, Object>> bodies,
                                    Map<String, String> headers) throws IOException {
        return callApi(apiName, params, bodies, headers, NodeSelector.ANY);
    }

    /**
     * Calls an elasticsearch api with the parameters and request body provided as arguments.
     * Saves the obtained response in the execution context.
     */
    public ClientYamlTestResponse callApi(String apiName, Map<String, String> params, List<Map<String, Object>> bodies,
                                    Map<String, String> headers, NodeSelector nodeSelector) throws IOException {
        //makes a copy of the parameters before modifying them for this specific request
        Map<String, String> requestParams = new HashMap<>(params);
        requestParams.putIfAbsent("error_trace", "true"); // By default ask for error traces, this my be overridden by params
        for (Map.Entry<String, String> entry : requestParams.entrySet()) {
            if (stash.containsStashedValue(entry.getValue())) {
                entry.setValue(stash.getValue(entry.getValue()).toString());
            }
        }

        //make a copy of the headers before modifying them for this specific request
        Map<String, String> requestHeaders = new HashMap<>(headers);
        for (Map.Entry<String, String> entry : requestHeaders.entrySet()) {
            if (stash.containsStashedValue(entry.getValue())) {
                entry.setValue(stash.getValue(entry.getValue()).toString());
            }
        }

        if (esVersion().before(Version.V_7_0_0)) {
            adaptRequestForOlderVersion(apiName, bodies, requestParams);
        }

        HttpEntity entity = createEntity(bodies, requestHeaders);
        try {
            response = callApiInternal(apiName, requestParams, entity, requestHeaders, nodeSelector);
            return response;
        } catch(ClientYamlTestResponseException e) {
            response = e.getRestTestResponse();
            throw e;
        } finally {
            // if we hit a bad exception the response is null
            Object responseBody = response != null ? response.getBody() : null;
            //we always stash the last response body
            stash.stashValue("body", responseBody);
            if(requestHeaders.isEmpty() == false) {
                stash.stashValue("request_headers", requestHeaders);
            }
        }
    }

    /**
     * To allow tests to run against a mixed 7.x/6.x cluster, we make certain modifications to the
     * request related to types.
     *
     * Specifically, we generally use typeless index creation and document writes in test set-up code.
     * This functionality is supported in 7.x, but is not supported in 6.x (or is not the default
     * behavior). Here we modify the request so that it will work against a 6.x node.
     */
    @SuppressWarnings("unchecked")
    private void adaptRequestForOlderVersion(String apiName,
                                             List<Map<String, Object>> bodies,
                                             Map<String, String> requestParams) {
        // For index creations, we specify 'include_type_name=false' if it is not explicitly set. This
        // allows us to omit the parameter in the test description, while still being able to communicate
        // with 6.x nodes where include_type_name defaults to 'true'.
        if (apiName.equals("indices.create") && requestParams.containsKey(INCLUDE_TYPE_NAME_PARAMETER) == false) {
            requestParams.put(INCLUDE_TYPE_NAME_PARAMETER, "false");
        }

        // We add the type to the document API requests if it's not already included.
        if ((apiName.equals("index") || apiName.equals("update") || apiName.equals("delete") || apiName.equals("get"))
                && requestParams.containsKey("type") == false) {
            requestParams.put("type", "_doc");
        }

        // We also add the type to the bulk API requests if it's not already included. The type can either
        // be on the request parameters or in the action metadata in the body of the request so we need to
        // be sensitive to both scenarios.
        if (apiName.equals("bulk") && requestParams.containsKey("type") == false) {
            if (requestParams.containsKey("index")) {
                requestParams.put("type", "_doc");
            } else {
                for (int i = 0; i < bodies.size(); i++) {
                    Map<String, Object> body = bodies.get(i);
                    Map<String, Object> actionMetadata;
                    if (body.containsKey("index")) {
                        actionMetadata = (Map<String, Object>) body.get("index");
                        i++;
                    } else if (body.containsKey("create")) {
                        actionMetadata = (Map<String, Object>) body.get("create");
                        i++;
                    } else if (body.containsKey("update")) {
                        actionMetadata = (Map<String, Object>) body.get("update");
                        i++;
                    } else if (body.containsKey("delete")) {
                        actionMetadata = (Map<String, Object>) body.get("delete");
                    } else {
                        // action metadata is malformed so leave it malformed since
                        // the test is probably testing for malformed action metadata
                        continue;
                    }
                    if (actionMetadata.containsKey("_type") == false) {
                        actionMetadata.put("_type", "_doc");
                    }
                }
            }
        }
    }

    private HttpEntity createEntity(List<Map<String, Object>> bodies, Map<String, String> headers) throws IOException {
        if (bodies.isEmpty()) {
            return null;
        }
        if (bodies.size() == 1) {
            XContentType xContentType = getContentType(headers, XContentType.values());
            BytesRef bytesRef = bodyAsBytesRef(bodies.get(0), xContentType);
            return new ByteArrayEntity(bytesRef.bytes, bytesRef.offset, bytesRef.length,
                    ContentType.create(xContentType.mediaTypeWithoutParameters(), StandardCharsets.UTF_8));
        } else {
            XContentType xContentType = getContentType(headers, STREAMING_CONTENT_TYPES);
            List<BytesRef> bytesRefList = new ArrayList<>(bodies.size());
            int totalBytesLength = 0;
            for (Map<String, Object> body : bodies) {
                BytesRef bytesRef = bodyAsBytesRef(body, xContentType);
                bytesRefList.add(bytesRef);
                totalBytesLength += bytesRef.length - bytesRef.offset + 1;
            }
            byte[] bytes = new byte[totalBytesLength];
            int position = 0;
            for (BytesRef bytesRef : bytesRefList) {
                for (int i = bytesRef.offset; i < bytesRef.length; i++) {
                    bytes[position++] = bytesRef.bytes[i];
                }
                bytes[position++] = xContentType.xContent().streamSeparator();
            }
            return new ByteArrayEntity(bytes, ContentType.create(xContentType.mediaTypeWithoutParameters(), StandardCharsets.UTF_8));
        }
    }

    private XContentType getContentType(Map<String, String> headers, XContentType[] supportedContentTypes) {
        XContentType xContentType = null;
        String contentType = headers.get("Content-Type");
        if (contentType != null) {
            xContentType = XContentType.fromMediaType(contentType);
        }
        if (xContentType != null) {
            return xContentType;
        }
        if (randomizeContentType) {
            return RandomizedTest.randomFrom(supportedContentTypes);
        }
        return XContentType.JSON;
    }

    private BytesRef bodyAsBytesRef(Map<String, Object> bodyAsMap, XContentType xContentType) throws IOException {
        Map<String, Object> finalBodyAsMap = stash.replaceStashedValues(bodyAsMap);
        try (XContentBuilder builder = XContentFactory.contentBuilder(xContentType)) {
            return BytesReference.bytes(builder.map(finalBodyAsMap)).toBytesRef();
        }
    }

    // pkg-private for testing
    ClientYamlTestResponse callApiInternal(String apiName, Map<String, String> params, HttpEntity entity,
            Map<String, String> headers, NodeSelector nodeSelector) throws IOException  {
        return clientYamlTestClient.callApi(apiName, params, entity, headers, nodeSelector);
    }

    /**
     * Extracts a specific value from the last saved response
     */
    public Object response(String path) throws IOException {
        return response.evaluate(path, stash);
    }

    /**
     * Clears the last obtained response and the stashed fields
     */
    public void clear() {
        logger.debug("resetting client, response and stash");
        response = null;
        stash.clear();
    }

    public Stash stash() {
        return stash;
    }

    /**
     * Returns the current es version as a string
     */
    public Version esVersion() {
        return clientYamlTestClient.getEsVersion();
    }

    public Version masterVersion() {
        return clientYamlTestClient.getMasterVersion();
    }

    public String os() {
        return clientYamlTestClient.getOs();
    }

    public ClientYamlTestCandidate getClientYamlTestCandidate() {
        return clientYamlTestCandidate;
    }
}
