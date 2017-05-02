/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.test.rest.yaml;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Execution context passed across the REST tests.
 * Holds the REST client used to communicate with elasticsearch.
 * Caches the last obtained test response and allows to stash part of it within variables
 * that can be used as input values in following requests.
 */
public class ClientYamlTestExecutionContext {

    private static final Logger logger = Loggers.getLogger(ClientYamlTestExecutionContext.class);

    private static final XContentType[] STREAMING_CONTENT_TYPES = new XContentType[]{XContentType.JSON, XContentType.SMILE};

    private final Stash stash = new Stash();
    private final ClientYamlTestClient clientYamlTestClient;

    private ClientYamlTestResponse response;

    private final boolean randomizeContentType;

    ClientYamlTestExecutionContext(ClientYamlTestClient clientYamlTestClient, boolean randomizeContentType) {
        this.clientYamlTestClient = clientYamlTestClient;
        this.randomizeContentType = randomizeContentType;
    }

    /**
     * Calls an elasticsearch api with the parameters and request body provided as arguments.
     * Saves the obtained response in the execution context.
     */
    public ClientYamlTestResponse callApi(String apiName, Map<String, String> params, List<Map<String, Object>> bodies,
                                    Map<String, String> headers) throws IOException {
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

        HttpEntity entity = createEntity(bodies, requestHeaders);
        try {
            response = callApiInternal(apiName, requestParams, entity, requestHeaders);
            return response;
        } catch(ClientYamlTestResponseException e) {
            response = e.getRestTestResponse();
            throw e;
        } finally {
            // if we hit a bad exception the response is null
            Object responseBody = response != null ? response.getBody() : null;
            //we always stash the last response body
            stash.stashValue("body", responseBody);
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
            return builder.map(finalBodyAsMap).bytes().toBytesRef();
        }
    }

    // pkg-private for testing
    ClientYamlTestResponse callApiInternal(String apiName, Map<String, String> params,
                                                   HttpEntity entity, Map<String, String> headers) throws IOException  {
        return clientYamlTestClient.callApi(apiName, params, entity, headers);
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

}
