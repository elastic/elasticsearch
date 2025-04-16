/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.test.rest.yaml;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.test.rest.Stash;
import org.elasticsearch.test.rest.TestFeatureService;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestApi;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

/**
 * Execution context passed across the REST tests.
 * Holds the REST client used to communicate with elasticsearch.
 * Caches the last obtained test response and allows to stash part of it within variables
 * that can be used as input values in following requests.
 */
public class ClientYamlTestExecutionContext {

    private static final Logger logger = LogManager.getLogger(ClientYamlTestExecutionContext.class);

    private static final XContentType[] STREAMING_CONTENT_TYPES = new XContentType[] { XContentType.JSON, XContentType.SMILE };

    private final Stash stash = new Stash();
    private final ClientYamlTestClient clientYamlTestClient;
    private final ClientYamlTestCandidate clientYamlTestCandidate;

    private ClientYamlTestResponse response;

    private final Set<String> nodesVersions;

    private final Set<String> osSet;
    private final TestFeatureService testFeatureService;

    private final boolean randomizeContentType;
    private final BiPredicate<ClientYamlSuiteRestApi, ClientYamlSuiteRestApi.Path> pathPredicate;

    public ClientYamlTestExecutionContext(
        ClientYamlTestCandidate clientYamlTestCandidate,
        ClientYamlTestClient clientYamlTestClient,
        boolean randomizeContentType,
        final Set<String> nodesVersions,
        final TestFeatureService testFeatureService,
        final Set<String> osSet
    ) {
        this(
            clientYamlTestCandidate,
            clientYamlTestClient,
            randomizeContentType,
            nodesVersions,
            testFeatureService,
            osSet,
            (ignoreApi, ignorePath) -> true
        );
    }

    public ClientYamlTestExecutionContext(
        ClientYamlTestCandidate clientYamlTestCandidate,
        ClientYamlTestClient clientYamlTestClient,
        boolean randomizeContentType,
        final Set<String> nodesVersions,
        final TestFeatureService testFeatureService,
        final Set<String> osSet,
        BiPredicate<ClientYamlSuiteRestApi, ClientYamlSuiteRestApi.Path> pathPredicate
    ) {
        this.clientYamlTestClient = clientYamlTestClient;
        this.clientYamlTestCandidate = clientYamlTestCandidate;
        this.randomizeContentType = randomizeContentType;
        this.nodesVersions = nodesVersions;
        this.testFeatureService = testFeatureService;
        this.osSet = osSet;
        this.pathPredicate = pathPredicate;
    }

    /**
     * Calls an elasticsearch api with the parameters and request body provided as arguments.
     * Saves the obtained response in the execution context.
     */
    public ClientYamlTestResponse callApi(
        String apiName,
        Map<String, String> params,
        List<Map<String, Object>> bodies,
        Map<String, String> headers
    ) throws IOException {
        return callApi(apiName, params, bodies, headers, NodeSelector.ANY);
    }

    /**
     * Calls an elasticsearch api with the parameters and request body provided as arguments.
     * Saves the obtained response in the execution context.
     */
    public ClientYamlTestResponse callApi(
        String apiName,
        Map<String, String> params,
        List<Map<String, Object>> bodies,
        Map<String, String> headers,
        NodeSelector nodeSelector
    ) throws IOException {
        // makes a copy of the parameters before modifying them for this specific request
        Map<String, String> requestParams = new HashMap<>(params);
        requestParams.compute("error_trace", (k, v) -> {
            if (v == null) {
                return "true";  // By default ask for error traces, this my be overridden by params
            } else if (v.equals("false")) {
                return null;
            } else {
                return v;
            }
        });
        for (Map.Entry<String, String> entry : requestParams.entrySet()) {
            if (stash.containsStashedValue(entry.getValue())) {
                entry.setValue(stash.getValue(entry.getValue()).toString());
            }
        }

        // make a copy of the headers before modifying them for this specific request
        Map<String, String> requestHeaders = new HashMap<>(headers);
        for (Map.Entry<String, String> entry : requestHeaders.entrySet()) {
            if (stash.containsStashedValue(entry.getValue())) {
                entry.setValue(stash.getValue(entry.getValue()).toString());
            }
        }

        HttpEntity entity = createEntity(bodies, requestHeaders);
        try {
            response = callApiInternal(apiName, requestParams, entity, requestHeaders, nodeSelector);
            return response;
        } catch (ClientYamlTestResponseException e) {
            response = e.getRestTestResponse();
            throw e;
        } finally {
            // if we hit a bad exception the response is null
            Object responseBody = response != null ? response.getBody() : null;
            // we always stash the last response body
            stash.stashValue("body", responseBody);
            if (requestHeaders.isEmpty() == false) {
                stash.stashValue("request_headers", requestHeaders);
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
            return new ByteArrayEntity(
                bytesRef.bytes,
                bytesRef.offset,
                bytesRef.length,
                ContentType.create(xContentType.mediaTypeWithoutParameters(), StandardCharsets.UTF_8)
            );
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
                bytes[position++] = xContentType.xContent().bulkSeparator();
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
    ClientYamlTestResponse callApiInternal(
        String apiName,
        Map<String, String> params,
        HttpEntity entity,
        Map<String, String> headers,
        NodeSelector nodeSelector
    ) throws IOException {
        return clientYamlTestClient(apiName).callApi(apiName, params, entity, headers, nodeSelector, pathPredicate);
    }

    protected ClientYamlTestClient clientYamlTestClient(String apiName) {
        return clientYamlTestClient;
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
     * @return the distinct node versions running in the cluster
     */
    public Set<String> nodesVersions() {
        return nodesVersions;
    }

    public String os() {
        return osSet.iterator().next();
    }

    public ClientYamlTestCandidate getClientYamlTestCandidate() {
        return clientYamlTestCandidate;
    }

    public boolean clusterHasFeature(String featureId, boolean any) {
        return testFeatureService.clusterHasFeature(featureId, any);
    }

    public Optional<Boolean> clusterHasCapabilities(
        String method,
        String path,
        String parametersString,
        String capabilitiesString,
        boolean any
    ) {
        Map<String, String> params = Maps.newMapWithExpectedSize(6);
        params.put("method", method);
        params.put("path", path);
        if (Strings.hasLength(parametersString)) {
            params.put("parameters", parametersString);
        }
        if (Strings.hasLength(capabilitiesString)) {
            params.put("capabilities", capabilitiesString);
        }
        params.put("error_trace", "false"); // disable error trace
        params.put("local_only", "true");   // we're calling each node individually

        // individually call each node, so we can control whether we do an 'any' or 'all' check
        List<Node> nodes = clientYamlTestClient.getRestClient(NodeSelector.ANY).getNodes();

        for (Node n : nodes) {
            Optional<Boolean> nodeResult = checkCapability(new SpecificNodeSelector(n), params);
            if (nodeResult.isEmpty()) {
                return Optional.empty();
            } else if (any == nodeResult.get()) {
                // either any == true and node has cap,
                // or any == false (ie all) and this node does not have cap
                return nodeResult;
            }
        }

        // if we got here, either any is true and no node has it, or any == false and all nodes have it
        return Optional.of(any == false);
    }

    private Optional<Boolean> checkCapability(NodeSelector nodeSelector, Map<String, String> params) {
        try {
            ClientYamlTestResponse resp = callApi("capabilities", params, emptyList(), emptyMap(), nodeSelector);
            // anything other than 200 should result in an exception, handled below
            assert resp.getStatusCode() == 200 : "Unknown response code " + resp.getStatusCode();
            return Optional.ofNullable(resp.evaluate("supported"));
        } catch (ClientYamlTestResponseException responseException) {
            if (responseException.getRestTestResponse().getStatusCode() / 100 == 4) {
                return Optional.empty(); // we don't know, the capabilities API is unsupported
            }
            throw new UncheckedIOException(responseException);
        } catch (IOException ioException) {
            throw new UncheckedIOException(ioException);
        }
    }

    private record SpecificNodeSelector(Node node) implements NodeSelector {
        @Override
        public void select(Iterable<Node> nodes) {
            // between getting the list of nodes, and checking here, the thing that is consistent is the host
            // which becomes one of the bound addresses
            for (var it = nodes.iterator(); it.hasNext();) {
                if (it.next().getBoundHosts().contains(node.getHost()) == false) {
                    it.remove();
                }
            }
        }
    }
}
