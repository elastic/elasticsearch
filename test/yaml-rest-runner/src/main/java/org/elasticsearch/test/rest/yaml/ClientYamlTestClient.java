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
import org.apache.http.HttpHost;
import org.apache.http.NameValuePair;
import org.apache.http.ParseException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestApi;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestSpec;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

import static com.carrotsearch.randomizedtesting.RandomizedTest.frequently;
import static java.util.Collections.emptyMap;

/**
 * Used by {@link ESClientYamlSuiteTestCase} to execute REST requests according to the tests written in yaml suite files. Wraps a
 * {@link RestClient} instance used to send the REST requests. Holds the {@link ClientYamlSuiteRestSpec} used to translate api calls into
 * REST calls.
 */
public class ClientYamlTestClient implements Closeable {
    private static final Logger logger = LogManager.getLogger(ClientYamlTestClient.class);

    private static final ContentType YAML_CONTENT_TYPE = ContentType.create("application/yaml");

    private final ClientYamlSuiteRestSpec restSpec;
    private final Map<NodeSelector, RestClient> restClients = new HashMap<>();
    private final CheckedSupplier<RestClientBuilder, IOException> clientBuilderWithSniffedNodes;

    ClientYamlTestClient(
        final ClientYamlSuiteRestSpec restSpec,
        final RestClient restClient,
        final List<HttpHost> hosts,
        final CheckedSupplier<RestClientBuilder, IOException> clientBuilderWithSniffedNodes
    ) {
        assert hosts.size() > 0;
        this.restSpec = restSpec;
        this.restClients.put(NodeSelector.ANY, restClient);
        this.clientBuilderWithSniffedNodes = clientBuilderWithSniffedNodes;
    }

    /**
     * Calls an api with the provided parameters and body
     */
    public ClientYamlTestResponse callApi(
        String apiName,
        String method,
        Map<String, String> params,
        HttpEntity entity,
        Map<String, String> headers,
        NodeSelector nodeSelector,
        BiPredicate<ClientYamlSuiteRestApi, ClientYamlSuiteRestApi.Path> pathPredicate
    ) throws IOException {

        ClientYamlSuiteRestApi restApi = restApi(apiName);

        Set<String> apiRequiredParameters = restApi.getParams()
            .entrySet()
            .stream()
            .filter(Entry::getValue)
            .map(Entry::getKey)
            .collect(Collectors.toSet());

        List<ClientYamlSuiteRestApi.Path> bestPaths = restApi.getBestMatchingPaths(params.keySet());
        List<ClientYamlSuiteRestApi.Path> filteredPaths = bestPaths.stream()
            .filter(path -> pathPredicate.test(restApi, path))
            .collect(Collectors.toUnmodifiableList());
        if (filteredPaths.isEmpty()) {
            throw new IllegalStateException(
                Strings.format(
                    "All possible paths [%s] for API [%s] have been skipped",
                    Strings.collectionToCommaDelimitedString(bestPaths),
                    apiName
                )
            );
        }
        // the rest path to use is randomized out of the matching ones (if more than one)
        ClientYamlSuiteRestApi.Path path = RandomizedTest.randomFrom(filteredPaths);

        // divide params between ones that go within query string and ones that go within path
        Map<String, String> pathParts = new HashMap<>();
        Map<String, String> queryStringParams = new HashMap<>();

        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (path.parts().contains(entry.getKey())) {
                pathParts.put(entry.getKey(), entry.getValue());
            } else if (restApi.getParams().containsKey(entry.getKey())
                || restSpec.isGlobalParameter(entry.getKey())
                || restSpec.isClientParameter(entry.getKey())) {
                    queryStringParams.put(entry.getKey(), entry.getValue());
                    apiRequiredParameters.remove(entry.getKey());
                } else {
                    throw new IllegalArgumentException(
                        "path/param [" + entry.getKey() + "] not supported by [" + restApi.getName() + "] " + "api"
                    );
                }
        }

        if (false == apiRequiredParameters.isEmpty()) {
            throw new IllegalArgumentException(
                "missing required parameter: " + apiRequiredParameters + " by [" + restApi.getName() + "] api"
            );
        }

        Set<String> partNames = pathParts.keySet();
        if (path.parts().size() != partNames.size() || path.parts().containsAll(partNames) == false) {
            throw new IllegalStateException("provided path parts don't match the best matching path: " + path.parts() + " - " + partNames);
        }

        String finalPath = path.path();
        for (Entry<String, String> pathPart : pathParts.entrySet()) {
            try {
                // Encode rules for path and query string parameters are different. We use URI to encode the path. We need to encode each
                // path part separately, as each one might contain slashes that need to be escaped, which needs to be done manually.
                // We prepend "/" to the path part to handle parts that start with - or other invalid characters.
                URI uri = new URI(null, null, null, -1, "/" + pathPart.getValue(), null, null);
                // manually escape any slash that each part may contain
                String encodedPathPart = uri.getRawPath().substring(1).replace("/", "%2F");
                finalPath = finalPath.replace("{" + pathPart.getKey() + "}", encodedPathPart);
            } catch (URISyntaxException e) {
                throw new RuntimeException("unable to build uri", e);
            }
        }

        List<String> supportedMethods = Arrays.asList(path.methods());
        String requestMethod;
        if (method != null) {
            // Method override specified - validate it's supported
            if (supportedMethods.contains(method) == false) {
                throw new IllegalArgumentException(
                    "method [" + method + "] is not supported by path [" + path.path() + "]. Supported methods: " + supportedMethods
                );
            }
            requestMethod = method;
        } else if (entity != null) {
            if (false == restApi.isBodySupported()) {
                throw new IllegalArgumentException("body is not supported by [" + restApi.getName() + "] api");
            }
            String contentType = entity.getContentType().getValue();
            // randomly test the GET with source param instead of GET/POST with body
            if (sendBodyAsSourceParam(supportedMethods, contentType, entity)) {
                logger.debug("sending the request body as source param with GET method");
                queryStringParams.put("source", EntityUtils.toString(entity));
                queryStringParams.put("source_content_type", contentType);
                requestMethod = HttpGet.METHOD_NAME;
                entity = null;
            } else {
                requestMethod = RandomizedTest.randomFrom(supportedMethods);
            }
        } else {
            if (restApi.isBodyRequired()) {
                throw new IllegalArgumentException("body is required by [" + restApi.getName() + "] api");
            }
            requestMethod = RandomizedTest.randomFrom(supportedMethods);
        }

        logger.debug("calling api [{}]", apiName);
        Request request = new Request(requestMethod, finalPath);
        for (Map.Entry<String, String> param : queryStringParams.entrySet()) {
            request.addParameter(param.getKey(), param.getValue());
        }
        request.setEntity(entity);
        setOptions(request, headers);

        try {
            Response response = getRestClient(nodeSelector).performRequest(request);
            ClientYamlTestResponse yamlResponse = new ClientYamlTestResponse(response);
            // Temporary: log index.version.created after each successful indices.create (for mixed-cluster / BWC debugging).
            if ("indices.create".equals(apiName) && pathParts.containsKey("index")) {
                String createdIndex = pathParts.get("index");
                // Index named used by tsdb/25_id_generation
                if ("id_generation_test".equals(createdIndex)) {
                    logIndexVersionCreatedAfterCreate(nodeSelector, finalPath, createdIndex);
                    logClusterNodesAfterIndicesCreate(nodeSelector, createdIndex);
                }
            }
            return yamlResponse;
        } catch (ResponseException e) {
            throw new ClientYamlTestResponseException(e);
        }
    }

    /**
     * Temporary helper for mixed-cluster debugging; remove when no longer needed.
     */
    private void logIndexVersionCreatedAfterCreate(NodeSelector nodeSelector, String encodedIndexPathPrefix, String indexName) {
        try {
            Request settingsRequest = new Request("GET", encodedIndexPathPrefix + "/_settings");
            settingsRequest.addParameter("flat_settings", "true");
            setOptions(settingsRequest, emptyMap());
            Response settingsResponse = getRestClient(nodeSelector).performRequest(settingsRequest);
            ClientYamlTestResponse parsed = new ClientYamlTestResponse(settingsResponse);
            Object body = parsed.getBody();
            String versionCreated = extractIndexVersionCreated(body, indexName);
            logger.info(
                "after indices.create index [{}] index.version.created [{}] settings {}",
                indexName,
                versionCreated,
                parsed.getBodyAsString()
            );
        } catch (RuntimeException | IOException e) {
            logger.warn("failed to log index settings after indices.create for index [{}]", indexName, e);
        }
    }

    private static String extractIndexVersionCreated(Object body, String indexName) {
        if (body instanceof Map<?, ?> == false) {
            return null;
        }
        Map<?, ?> root = (Map<?, ?>) body;
        Object idx = root.get(indexName);
        if (idx instanceof Map<?, ?> == false) {
            return null;
        }
        Map<?, ?> idxMap = (Map<?, ?>) idx;
        Object settings = idxMap.get("settings");
        if (settings instanceof Map<?, ?> == false) {
            return null;
        }
        Map<?, ?> setMap = (Map<?, ?>) settings;
        Object flat = setMap.get("index.version.created");
        if (flat != null) {
            return String.valueOf(flat);
        }
        Object index = setMap.get("index");
        if (index instanceof Map<?, ?> im) {
            Object version = im.get("version");
            if (version instanceof Map<?, ?> vm) {
                Object created = vm.get("created");
                if (created != null) {
                    return String.valueOf(created);
                }
            }
        }
        return null;
    }

    /**
     * Temporary helper for mixed-cluster debugging; remove when no longer needed.
     */
    private void logClusterNodesAfterIndicesCreate(NodeSelector nodeSelector, String indexName) {
        try {
            RestClient client = getRestClient(nodeSelector);

            Request masterRequest = new Request("GET", "/_cluster/state/master_node");
            setOptions(masterRequest, emptyMap());
            Response masterResponse = client.performRequest(masterRequest);
            Object masterBody = new ClientYamlTestResponse(masterResponse).getBody();
            String masterNodeId = extractMasterNodeId(masterBody);

            Request nodesRequest = new Request("GET", "/_nodes");
            nodesRequest.addParameter("filter_path", "nodes.*.name,nodes.*.version");
            setOptions(nodesRequest, emptyMap());
            Response nodesResponse = client.performRequest(nodesRequest);
            ClientYamlTestResponse nodesParsed = new ClientYamlTestResponse(nodesResponse);
            String summary = formatClusterNodesSummary(nodesParsed.getBody(), masterNodeId);

            logger.info("after indices.create index [{}] cluster master_node_id [{}]; nodes {}", indexName, masterNodeId, summary);
        } catch (RuntimeException | IOException e) {
            logger.warn("failed to log cluster nodes after indices.create for index [{}]", indexName, e);
        }
    }

    private static String extractMasterNodeId(Object clusterStateBody) {
        if (clusterStateBody instanceof Map<?, ?> == false) {
            return null;
        }
        Object id = ((Map<?, ?>) clusterStateBody).get("master_node");
        return id != null ? String.valueOf(id) : null;
    }

    private static String formatClusterNodesSummary(Object nodesInfoBody, String masterNodeId) {
        if (nodesInfoBody instanceof Map<?, ?> == false) {
            return nodesInfoBody == null ? "null" : String.valueOf(nodesInfoBody);
        }
        Object nodesObj = ((Map<?, ?>) nodesInfoBody).get("nodes");
        if (nodesObj instanceof Map<?, ?> == false) {
            return String.valueOf(nodesInfoBody);
        }
        Map<?, ?> nodes = (Map<?, ?>) nodesObj;
        if (nodes.isEmpty()) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<?, ?> e : nodes.entrySet()) {
            String id = String.valueOf(e.getKey());
            if (e.getValue() instanceof Map<?, ?> n) {
                sb.append(id).append("=name:").append(n.get("name")).append(",version:").append(n.get("version"));
                if (masterNodeId != null && masterNodeId.equals(id)) {
                    sb.append(",role:master");
                }
                sb.append("; ");
            } else {
                sb.append(id).append('=').append(e.getValue()).append("; ");
            }
        }
        return sb.toString();
    }

    protected RestClient getRestClient(NodeSelector nodeSelector) {
        // lazily build a new client in case we need to point to some specific node
        return restClients.computeIfAbsent(nodeSelector, selector -> {
            RestClientBuilder builder;
            try {
                builder = clientBuilderWithSniffedNodes.get();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            builder.setNodeSelector(selector);
            return builder.build();
        });
    }

    protected static void setOptions(Request request, Map<String, String> headers) {
        RequestOptions.Builder options = request.getOptions().toBuilder();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            logger.debug("Adding header {} with value {}", header.getKey(), header.getValue());
            options.addHeader(header.getKey(), header.getValue());
        }
        // We check the warnings ourselves so we don't need the client to do it for us
        options.setWarningsHandler(WarningsHandler.PERMISSIVE);
        request.setOptions(options);
    }

    private static boolean sendBodyAsSourceParam(List<String> supportedMethods, String contentType, HttpEntity entity)
        throws ParseException, IOException {
        if (false == supportedMethods.contains(HttpGet.METHOD_NAME)) {
            // The API doesn't claim to support GET anyway
            return false;
        }
        if (entity.getContentLength() < 0) {
            // Negative length means "unknown" or "huge" in this case. Either way we can't send it as a parameter
            return false;
        }
        if (entity.getContentLength() > 2000) {
            /*
             * HTTP lines longer than 4096 bytes will cause a too_long_frame_exception
             * so we chop at 2000 just to give us some room for extra parameters and
             * url encoding.
             */
            return false;
        }
        if (false == contentType.startsWith(ContentType.APPLICATION_JSON.getMimeType())
            && false == contentType.startsWith(YAML_CONTENT_TYPE.getMimeType())) {
            // We can only encode JSON or YAML this way.
            return false;
        }
        if (frequently()) {
            return false;
        }
        /*
         * Now, the last (expensive) test: make sure the *url encoded* size
         * isn't too big. We limit ourselves to 3000 bytes for the source of
         * the request out of 4096 so we can use the rest for other parameters
         * and the url and stuff.
         */
        NameValuePair param = new BasicNameValuePair("source", EntityUtils.toString(entity));
        String encoded = URLEncodedUtils.format(List.of(param), StandardCharsets.UTF_8);
        return encoded.length() < 3000;
    }

    private ClientYamlSuiteRestApi restApi(String apiName) {
        ClientYamlSuiteRestApi restApi = restSpec.getApi(apiName);
        if (restApi == null) {
            throw new IllegalArgumentException(
                "Rest api ["
                    + apiName
                    + "] cannot be found in the rest spec. Either it doesn't exist or "
                    + "is missing from the test classpath. Check the 'restResources' block of your project's build.gradle file."
            );
        }
        return restApi;
    }

    @Override
    public void close() throws IOException {
        for (RestClient restClient : restClients.values()) {
            restClient.close();
        }
    }
}
