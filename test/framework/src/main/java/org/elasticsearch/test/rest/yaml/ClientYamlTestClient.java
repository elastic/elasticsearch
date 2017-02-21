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
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestApi;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestPath;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestSpec;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Used by {@link ESClientYamlSuiteTestCase} to execute REST requests according to the tests written in yaml suite files. Wraps a
 * {@link RestClient} instance used to send the REST requests. Holds the {@link ClientYamlSuiteRestSpec} used to translate api calls into
 * REST calls.
 */
public class ClientYamlTestClient {
    private static final Logger logger = Loggers.getLogger(ClientYamlTestClient.class);

    private final ClientYamlSuiteRestSpec restSpec;
    private final RestClient restClient;
    private final Version esVersion;

    public ClientYamlTestClient(ClientYamlSuiteRestSpec restSpec, RestClient restClient, List<HttpHost> hosts,
                                Version esVersion) throws IOException {
        assert hosts.size() > 0;
        this.restSpec = restSpec;
        this.restClient = restClient;
        this.esVersion = esVersion;
    }

    public Version getEsVersion() {
        return esVersion;
    }

    /**
     * Calls an api with the provided parameters and body
     */
    public ClientYamlTestResponse callApi(String apiName, Map<String, String> params, String body, Map<String, String> headers)
            throws IOException {

        if ("raw".equals(apiName)) {
            // Raw requests are bit simpler....
            Map<String, String> queryStringParams = new HashMap<>(params);
            String method = Objects.requireNonNull(queryStringParams.remove("method"), "Method must be set to use raw request");
            String path = "/"+ Objects.requireNonNull(queryStringParams.remove("path"), "Path must be set to use raw request");
            HttpEntity entity = null;
            if (body != null && body.length() > 0) {
                entity = new StringEntity(body, ContentType.APPLICATION_JSON);
            }
            // And everything else is a url parameter!
            try {
                Response response = restClient.performRequest(method, path, queryStringParams, entity);
                return new ClientYamlTestResponse(response);
            } catch(ResponseException e) {
                throw new ClientYamlTestResponseException(e);
            }
        }

        ClientYamlSuiteRestApi restApi = restApi(apiName);

        //divide params between ones that go within query string and ones that go within path
        Map<String, String> pathParts = new HashMap<>();
        Map<String, String> queryStringParams = new HashMap<>();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (restApi.getPathParts().contains(entry.getKey())) {
                pathParts.put(entry.getKey(), entry.getValue());
            } else {
                if (restApi.getParams().contains(entry.getKey()) || restSpec.isGlobalParameter(entry.getKey())
                        || restSpec.isClientParameter(entry.getKey())) {
                    queryStringParams.put(entry.getKey(), entry.getValue());
                } else {
                    throw new IllegalArgumentException("param [" + entry.getKey() + "] not supported in ["
                            + restApi.getName() + "] " + "api");
                }
            }
        }

        List<String> supportedMethods = restApi.getSupportedMethods(pathParts.keySet());
        String requestMethod;
        StringEntity requestBody = null;
        if (Strings.hasLength(body)) {
            if (!restApi.isBodySupported()) {
                throw new IllegalArgumentException("body is not supported by [" + restApi.getName() + "] api");
            }
            //randomly test the GET with source param instead of GET/POST with body
            if (supportedMethods.contains("GET") && RandomizedTest.rarely()) {
                logger.debug("sending the request body as source param with GET method");
                queryStringParams.put("source", body);
                queryStringParams.put("source_content_type", ContentType.APPLICATION_JSON.toString());
                requestMethod = "GET";
            } else {
                requestMethod = RandomizedTest.randomFrom(supportedMethods);
                requestBody = new StringEntity(body, ContentType.APPLICATION_JSON);
            }
        } else {
            if (restApi.isBodyRequired()) {
                throw new IllegalArgumentException("body is required by [" + restApi.getName() + "] api");
            }
            requestMethod = RandomizedTest.randomFrom(supportedMethods);
        }

        //the rest path to use is randomized out of the matching ones (if more than one)
        ClientYamlSuiteRestPath restPath = RandomizedTest.randomFrom(restApi.getFinalPaths(pathParts));
        //Encode rules for path and query string parameters are different. We use URI to encode the path.
        //We need to encode each path part separately, as each one might contain slashes that need to be escaped, which needs to
        //be done manually.
        String requestPath;
        if (restPath.getPathParts().length == 0) {
            requestPath = "/";
        } else {
            StringBuilder finalPath = new StringBuilder();
            for (String pathPart : restPath.getPathParts()) {
                try {
                    finalPath.append('/');
                    // We append "/" to the path part to handle parts that start with - or other invalid characters
                    URI uri = new URI(null, null, null, -1, "/" + pathPart, null, null);
                    //manually escape any slash that each part may contain
                    finalPath.append(uri.getRawPath().substring(1).replaceAll("/", "%2F"));
                } catch (URISyntaxException e) {
                    throw new RuntimeException("unable to build uri", e);
                }
            }
            requestPath = finalPath.toString();
        }

        Header[] requestHeaders = new Header[headers.size()];
        int index = 0;
        for (Map.Entry<String, String> header : headers.entrySet()) {
            logger.info("Adding header {}\n with value {}", header.getKey(), header.getValue());
            requestHeaders[index++] = new BasicHeader(header.getKey(), header.getValue());
        }

        logger.debug("calling api [{}]", apiName);
        try {
            Response response = restClient.performRequest(requestMethod, requestPath, queryStringParams, requestBody, requestHeaders);
            return new ClientYamlTestResponse(response);
        } catch(ResponseException e) {
            throw new ClientYamlTestResponseException(e);
        }
    }

    private ClientYamlSuiteRestApi restApi(String apiName) {
        ClientYamlSuiteRestApi restApi = restSpec.getApi(apiName);
        if (restApi == null) {
            throw new IllegalArgumentException("rest api [" + apiName + "] doesn't exist in the rest spec");
        }
        return restApi;
    }
}
