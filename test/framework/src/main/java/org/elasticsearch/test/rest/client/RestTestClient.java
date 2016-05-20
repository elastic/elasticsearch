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
package org.elasticsearch.test.rest.client;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContexts;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.ElasticsearchResponse;
import org.elasticsearch.client.ElasticsearchResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.rest.spec.RestApi;
import org.elasticsearch.test.rest.spec.RestSpec;

import javax.net.ssl.SSLContext;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * REST client used to test the elasticsearch REST layer
 * Holds the {@link RestSpec} used to translate api calls into REST calls
 */
public class RestTestClient implements Closeable {

    public static final String PROTOCOL = "protocol";
    public static final String TRUSTSTORE_PATH = "truststore.path";
    public static final String TRUSTSTORE_PASSWORD = "truststore.password";


    private static final ESLogger logger = Loggers.getLogger(RestTestClient.class);
    //query_string params that don't need to be declared in the spec, thay are supported by default
    private static final Set<String> ALWAYS_ACCEPTED_QUERY_STRING_PARAMS = Sets.newHashSet("pretty", "source", "filter_path");

    private final RestSpec restSpec;
    private final RestClient restClient;
    private final Version esVersion;

    public RestTestClient(RestSpec restSpec, Settings settings, URL[] urls) throws IOException {
        assert urls.length > 0;
        this.restSpec = restSpec;
        this.restClient = createRestClient(urls, settings);
        this.esVersion = readAndCheckVersion(urls);
        logger.info("REST client initialized {}, elasticsearch version: [{}]", urls, esVersion);
    }

    private Version readAndCheckVersion(URL[] urls) throws IOException {
        RestApi restApi = restApi("info");
        assert restApi.getPaths().size() == 1;
        assert restApi.getMethods().size() == 1;

        String version = null;
        for (URL ignored : urls) {
            //we don't really use the urls here, we rely on the client doing round-robin to touch all the nodes in the cluster
            String method = restApi.getMethods().get(0);
            String endpoint = restApi.getPaths().get(0);
            ElasticsearchResponse elasticsearchResponse = restClient.performRequest(method, endpoint, Collections.emptyMap(), null);
            RestTestResponse restTestResponse = new RestTestResponse(elasticsearchResponse);
            Object latestVersion = restTestResponse.evaluate("version.number");
            if (latestVersion == null) {
                throw new RuntimeException("elasticsearch version not found in the response");
            }
            if (version == null) {
                version = latestVersion.toString();
            } else {
                if (!latestVersion.equals(version)) {
                    throw new IllegalArgumentException("provided nodes addresses run different elasticsearch versions");
                }
            }
        }
        return Version.fromString(version);
    }

    public Version getEsVersion() {
        return esVersion;
    }

    /**
     * Calls an api with the provided parameters and body
     */
    public RestTestResponse callApi(String apiName, Map<String, String> params, String body, Map<String, String> headers)
            throws IOException {

        if ("raw".equals(apiName)) {
            // Raw requests are bit simpler....
            HashMap<String, String> queryStringParams = new HashMap<>(params);
            String method = Objects.requireNonNull(queryStringParams.remove("method"), "Method must be set to use raw request");
            String path = "/"+ Objects.requireNonNull(queryStringParams.remove("path"), "Path must be set to use raw request");
            HttpEntity entity = null;
            if (body != null && body.length() > 0) {
                entity = new StringEntity(body, RestClient.JSON_CONTENT_TYPE);
            }
            // And everything else is a url parameter!
            ElasticsearchResponse response = restClient.performRequest(method, path, queryStringParams, entity);
            return new RestTestResponse(response);
        }

        List<Integer> ignores = new ArrayList<>();
        Map<String, String> requestParams;
        if (params == null) {
            requestParams = Collections.emptyMap();
        } else {
            requestParams = new HashMap<>(params);
            if (params.isEmpty() == false) {
                //ignore is a special parameter supported by the clients, shouldn't be sent to es
                String ignoreString = requestParams.remove("ignore");
                if (ignoreString != null) {
                    try {
                        ignores.add(Integer.valueOf(ignoreString));
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("ignore value should be a number, found [" + ignoreString + "] instead");
                    }
                }
            }
        }

        //create doesn't exist in the spec but is supported in the clients (index with op_type=create)
        boolean indexCreateApi = "create".equals(apiName);
        String api = indexCreateApi ? "index" : apiName;
        RestApi restApi = restApi(api);

        //divide params between ones that go within query string and ones that go within path
        Map<String, String> pathParts = new HashMap<>();
        Map<String, String> queryStringParams = new HashMap<>();
        for (Map.Entry<String, String> entry : requestParams.entrySet()) {
            if (restApi.getPathParts().contains(entry.getKey())) {
                pathParts.put(entry.getKey(), entry.getValue());
            } else {
                if (restApi.getParams().contains(entry.getKey()) || ALWAYS_ACCEPTED_QUERY_STRING_PARAMS.contains(entry.getKey())) {
                    queryStringParams.put(entry.getKey(), entry.getValue());
                } else {
                    throw new IllegalArgumentException("param [" + entry.getKey() + "] not supported in ["
                            + restApi.getName() + "] " + "api");
                }
            }
        }

        if (indexCreateApi) {
            queryStringParams.put("op_type", "create");
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
                requestMethod = "GET";
            } else {
                requestMethod = RandomizedTest.randomFrom(supportedMethods);
                requestBody = new StringEntity(body, RestClient.JSON_CONTENT_TYPE);
            }
        } else {
            if (restApi.isBodyRequired()) {
                throw new IllegalArgumentException("body is required by [" + restApi.getName() + "] api");
            }
            requestMethod = RandomizedTest.randomFrom(supportedMethods);
        }

        //the rest path to use is randomized out of the matching ones (if more than one)
        RestPath restPath = RandomizedTest.randomFrom(restApi.getFinalPaths(pathParts));
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
            ElasticsearchResponse response = restClient.performRequest(requestMethod, requestPath,
                    queryStringParams, requestBody, requestHeaders);
            return new RestTestResponse(response);
        } catch(ElasticsearchResponseException e) {
            if (ignores.contains(e.getElasticsearchResponse().getStatusLine().getStatusCode())) {
                return new RestTestResponse(e);
            }
            throw e;
        }
    }

    private RestApi restApi(String apiName) {
        RestApi restApi = restSpec.getApi(apiName);
        if (restApi == null) {
            throw new IllegalArgumentException("rest api [" + apiName + "] doesn't exist in the rest spec");
        }
        return restApi;
    }

    protected RestClient createRestClient(URL[] urls, Settings settings) throws IOException {
        SSLConnectionSocketFactory sslsf;
        String keystorePath = settings.get(TRUSTSTORE_PATH);
        if (keystorePath != null) {
            final String keystorePass = settings.get(TRUSTSTORE_PASSWORD);
            if (keystorePass == null) {
                throw new IllegalStateException(TRUSTSTORE_PATH + " is provided but not " + TRUSTSTORE_PASSWORD);
            }
            Path path = PathUtils.get(keystorePath);
            if (!Files.exists(path)) {
                throw new IllegalStateException(TRUSTSTORE_PATH + " is set but points to a non-existing file");
            }
            try {
                KeyStore keyStore = KeyStore.getInstance("jks");
                try (InputStream is = Files.newInputStream(path)) {
                    keyStore.load(is, keystorePass.toCharArray());
                }
                SSLContext sslcontext = SSLContexts.custom().loadTrustMaterial(keyStore, null).build();
                sslsf = new SSLConnectionSocketFactory(sslcontext);
            } catch (KeyStoreException|NoSuchAlgorithmException|KeyManagementException|CertificateException e) {
                throw new RuntimeException(e);
            }
        } else {
            sslsf = SSLConnectionSocketFactory.getSocketFactory();
        }

        Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory())
                .register("https", sslsf)
                .build();

        List<Header> headers = new ArrayList<>();
        try (ThreadContext threadContext = new ThreadContext(settings)) {
            for (Map.Entry<String, String> entry : threadContext.getHeaders().entrySet()) {
                headers.add(new BasicHeader(entry.getKey(), entry.getValue()));
            }
        }

        CloseableHttpClient httpClient = HttpClientBuilder.create().setDefaultHeaders(headers)
                .setConnectionManager(new PoolingHttpClientConnectionManager(socketFactoryRegistry)).build();

        String protocol = settings.get(PROTOCOL, "http");
        HttpHost[] hosts = new HttpHost[urls.length];
        for (int i = 0; i < hosts.length; i++) {
            URL url = urls[i];
            hosts[i] = new HttpHost(url.getHost(), url.getPort(), protocol);
        }
        return RestClient.builder().setHttpClient(httpClient).setHosts(hosts).build();
    }

    /**
     * Closes the REST client and the underlying http client
     */
    @Override
    public void close() {
        IOUtils.closeWhileHandlingException(restClient);
    }
}
