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
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.support.Headers;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.elasticsearch.test.rest.spec.RestApi;
import org.elasticsearch.test.rest.spec.RestSpec;

import javax.net.ssl.SSLContext;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * REST client used to test the elasticsearch REST layer
 * Holds the {@link RestSpec} used to translate api calls into REST calls
 */
public class RestClient implements Closeable {

    public static final String PROTOCOL = "protocol";
    public static final String TRUSTSTORE_PATH = "truststore.path";
    public static final String TRUSTSTORE_PASSWORD = "truststore.password";

    private static final ESLogger logger = Loggers.getLogger(RestClient.class);
    //query_string params that don't need to be declared in the spec, thay are supported by default
    private static final Set<String> ALWAYS_ACCEPTED_QUERY_STRING_PARAMS = Sets.newHashSet("pretty", "source", "filter_path");

    private final String protocol;
    private final RestSpec restSpec;
    private final CloseableHttpClient httpClient;
    private final Headers headers;
    private final InetSocketAddress[] addresses;
    private final Version esVersion;

    public RestClient(RestSpec restSpec, Settings settings, InetSocketAddress[] addresses) throws IOException, RestException {
        assert addresses.length > 0;
        this.restSpec = restSpec;
        this.headers = new Headers(settings);
        this.protocol = settings.get(PROTOCOL, "http");
        this.httpClient = createHttpClient(settings);
        this.addresses = addresses;
        this.esVersion = readAndCheckVersion();
        logger.info("REST client initialized {}, elasticsearch version: [{}]", addresses, esVersion);
    }

    private Version readAndCheckVersion() throws IOException, RestException {
        //we make a manual call here without using callApi method, mainly because we are initializing
        //and the randomized context doesn't exist for the current thread (would be used to choose the method otherwise)
        RestApi restApi = restApi("info");
        assert restApi.getPaths().size() == 1;
        assert restApi.getMethods().size() == 1;

        String version = null;
        for (InetSocketAddress address : addresses) {
            RestResponse restResponse = new RestResponse(httpRequestBuilder(address)
                    .path(restApi.getPaths().get(0))
                    .method(restApi.getMethods().get(0)).execute());
            checkStatusCode(restResponse);

            Object latestVersion = restResponse.evaluate("version.number");
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
     * @throws RestException if the obtained status code is non ok, unless the specific error code needs to be ignored
     * according to the ignore parameter received as input (which won't get sent to elasticsearch)
     */
    public RestResponse callApi(String apiName, Map<String, String> params, String body) throws IOException, RestException {

        List<Integer> ignores = new ArrayList<>();
        Map<String, String> requestParams = null;
        if (params != null) {
            //makes a copy of the parameters before modifying them for this specific request
            requestParams = new HashMap<>(params);
            //ignore is a special parameter supported by the clients, shouldn't be sent to es
            String ignoreString = requestParams.remove("ignore");
            if (Strings.hasLength(ignoreString)) {
                try {
                    ignores.add(Integer.valueOf(ignoreString));
                } catch(NumberFormatException e) {
                    throw new IllegalArgumentException("ignore value should be a number, found [" + ignoreString + "] instead");
                }
            }
        }

        HttpRequestBuilder httpRequestBuilder = callApiBuilder(apiName, requestParams, body);
        logger.debug("calling api [{}]", apiName);
        HttpResponse httpResponse = httpRequestBuilder.execute();

        // http HEAD doesn't support response body
        // For the few api (exists class of api) that use it we need to accept 404 too
        if (!httpResponse.supportsBody()) {
            ignores.add(404);
        }

        RestResponse restResponse = new RestResponse(httpResponse);
        checkStatusCode(restResponse, ignores);
        return restResponse;
    }

    private void checkStatusCode(RestResponse restResponse, List<Integer> ignores) throws RestException {
        //ignore is a catch within the client, to prevent the client from throwing error if it gets non ok codes back
        if (ignores.contains(restResponse.getStatusCode())) {
            if (logger.isDebugEnabled()) {
                logger.debug("ignored non ok status codes {} as requested", ignores);
            }
            return;
        }
        checkStatusCode(restResponse);
    }

    private void checkStatusCode(RestResponse restResponse) throws RestException {
        if (restResponse.isError()) {
            throw new RestException("non ok status code [" + restResponse.getStatusCode() + "] returned", restResponse);
        }
    }

    private HttpRequestBuilder callApiBuilder(String apiName, Map<String, String> params, String body) {

        //create doesn't exist in the spec but is supported in the clients (index with op_type=create)
        boolean indexCreateApi = "create".equals(apiName);
        String api = indexCreateApi ? "index" : apiName;
        RestApi restApi = restApi(api);

        HttpRequestBuilder httpRequestBuilder = httpRequestBuilder();

        //divide params between ones that go within query string and ones that go within path
        Map<String, String> pathParts = new HashMap<>();
        if (params != null) {
            for (Map.Entry<String, String> entry : params.entrySet()) {
                if (restApi.getPathParts().contains(entry.getKey())) {
                    pathParts.put(entry.getKey(), entry.getValue());
                } else {
                    if (restApi.getParams().contains(entry.getKey()) || ALWAYS_ACCEPTED_QUERY_STRING_PARAMS.contains(entry.getKey())) {
                        httpRequestBuilder.addParam(entry.getKey(), entry.getValue());
                    } else {
                        throw new IllegalArgumentException("param [" + entry.getKey() + "] not supported in [" + restApi.getName() + "] api");
                    }
                }
            }
        }

        if (indexCreateApi) {
            httpRequestBuilder.addParam("op_type", "create");
        }

        List<String> supportedMethods = restApi.getSupportedMethods(pathParts.keySet());
        if (Strings.hasLength(body)) {
            if (!restApi.isBodySupported()) {
                throw new IllegalArgumentException("body is not supported by [" + restApi.getName() + "] api");
            }
            //test the GET with source param instead of GET/POST with body
            if (supportedMethods.contains("GET") && RandomizedTest.rarely()) {
                logger.debug("sending the request body as source param with GET method");
                httpRequestBuilder.addParam("source", body).method("GET");
            } else {
                httpRequestBuilder.body(body).method(RandomizedTest.randomFrom(supportedMethods));
            }
        } else {
            if (restApi.isBodyRequired()) {
                throw new IllegalArgumentException("body is required by [" + restApi.getName() + "] api");
            }
            httpRequestBuilder.method(RandomizedTest.randomFrom(supportedMethods));
        }

        //the http method is randomized (out of the available ones with the chosen api)
        return httpRequestBuilder.path(RandomizedTest.randomFrom(restApi.getFinalPaths(pathParts)));
    }

    private RestApi restApi(String apiName) {
        RestApi restApi = restSpec.getApi(apiName);
        if (restApi == null) {
            throw new IllegalArgumentException("rest api [" + apiName + "] doesn't exist in the rest spec");
        }
        return restApi;
    }

    protected HttpRequestBuilder httpRequestBuilder(InetSocketAddress address) {
        return new HttpRequestBuilder(httpClient)
                .addHeaders(headers)
                .protocol(protocol)
                .host(NetworkAddress.formatAddress(address.getAddress())).port(address.getPort());
    }

    protected HttpRequestBuilder httpRequestBuilder() {
        //the address used is randomized between the available ones
        InetSocketAddress address = RandomizedTest.randomFrom(addresses);
        return httpRequestBuilder(address);
    }

    protected CloseableHttpClient createHttpClient(Settings settings) throws IOException {
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
                SSLContext sslcontext = SSLContexts.custom()
                        .loadTrustMaterial(keyStore, null)
                        .build();
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
        return HttpClients.createMinimal(new PoolingHttpClientConnectionManager(socketFactoryRegistry, null, null, null, 15, TimeUnit.SECONDS));
    }

    /**
     * Closes the REST client and the underlying http client
     */
    @Override
    public void close() {
        IOUtils.closeWhileHandlingException(httpClient);
    }
}
