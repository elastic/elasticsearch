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

package org.elasticsearch.client.documentation;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.RequestLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Base64;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

/**
 * This class is used to generate the Java low-level REST client documentation.
 * You need to wrap your code between two tags like:
 * // tag::example[]
 * // end::example[]
 *
 * Where example is your tag name.
 *
 * Then in the documentation, you can extract what is between tag and end tags with
 * ["source","java",subs="attributes,callouts,macros"]
 * --------------------------------------------------
 * include-tagged::{doc-tests}/RestClientDocumentation.java[example]
 * --------------------------------------------------
 *
 * Note that this is not a test class as we are only interested in testing that docs snippets compile. We don't want
 * to send requests to a node and we don't even have the tools to do it.
 */
@SuppressWarnings("unused")
public class RestClientDocumentation {
    private static final String TOKEN = "DUMMY";

    // tag::rest-client-options-singleton
    private static final RequestOptions COMMON_OPTIONS;
    static {
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.addHeader("Authorization", "Bearer " + TOKEN); // <1>
        builder.setHttpAsyncResponseConsumerFactory(           // <2>
            new HttpAsyncResponseConsumerFactory
                .HeapBufferedResponseConsumerFactory(30 * 1024 * 1024 * 1024));
        COMMON_OPTIONS = builder.build();
    }
    // end::rest-client-options-singleton

    @SuppressWarnings("unused")
    public void usage() throws IOException, InterruptedException {

        //tag::rest-client-init
        RestClient restClient = RestClient.builder(
            new HttpHost("localhost", 9200, "http"),
            new HttpHost("localhost", 9201, "http")).build();
        //end::rest-client-init

        //tag::rest-client-close
        restClient.close();
        //end::rest-client-close

        {
            //tag::rest-client-init-default-headers
            RestClientBuilder builder = RestClient.builder(
                new HttpHost("localhost", 9200, "http"));
            Header[] defaultHeaders = new Header[]{new BasicHeader("header", "value")};
            builder.setDefaultHeaders(defaultHeaders); // <1>
            //end::rest-client-init-default-headers
        }
        {
            //tag::rest-client-init-node-selector
            RestClientBuilder builder = RestClient.builder(
                new HttpHost("localhost", 9200, "http"));
            builder.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS); // <1>
            //end::rest-client-init-node-selector
        }
        {
            //tag::rest-client-init-allocation-aware-selector
            RestClientBuilder builder = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"));
            builder.setNodeSelector(new NodeSelector() { // <1>
                @Override
                public void select(Iterable<Node> nodes) {
                    /*
                     * Prefer any node that belongs to rack_one. If none is around
                     * we will go to another rack till it's time to try and revive
                     * some of the nodes that belong to rack_one.
                     */
                    boolean foundOne = false;
                    for (Node node : nodes) {
                        String rackId = node.getAttributes().get("rack_id").get(0);
                        if ("rack_one".equals(rackId)) {
                            foundOne = true;
                            break;
                        }
                    }
                    if (foundOne) {
                        Iterator<Node> nodesIt = nodes.iterator();
                        while (nodesIt.hasNext()) {
                            Node node = nodesIt.next();
                            String rackId = node.getAttributes().get("rack_id").get(0);
                            if ("rack_one".equals(rackId) == false) {
                                nodesIt.remove();
                            }
                        }
                    }
                }
            });
            //end::rest-client-init-allocation-aware-selector
        }
        {
            //tag::rest-client-init-failure-listener
            RestClientBuilder builder = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"));
            builder.setFailureListener(new RestClient.FailureListener() {
                @Override
                public void onFailure(Node node) {
                    // <1>
                }
            });
            //end::rest-client-init-failure-listener
        }
        {
            //tag::rest-client-init-request-config-callback
            RestClientBuilder builder = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"));
            builder.setRequestConfigCallback(
                new RestClientBuilder.RequestConfigCallback() {
                    @Override
                    public RequestConfig.Builder customizeRequestConfig(
                            RequestConfig.Builder requestConfigBuilder) {
                        return requestConfigBuilder.setSocketTimeout(10000); // <1>
                    }
                });
            //end::rest-client-init-request-config-callback
        }
        {
            //tag::rest-client-init-client-config-callback
            RestClientBuilder builder = RestClient.builder(
                new HttpHost("localhost", 9200, "http"));
            builder.setHttpClientConfigCallback(new HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(
                            HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setProxy(
                            new HttpHost("proxy", 9000, "http"));  // <1>
                    }
                });
            //end::rest-client-init-client-config-callback
        }

        {
            //tag::rest-client-sync
            Request request = new Request(
                "GET",  // <1>
                "/");   // <2>
            Response response = restClient.performRequest(request);
            //end::rest-client-sync
        }
        {
            //tag::rest-client-async
            Request request = new Request(
                "GET",  // <1>
                "/");   // <2>
            Cancellable cancellable = restClient.performRequestAsync(request,
                new ResponseListener() {
                    @Override
                    public void onSuccess(Response response) {
                        // <3>
                    }

                    @Override
                    public void onFailure(Exception exception) {
                        // <4>
                    }
            });
            //end::rest-client-async
        }
        {
            Request request = new Request("GET", "/");
            //tag::rest-client-parameters
            request.addParameter("pretty", "true");
            //end::rest-client-parameters
            //tag::rest-client-body
            request.setEntity(new NStringEntity(
                    "{\"json\":\"text\"}",
                    ContentType.APPLICATION_JSON));
            //end::rest-client-body
            //tag::rest-client-body-shorter
            request.setJsonEntity("{\"json\":\"text\"}");
            //end::rest-client-body-shorter
            //tag::rest-client-options-set-singleton
            request.setOptions(COMMON_OPTIONS);
            //end::rest-client-options-set-singleton
            {
                //tag::rest-client-options-customize-header
                RequestOptions.Builder options = COMMON_OPTIONS.toBuilder();
                options.addHeader("cats", "knock things off of other things");
                request.setOptions(options);
                //end::rest-client-options-customize-header
            }
        }
        {
            HttpEntity[] documents = new HttpEntity[10];
            //tag::rest-client-async-example
            final CountDownLatch latch = new CountDownLatch(documents.length);
            for (int i = 0; i < documents.length; i++) {
                Request request = new Request("PUT", "/posts/doc/" + i);
                //let's assume that the documents are stored in an HttpEntity array
                request.setEntity(documents[i]);
                restClient.performRequestAsync(
                        request,
                        new ResponseListener() {
                            @Override
                            public void onSuccess(Response response) {
                                // <1>
                                latch.countDown();
                            }

                            @Override
                            public void onFailure(Exception exception) {
                                // <2>
                                latch.countDown();
                            }
                        }
                );
            }
            latch.await();
            //end::rest-client-async-example
        }
        {
            //tag::rest-client-async-cancel
            Request request = new Request("GET", "/posts/_search");
            Cancellable cancellable = restClient.performRequestAsync(
                request,
                new ResponseListener() {
                    @Override
                    public void onSuccess(Response response) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception exception) {
                        // <2>
                    }
                }
            );
            cancellable.cancel();
            //end::rest-client-async-cancel
        }
        {
            //tag::rest-client-response2
            Response response = restClient.performRequest(new Request("GET", "/"));
            RequestLine requestLine = response.getRequestLine(); // <1>
            HttpHost host = response.getHost(); // <2>
            int statusCode = response.getStatusLine().getStatusCode(); // <3>
            Header[] headers = response.getHeaders(); // <4>
            String responseBody = EntityUtils.toString(response.getEntity()); // <5>
            //end::rest-client-response2
        }
    }

    @SuppressWarnings("unused")
    public void commonConfiguration() throws Exception {
        {
            //tag::rest-client-config-timeouts
            RestClientBuilder builder = RestClient.builder(
                new HttpHost("localhost", 9200))
                .setRequestConfigCallback(
                    new RestClientBuilder.RequestConfigCallback() {
                        @Override
                        public RequestConfig.Builder customizeRequestConfig(
                                RequestConfig.Builder requestConfigBuilder) {
                            return requestConfigBuilder
                                .setConnectTimeout(5000)
                                .setSocketTimeout(60000);
                        }
                    });
            //end::rest-client-config-timeouts
        }
        {
            //tag::rest-client-config-threads
            RestClientBuilder builder = RestClient.builder(
                new HttpHost("localhost", 9200))
                .setHttpClientConfigCallback(new HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(
                            HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultIOReactorConfig(
                            IOReactorConfig.custom()
                                .setIoThreadCount(1)
                                .build());
                    }
                });
            //end::rest-client-config-threads
        }
        {
            //tag::rest-client-config-basic-auth
            final CredentialsProvider credentialsProvider =
                new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("user", "password"));

            RestClientBuilder builder = RestClient.builder(
                new HttpHost("localhost", 9200))
                .setHttpClientConfigCallback(new HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(
                            HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder
                            .setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
            //end::rest-client-config-basic-auth
        }
        {
            //tag::rest-client-config-disable-preemptive-auth
            final CredentialsProvider credentialsProvider =
                new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("user", "password"));

            RestClientBuilder builder = RestClient.builder(
                new HttpHost("localhost", 9200))
                .setHttpClientConfigCallback(new HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(
                            HttpAsyncClientBuilder httpClientBuilder) {
                        httpClientBuilder.disableAuthCaching(); // <1>
                        return httpClientBuilder
                            .setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
            //end::rest-client-config-disable-preemptive-auth
        }
        {
            String keyStorePass = "";
            //tag::rest-client-config-encrypted-communication
            Path trustStorePath = Paths.get("/path/to/truststore.p12");
            KeyStore truststore = KeyStore.getInstance("pkcs12");
            try (InputStream is = Files.newInputStream(trustStorePath)) {
                truststore.load(is, keyStorePass.toCharArray());
            }
            SSLContextBuilder sslBuilder = SSLContexts.custom()
                .loadTrustMaterial(truststore, null);
            final SSLContext sslContext = sslBuilder.build();
            RestClientBuilder builder = RestClient.builder(
                new HttpHost("localhost", 9200, "https"))
                .setHttpClientConfigCallback(new HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(
                            HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setSSLContext(sslContext);
                    }
                });
            //end::rest-client-config-encrypted-communication
        }
        {
            //tag::rest-client-config-trust-ca-pem
            Path caCertificatePath = Paths.get("/path/to/ca.crt");
            CertificateFactory factory =
                CertificateFactory.getInstance("X.509");
            Certificate trustedCa;
            try (InputStream is = Files.newInputStream(caCertificatePath)) {
                trustedCa = factory.generateCertificate(is);
            }
            KeyStore trustStore = KeyStore.getInstance("pkcs12");
            trustStore.load(null, null);
            trustStore.setCertificateEntry("ca", trustedCa);
            SSLContextBuilder sslContextBuilder = SSLContexts.custom()
                .loadTrustMaterial(trustStore, null);
            final SSLContext sslContext = sslContextBuilder.build();
            RestClient.builder(
                new HttpHost("localhost", 9200, "https"))
                .setHttpClientConfigCallback(new HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(
                        HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setSSLContext(sslContext);
                    }
                });
            //end::rest-client-config-trust-ca-pem
        }
        {
            String trustStorePass = "";
            String keyStorePass = "";
            //tag::rest-client-config-mutual-tls-authentication
            Path trustStorePath = Paths.get("/path/to/your/truststore.p12");
            Path keyStorePath = Paths.get("/path/to/your/keystore.p12");
            KeyStore trustStore = KeyStore.getInstance("pkcs12");
            KeyStore keyStore = KeyStore.getInstance("pkcs12");
            try (InputStream is = Files.newInputStream(trustStorePath)) {
                trustStore.load(is, trustStorePass.toCharArray());
            }
            try (InputStream is = Files.newInputStream(keyStorePath)) {
                keyStore.load(is, keyStorePass.toCharArray());
            }
            SSLContextBuilder sslBuilder = SSLContexts.custom()
                .loadTrustMaterial(trustStore, null)
                .loadKeyMaterial(keyStore, keyStorePass.toCharArray());
            final SSLContext sslContext = sslBuilder.build();
            RestClientBuilder builder = RestClient.builder(
                new HttpHost("localhost", 9200, "https"))
                .setHttpClientConfigCallback(new HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(
                        HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setSSLContext(sslContext);
                    }
                });
            //end::rest-client-config-mutual-tls-authentication
        }
        {
            //tag::rest-client-auth-bearer-token
            RestClientBuilder builder = RestClient.builder(
                new HttpHost("localhost", 9200, "http"));
            Header[] defaultHeaders =
                new Header[]{new BasicHeader("Authorization",
                    "Bearer u6iuAxZ0RG1Kcm5jVFI4eU4tZU9aVFEwT2F3")};
            builder.setDefaultHeaders(defaultHeaders);
            //end::rest-client-auth-bearer-token
        }
        {
            //tag::rest-client-auth-api-key
            String apiKeyId = "uqlEyn8B_gQ_jlvwDIvM";
            String apiKeySecret = "HxHWk2m4RN-V_qg9cDpuX";
            String apiKeyAuth =
                Base64.getEncoder().encodeToString(
                    (apiKeyId + ":" + apiKeySecret)
                        .getBytes(StandardCharsets.UTF_8));
            RestClientBuilder builder = RestClient.builder(
                new HttpHost("localhost", 9200, "http"));
            Header[] defaultHeaders =
                new Header[]{new BasicHeader("Authorization",
                    "ApiKey " + apiKeyAuth)};
            builder.setDefaultHeaders(defaultHeaders);
            //end::rest-client-auth-api-key
        }

    }
}
