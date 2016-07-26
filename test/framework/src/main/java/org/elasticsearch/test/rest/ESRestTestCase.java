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

package org.elasticsearch.test.rest;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.ssl.SSLContexts;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.SSLContext;

import static java.util.Collections.sort;
import static java.util.Collections.unmodifiableList;

/**
 * Superclass for tests that interact with an external test cluster using Elasticsearch's {@link RestClient}.
 */
public class ESRestTestCase extends ESTestCase {
    public static final String TRUSTSTORE_PATH = "truststore.path";
    public static final String TRUSTSTORE_PASSWORD = "truststore.password";

    /**
     * Convert the entity from a {@link Response} into a map of maps.
     */
    public static Map<String, Object> entityAsMap(Response response) throws IOException {
        XContentType xContentType = XContentType.fromMediaTypeOrFormat(response.getEntity().getContentType().getValue());
        try (XContentParser parser = xContentType.xContent().createParser(response.getEntity().getContent())) {
            return parser.map();
        }
    }

    private final List<HttpHost> clusterHosts;
    /**
     * A client for the running Elasticsearch cluster. Lazily initialized on first use.
     */
    private final RestClient client;
    /**
     * A client for the running Elasticsearch cluster configured to take test administrative actions like remove all indexes after the test
     * completes. Lazily initialized on first use.
     */
    private final RestClient adminClient;

    public ESRestTestCase() {
        String cluster = System.getProperty("tests.rest.cluster");
        if (cluster == null) {
            throw new RuntimeException("Must specify [tests.rest.cluster] system property with a comma delimited list of [host:port] "
                    + "to which to send REST requests");
        }
        String[] stringUrls = cluster.split(",");
        List<HttpHost> clusterHosts = new ArrayList<>(stringUrls.length);
        for (String stringUrl : stringUrls) {
            int portSeparator = stringUrl.lastIndexOf(':');
            if (portSeparator < 0) {
                throw new IllegalArgumentException("Illegal cluster url [" + stringUrl + "]");
            }
            String host = stringUrl.substring(0, portSeparator);
            int port = Integer.valueOf(stringUrl.substring(portSeparator + 1));
            clusterHosts.add(new HttpHost(host, port, getProtocol()));
        }
        this.clusterHosts = unmodifiableList(clusterHosts);
        try {
            client = buildClient(restClientSettings());
            adminClient = buildClient(restAdminSettings());
        } catch (IOException e) {
            // Wrap the IOException so children don't have to declare a constructor just to rethrow it.
            throw new RuntimeException("Error building clients", e);
        }
    }

    /**
     * Clean up after the test case.
     */
    @After
    public final void after() throws Exception {
        wipeCluster();
        logIfThereAreRunningTasks();
        closeClients();
    }

    /**
     * Get a client, building it if it hasn't been built for this test.
     */
    protected final RestClient client() {
        return client;
    }

    /**
     * Get the client used for test administrative actions. Do not use this while writing a test. Only use it for cleaning up after tests.
     */
    protected final RestClient adminClient() {
        return adminClient;
    }

    private void wipeCluster() throws IOException {
        // wipe indices
        try {
            adminClient().performRequest("DELETE", "*");
        } catch (ResponseException e) {
            // 404 here just means we had no indexes
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }

        // wipe index templates
        adminClient().performRequest("DELETE", "_template/*");

        // wipe snapshots
        // Technically this deletes all repositories and leave the snapshots in the repository. OK.
        adminClient().performRequest("DELETE", "_snapshot/*");
    }

    /**
     * Logs a message if there are still running tasks. The reasoning is that any tasks still running are state the is trying to bleed into
     * other tests.
     */
    private void logIfThereAreRunningTasks() throws InterruptedException, IOException {
        Set<String> runningTasks = runningTasks(adminClient().performRequest("GET", "_tasks"));
        // Ignore the task list API - it doens't count against us
        runningTasks.remove(ListTasksAction.NAME);
        runningTasks.remove(ListTasksAction.NAME + "[n]");
        if (runningTasks.isEmpty()) {
            return;
        }
        List<String> stillRunning = new ArrayList<>(runningTasks);
        sort(stillRunning);
        logger.info("There are still tasks running after this test that might break subsequent tests {}.", stillRunning);
        /*
         * This isn't a higher level log or outright failure because some of these tasks are run by the cluster in the background. If we
         * could determine that some tasks are run by the user we'd fail the tests if those tasks were running and ignore any background
         * tasks.
         */
    }

    private void closeClients() throws IOException {
        IOUtils.close(client, adminClient);
    }

    /**
     * Used to obtain settings for the REST client that is used to send REST requests.
     */
    protected Settings restClientSettings() {
        return Settings.EMPTY;
    }

    /**
     * Returns the REST client settings used for admin actions like cleaning up after the test has completed.
     */
    protected Settings restAdminSettings() {
        return restClientSettings(); // default to the same client settings
    }

    /**
     * Get the list of hosts in the cluster.
     */
    protected final List<HttpHost> getClusterHosts() {
        return clusterHosts;
    }

    /**
     * Override this to switch to testing https.
     */
    protected String getProtocol() {
        return "http";
    }

    private RestClient buildClient(Settings settings) throws IOException {
        RestClientBuilder builder = RestClient.builder(clusterHosts.toArray(new HttpHost[0])).setMaxRetryTimeoutMillis(30000)
                .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setSocketTimeout(30000));
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
                SSLIOSessionStrategy sessionStrategy = new SSLIOSessionStrategy(sslcontext);
                builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setSSLStrategy(sessionStrategy));
            } catch (KeyStoreException|NoSuchAlgorithmException|KeyManagementException|CertificateException e) {
                throw new RuntimeException("Error setting up ssl", e);
            }
        }

        try (ThreadContext threadContext = new ThreadContext(settings)) {
            Header[] defaultHeaders = new Header[threadContext.getHeaders().size()];
            int i = 0;
            for (Map.Entry<String, String> entry : threadContext.getHeaders().entrySet()) {
                defaultHeaders[i++] = new BasicHeader(entry.getKey(), entry.getValue());
            }
            builder.setDefaultHeaders(defaultHeaders);
        }
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    private Set<String> runningTasks(Response response) throws IOException {
        Set<String> runningTasks = new HashSet<>();

        Map<String, Object> nodes = (Map<String, Object>) entityAsMap(response).get("nodes");
        for (Map.Entry<String, Object> node : nodes.entrySet()) {
            Map<String, Object> nodeInfo = (Map<String, Object>) node.getValue();
            Map<String, Object> nodeTasks = (Map<String, Object>) nodeInfo.get("tasks");
            for (Map.Entry<String, Object> taskAndName : nodeTasks.entrySet()) {
                Map<String, Object> task = (Map<String, Object>) taskAndName.getValue();
                runningTasks.add(task.get("action").toString());
            }
        }
        return runningTasks;
    }

}
