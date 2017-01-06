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
import org.junit.AfterClass;
import org.junit.Before;

import javax.net.ssl.SSLContext;
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

import static java.util.Collections.singletonMap;
import static java.util.Collections.sort;
import static java.util.Collections.unmodifiableList;

/**
 * Superclass for tests that interact with an external test cluster using Elasticsearch's {@link RestClient}.
 */
public abstract class ESRestTestCase extends ESTestCase {
    public static final String TRUSTSTORE_PATH = "truststore.path";
    public static final String TRUSTSTORE_PASSWORD = "truststore.password";

    /**
     * Convert the entity from a {@link Response} into a map of maps.
     */
    public Map<String, Object> entityAsMap(Response response) throws IOException {
        XContentType xContentType = XContentType.fromMediaTypeOrFormat(response.getEntity().getContentType().getValue());
        try (XContentParser parser = createParser(xContentType.xContent(), response.getEntity().getContent())) {
            return parser.map();
        }
    }

    private static List<HttpHost> clusterHosts;
    /**
     * A client for the running Elasticsearch cluster
     */
    private static RestClient client;
    /**
     * A client for the running Elasticsearch cluster configured to take test administrative actions like remove all indexes after the test
     * completes
     */
    private static RestClient adminClient;

    @Before
    public void initClient() throws IOException {
        if (client == null) {
            assert adminClient == null;
            assert clusterHosts == null;
            String cluster = System.getProperty("tests.rest.cluster");
            if (cluster == null) {
                throw new RuntimeException("Must specify [tests.rest.cluster] system property with a comma delimited list of [host:port] "
                        + "to which to send REST requests");
            }
            String[] stringUrls = cluster.split(",");
            List<HttpHost> hosts = new ArrayList<>(stringUrls.length);
            for (String stringUrl : stringUrls) {
                int portSeparator = stringUrl.lastIndexOf(':');
                if (portSeparator < 0) {
                    throw new IllegalArgumentException("Illegal cluster url [" + stringUrl + "]");
                }
                String host = stringUrl.substring(0, portSeparator);
                int port = Integer.valueOf(stringUrl.substring(portSeparator + 1));
                hosts.add(new HttpHost(host, port, getProtocol()));
            }
            clusterHosts = unmodifiableList(hosts);
            logger.info("initializing REST clients against {}", clusterHosts);
            client = buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[clusterHosts.size()]));
            adminClient = buildClient(restAdminSettings(), clusterHosts.toArray(new HttpHost[clusterHosts.size()]));
        }
        assert client != null;
        assert adminClient != null;
        assert clusterHosts != null;
    }

    /**
     * Clean up after the test case.
     */
    @After
    public final void cleanUpCluster() throws Exception {
        wipeCluster();
        logIfThereAreRunningTasks();
    }

    @AfterClass
    public static void closeClients() throws IOException {
        try {
            IOUtils.close(client, adminClient);
        } finally {
            clusterHosts = null;
            client = null;
            adminClient = null;
        }
    }

    /**
     * Get the client used for ordinary api calls while writing a test
     */
    protected static RestClient client() {
        return client;
    }

    /**
     * Get the client used for test administrative actions. Do not use this while writing a test. Only use it for cleaning up after tests.
     */
    protected static RestClient adminClient() {
        return adminClient;
    }

    /**
     * Returns whether to preserve the indices created during this test on completion of this test.
     * Defaults to {@code false}. Override this method if indices should be preserved after the test,
     * with the assumption that some other process or test will clean up the indices afterward.
     * This is useful if the data directory and indices need to be preserved between test runs
     * (for example, when testing rolling upgrades).
     */
    protected boolean preserveIndicesUponCompletion() {
        return false;
    }

    /**
     * Controls whether or not to preserve templates upon completion of this test. The default implementation is to delete not preserve
     * templates.
     *
     * @return whether or not to preserve templates
     */
    protected boolean preserveTemplatesUponCompletion() {
        return false;
    }

    /**
     * Returns whether to preserve the repositories on completion of this test.
     */
    protected boolean preserveReposUponCompletion() {
        return false;
    }

    private void wipeCluster() throws IOException {
        if (preserveIndicesUponCompletion() == false) {
            // wipe indices
            try {
                adminClient().performRequest("DELETE", "*");
            } catch (ResponseException e) {
                // 404 here just means we had no indexes
                if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                    throw e;
                }
            }
        }

        // wipe index templates
        if (preserveTemplatesUponCompletion() == false) {
            adminClient().performRequest("DELETE", "_template/*");
        }

        wipeSnapshots();
    }

    /**
     * Wipe fs snapshots we created one by one and all repositories so that the next test can create the repositories fresh and they'll
     * start empty. There isn't an API to delete all snapshots. There is an API to delete all snapshot repositories but that leaves all of
     * the snapshots intact in the repository.
     */
    private void wipeSnapshots() throws IOException {
        for (Map.Entry<String, ?> repo : entityAsMap(adminClient.performRequest("GET", "_snapshot/_all")).entrySet()) {
            String repoName = repo.getKey();
            Map<?, ?> repoSpec = (Map<?, ?>) repo.getValue();
            String repoType = (String) repoSpec.get("type");
            if (repoType.equals("fs")) {
                // All other repo types we really don't have a chance of being able to iterate properly, sadly.
                String url = "_snapshot/" + repoName + "/_all";
                Map<String, String> params = singletonMap("ignore_unavailable", "true");
                List<?> snapshots = (List<?>) entityAsMap(adminClient.performRequest("GET", url, params)).get("snapshots");
                for (Object snapshot : snapshots) {
                    Map<?, ?> snapshotInfo = (Map<?, ?>) snapshot;
                    String name = (String) snapshotInfo.get("snapshot");
                    logger.debug("wiping snapshot [{}/{}]", repoName, name);
                    adminClient().performRequest("DELETE", "_snapshot/" + repoName + "/" + name);
                }
            }
            if (preserveReposUponCompletion() == false) {
                logger.debug("wiping snapshot repository [{}]", repoName);
                adminClient().performRequest("DELETE", "_snapshot/" + repoName);
            }
        }
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

    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        RestClientBuilder builder = RestClient.builder(hosts);
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
