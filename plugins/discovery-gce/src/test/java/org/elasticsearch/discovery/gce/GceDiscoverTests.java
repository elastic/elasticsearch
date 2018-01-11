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

package org.elasticsearch.discovery.gce;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.cloud.gce.GceInstancesService;
import org.elasticsearch.cloud.gce.GceInstancesServiceImpl;
import org.elasticsearch.cloud.gce.GceMetadataService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.plugin.discovery.gce.GceDiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;


@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, transportClientRatio = 0.0, numClientNodes = 0)
@SuppressForbidden(reason = "use http server")
// TODO this should be a IT but currently all ITs in this project run against a real cluster
public class GceDiscoverTests extends ESIntegTestCase {

    public static class TestPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return Arrays.asList(GceMetadataService.GCE_HOST, GceInstancesServiceImpl.GCE_ROOT_URL,
                GceInstancesServiceImpl.GCE_VALIDATE_CERTIFICATES);
        }
    }

    private static HttpsServer httpsServer;
    private static HttpServer httpServer;
    private static Path logDir;

    private static final Map<String, DiscoveryNode> nodes = new ConcurrentHashMap<>();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(GceDiscoveryPlugin.class, TestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Path resolve = logDir.resolve(Integer.toString(nodeOrdinal));
        try {
            Files.createDirectory(resolve);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
            .put("discovery.zen.hosts_provider", "gce")
            .put("path.logs", resolve)
            .put("transport.tcp.port", 0)
            .put("cloud.gce.project_id", "testproject")
            .put("cloud.gce.zone", "primaryzone")
            .put("cloud.gce.host", "http://" + httpServer.getAddress().getHostName() + ":" + httpServer.getAddress().getPort())
            .put("cloud.gce.root_url", "https://" + httpsServer.getAddress().getHostName() +
                ":" + httpsServer.getAddress().getPort())
            // this is annoying but by default the client pulls a static list of trusted CAs
            .put("cloud.gce.validate_certificates", false)
            .put(GceInstancesService.CONNECTION_TIMEOUT_SETTING.getKey(), "5s")
            .put(GceInstancesService.READ_TIMEOUT_SETTING.getKey(), "1s")
            .build();
    }

    @Override
    protected boolean addTestZenDiscovery() {
        return false;
    }

    @BeforeClass
    public static void startHttpd() throws Exception {
        logDir = createTempDir();
        SSLContext sslContext = getSSLContext();
        httpsServer = MockHttpServer.createHttps(new InetSocketAddress(InetAddress.getLoopbackAddress().getHostAddress(), 0), 0);
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress().getHostAddress(), 0), 0);
        httpsServer.setHttpsConfigurator(new HttpsConfigurator(sslContext));
        httpServer.createContext("/computeMetadata/v1/instance/service-accounts/default/token", (s) -> {
            String response = GceMockUtils.readGoogleInternalJsonResponse(
                "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token");
            byte[] responseAsBytes = response.getBytes(StandardCharsets.UTF_8);
            s.sendResponseHeaders(200, responseAsBytes.length);
            OutputStream responseBody = s.getResponseBody();
            responseBody.write(responseAsBytes);
            responseBody.close();
        });

        httpsServer.createContext("/compute/v1/projects/testproject/zones/primaryzone/instances", (s) -> {
            final Headers headers = s.getResponseHeaders();
            headers.add("Content-Type", "application/json; charset=UTF-8");

            final XContentBuilder builder = jsonBuilder().startObject();
            {
                builder.field("id", "test");
                builder.startArray("items");
                {
                    for (Map.Entry<String, DiscoveryNode> node : nodes.entrySet()) {
                        builder.startObject();
                        builder.field("description", node.getKey());
                        builder.field("status", "RUNNING");
                        builder.startArray("networkInterfaces");
                        {
                            builder.startObject();
                            final TransportAddress transportAddress = node.getValue().getAddress();
                            builder.field("networkIP", NetworkAddress.format(transportAddress.address()));
                            builder.endObject();
                        }
                        builder.endArray();
                        builder.endObject();
                    }
                }
                builder.endArray();
            }
            builder.endObject();

            final OutputStream out = s.getResponseBody();
            try {
                final BytesReference responseAsBytes = builder.bytes();
                s.sendResponseHeaders(200, responseAsBytes.length());
                responseAsBytes.writeTo(out);
            } catch (Exception e) {
                byte[] responseAsBytes =  ("{ \"error\" : {\"message\" : \"" + e.toString() + "\" } }").getBytes(StandardCharsets.UTF_8);
                s.sendResponseHeaders(500, responseAsBytes.length);
                out.write(responseAsBytes);
            } finally {
                IOUtils.close(out);
            }
        });
        httpsServer.start();
        httpServer.start();
    }

    private static SSLContext getSSLContext() throws Exception{
        char[] passphrase = "keypass".toCharArray();
        KeyStore ks = KeyStore.getInstance("JKS");
        try (InputStream stream = GceDiscoverTests.class.getResourceAsStream("/test-node.jks")) {
            assertNotNull("can't find keystore file", stream);
            ks.load(stream, passphrase);
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        kmf.init(ks, passphrase);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
        tmf.init(ks);
        SSLContext ssl = SSLContext.getInstance("TLS");
        ssl.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        return ssl;
    }

    @After
    public void clearGceNodes() {
        nodes.clear();
    }

    @AfterClass
    public static void stopHttpd() throws IOException {
        for (int i = 0; i < internalCluster().size(); i++) {
            // shut them all down otherwise we get spammed with connection refused exceptions
            internalCluster().stopRandomDataNode();
        }

        final int delayInSeconds = 10;
        httpsServer.stop(delayInSeconds);
        httpServer.stop(delayInSeconds);
        httpsServer = null;
        httpServer = null;
        logDir = null;
    }

    /**
     * Register an existing node as a GCE node, exposing its address
     *
     * @param nodeName the name of the node
     */
    private void registerGceNode(final String nodeName) {
        TransportService transportService = internalCluster().getInstance(TransportService.class, nodeName);
        assertNotNull(transportService);
        DiscoveryNode discoveryNode = transportService.getLocalNode();
        assertNotNull(discoveryNode);
        if (nodes.put(discoveryNode.getName(), discoveryNode) != null) {
            throw new IllegalArgumentException("Node [" + discoveryNode.getName() + "] cannot be registered twice");
        }
    }

    private void assertNumberOfNodes(int expected) {
        NodesInfoResponse nodeInfos = client().admin().cluster().prepareNodesInfo().clear().execute().actionGet();
        assertNotNull(nodeInfos);
        assertNotNull(nodeInfos.getNodes());
        assertEquals(expected, nodeInfos.getNodes().size());
    }

    public void testJoin() throws ExecutionException, InterruptedException {
        registerGceNode(internalCluster().startMasterOnlyNode());
        assertNotNull(client().admin().cluster().prepareState().setMasterNodeTimeout("1s").get().getState().nodes().getMasterNodeId());

        registerGceNode(internalCluster().startNode());
        assertNotNull(client().admin().cluster().prepareState().setMasterNodeTimeout("1s").get().getState().nodes().getMasterNodeId());

        // wait for the cluster to form
        assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(2)).get());
        assertNumberOfNodes(2);

        // add one more node and wait for it to join
        registerGceNode(internalCluster().startDataOnlyNode());
        assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(3)).get());
        assertNumberOfNodes(3);
    }
}
