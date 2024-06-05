/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery.azure.classic;

import com.microsoft.windowsazure.management.compute.models.DeploymentSlot;
import com.microsoft.windowsazure.management.compute.models.DeploymentStatus;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import org.elasticsearch.cloud.azure.classic.management.AzureComputeService;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugin.discovery.azure.classic.AzureDiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportSettings;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.xml.XMLConstants;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0)
@SuppressForbidden(reason = "use http server")
public class AzureDiscoveryClusterFormationTests extends ESIntegTestCase {

    public static class TestPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return Arrays.asList(AzureComputeService.Management.ENDPOINT_SETTING);
        }
    }

    private static HttpsServer httpsServer;
    private static Path logDir;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(AzureDiscoveryPlugin.class, TestPlugin.class);
    }

    private static Path keyStoreFile;

    @ClassRule
    public static final ExternalResource MUTE_IN_FIPS_JVM = new ExternalResource() {
        @Override
        protected void before() {
            assumeFalse("Can't run in a FIPS JVM because none of the supported Keystore types can be used", inFipsJvm());
        }
    };

    @BeforeClass
    public static void setupKeyStore() throws IOException {
        Path tempDir = createTempDir();
        keyStoreFile = tempDir.resolve("test-node.jks");
        try (InputStream stream = AzureDiscoveryClusterFormationTests.class.getResourceAsStream("/test-node.jks")) {
            assertNotNull("can't find keystore file", stream);
            Files.copy(stream, keyStoreFile);
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Path resolve = logDir.resolve(Integer.toString(nodeOrdinal));
        try {
            Files.createDirectory(resolve);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING.getKey(), AzureDiscoveryPlugin.AZURE)
            .put(Environment.PATH_LOGS_SETTING.getKey(), resolve)
            .put(TransportSettings.PORT.getKey(), 0)
            .put(Node.WRITE_PORTS_FILE_SETTING.getKey(), "true")
            .put(
                AzureComputeService.Management.ENDPOINT_SETTING.getKey(),
                "https://" + InetAddress.getLoopbackAddress().getHostAddress() + ":" + httpsServer.getAddress().getPort()
            )
            .put(AzureComputeService.Management.KEYSTORE_PATH_SETTING.getKey(), keyStoreFile.toAbsolutePath())
            .put(AzureComputeService.Discovery.HOST_TYPE_SETTING.getKey(), AzureSeedHostsProvider.HostType.PUBLIC_IP.name())
            .put(AzureComputeService.Management.KEYSTORE_PASSWORD_SETTING.getKey(), "keypass")
            .put(AzureComputeService.Management.KEYSTORE_TYPE_SETTING.getKey(), "jks")
            .put(AzureComputeService.Management.SERVICE_NAME_SETTING.getKey(), "myservice")
            .put(AzureComputeService.Management.SUBSCRIPTION_ID_SETTING.getKey(), "subscription")
            .put(AzureComputeService.Discovery.DEPLOYMENT_NAME_SETTING.getKey(), "mydeployment")
            .put(AzureComputeService.Discovery.ENDPOINT_NAME_SETTING.getKey(), "myendpoint")
            .put(AzureComputeService.Discovery.DEPLOYMENT_SLOT_SETTING.getKey(), AzureSeedHostsProvider.Deployment.PRODUCTION.name())
            .build();
    }

    @Override
    protected Path nodeConfigPath(int nodeOrdinal) {
        return keyStoreFile.getParent();
    }

    /**
     * Creates mock EC2 endpoint providing the list of started nodes to the DescribeInstances API call
     */
    @BeforeClass
    public static void startHttpd() throws Exception {
        logDir = createTempDir();
        SSLContext sslContext = getSSLContext();
        httpsServer = MockHttpServer.createHttps(new InetSocketAddress(InetAddress.getLoopbackAddress().getHostAddress(), 0), 0);
        httpsServer.setHttpsConfigurator(new HttpsConfigurator(sslContext));
        httpsServer.createContext("/subscription/services/hostedservices/myservice", (s) -> {
            Headers headers = s.getResponseHeaders();
            headers.add("Content-Type", "text/xml; charset=UTF-8");
            XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newFactory();
            xmlOutputFactory.setProperty(XMLOutputFactory.IS_REPAIRING_NAMESPACES, true);
            StringWriter out = new StringWriter();
            XMLStreamWriter sw;
            try {
                sw = xmlOutputFactory.createXMLStreamWriter(out);
                sw.writeStartDocument();

                String namespace = "http://schemas.microsoft.com/windowsazure";
                sw.setDefaultNamespace(namespace);
                sw.writeStartElement(XMLConstants.DEFAULT_NS_PREFIX, "HostedService", namespace);
                {
                    sw.writeStartElement("Deployments");
                    {
                        Path[] files = FileSystemUtils.files(logDir);
                        for (int i = 0; i < files.length; i++) {
                            Path resolve = files[i].resolve("transport.ports");
                            if (Files.exists(resolve)) {
                                List<String> addresses = Files.readAllLines(resolve);
                                Collections.shuffle(addresses, random());
                                String address = addresses.get(0);
                                int indexOfLastColon = address.lastIndexOf(':');
                                String host = address.substring(0, indexOfLastColon);
                                String port = address.substring(indexOfLastColon + 1);

                                sw.writeStartElement("Deployment");
                                {
                                    sw.writeStartElement("Name");
                                    sw.writeCharacters("mydeployment");
                                    sw.writeEndElement();

                                    sw.writeStartElement("DeploymentSlot");
                                    sw.writeCharacters(DeploymentSlot.Production.name());
                                    sw.writeEndElement();

                                    sw.writeStartElement("Status");
                                    sw.writeCharacters(DeploymentStatus.Running.name());
                                    sw.writeEndElement();

                                    sw.writeStartElement("RoleInstanceList");
                                    {
                                        sw.writeStartElement("RoleInstance");
                                        {
                                            sw.writeStartElement("RoleName");
                                            sw.writeCharacters(UUID.randomUUID().toString());
                                            sw.writeEndElement();

                                            sw.writeStartElement("IpAddress");
                                            sw.writeCharacters(host);
                                            sw.writeEndElement();

                                            sw.writeStartElement("InstanceEndpoints");
                                            {
                                                sw.writeStartElement("InstanceEndpoint");
                                                {
                                                    sw.writeStartElement("Name");
                                                    sw.writeCharacters("myendpoint");
                                                    sw.writeEndElement();

                                                    sw.writeStartElement("Vip");
                                                    sw.writeCharacters(host);
                                                    sw.writeEndElement();

                                                    sw.writeStartElement("PublicPort");
                                                    sw.writeCharacters(port);
                                                    sw.writeEndElement();
                                                }
                                                sw.writeEndElement();
                                            }
                                            sw.writeEndElement();
                                        }
                                        sw.writeEndElement();
                                    }
                                    sw.writeEndElement();
                                }
                                sw.writeEndElement();
                            }
                        }
                    }
                    sw.writeEndElement();
                }
                sw.writeEndElement();

                sw.writeEndDocument();
                sw.flush();

                final byte[] responseAsBytes = out.toString().getBytes(StandardCharsets.UTF_8);
                s.sendResponseHeaders(200, responseAsBytes.length);
                OutputStream responseBody = s.getResponseBody();
                responseBody.write(responseAsBytes);
                responseBody.close();
            } catch (XMLStreamException e) {
                LogManager.getLogger(AzureDiscoveryClusterFormationTests.class).error("Failed serializing XML", e);
                throw new RuntimeException(e);
            }
        });

        httpsServer.start();
    }

    private static SSLContext getSSLContext() throws Exception {
        char[] passphrase = "keypass".toCharArray();
        KeyStore ks = KeyStore.getInstance("JKS");
        try (InputStream stream = AzureDiscoveryClusterFormationTests.class.getResourceAsStream("/test-node.jks")) {
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

    @AfterClass
    public static void stopHttpd() throws IOException {
        try {
            // shut them all down otherwise we get spammed with connection refused exceptions
            internalCluster().close();
        } finally {
            httpsServer.stop(0);
            httpsServer = null;
            logDir = null;
        }
    }

    public void testJoin() throws ExecutionException, InterruptedException {
        // only wait for the cluster to form
        ensureClusterSizeConsistency();
        // add one more node and wait for it to join
        internalCluster().startDataOnlyNode();
        ensureClusterSizeConsistency();
    }
}
