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

package org.elasticsearch.discovery.ec2;

import com.amazonaws.util.IOUtils;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import javax.xml.XMLConstants;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(supportsDedicatedMasters = false, numDataNodes = 2, numClientNodes = 0)
@SuppressForbidden(reason = "use http server")
// TODO this should be a IT but currently all ITs in this project run against a real cluster
public class Ec2DiscoveryClusterFormationTests extends ESIntegTestCase {

    private static HttpServer httpServer;
    private static Path logDir;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(Ec2DiscoveryPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Path resolve = logDir.resolve(Integer.toString(nodeOrdinal));
        try {
            Files.createDirectory(resolve);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(AwsEc2Service.ACCESS_KEY_SETTING.getKey(), "some_access");
        secureSettings.setString(AwsEc2Service.SECRET_KEY_SETTING.getKey(), "some_secret");
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
            .put(DiscoveryModule.DISCOVERY_HOSTS_PROVIDER_SETTING.getKey(), "ec2")
            .put("path.logs", resolve)
            .put("transport.tcp.port", 0)
            .put("node.portsfile", "true")
            .put(AwsEc2Service.ENDPOINT_SETTING.getKey(), "http://" + httpServer.getAddress().getHostName() + ":" +
                httpServer.getAddress().getPort())
            .setSecureSettings(secureSettings)
            .build();
    }

    /**
     * Creates mock EC2 endpoint providing the list of started nodes to the DescribeInstances API call
     */
    @BeforeClass
    public static void startHttpd() throws Exception {
        logDir = createTempDir();
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress().getHostAddress(), 0), 0);

        httpServer.createContext("/", (s) -> {
            Headers headers = s.getResponseHeaders();
            headers.add("Content-Type", "text/xml; charset=UTF-8");
            String action = null;
            for (NameValuePair parse : URLEncodedUtils.parse(IOUtils.toString(s.getRequestBody()), StandardCharsets.UTF_8)) {
                if ("Action".equals(parse.getName())) {
                    action = parse.getValue();
                    break;
                }
            }
            assertThat(action, equalTo("DescribeInstances"));

            XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newFactory();
            xmlOutputFactory.setProperty(XMLOutputFactory.IS_REPAIRING_NAMESPACES, true);
            StringWriter out = new StringWriter();
            XMLStreamWriter sw;
            try {
                sw = xmlOutputFactory.createXMLStreamWriter(out);
                sw.writeStartDocument();

                String namespace = "http://ec2.amazonaws.com/doc/2013-02-01/";
                sw.setDefaultNamespace(namespace);
                sw.writeStartElement(XMLConstants.DEFAULT_NS_PREFIX, "DescribeInstancesResponse", namespace);
                {
                    sw.writeStartElement("requestId");
                    sw.writeCharacters(UUID.randomUUID().toString());
                    sw.writeEndElement();

                    sw.writeStartElement("reservationSet");
                    {
                        Path[] files = FileSystemUtils.files(logDir);
                        for (int i = 0; i < files.length; i++) {
                            Path resolve = files[i].resolve("transport.ports");
                            if (Files.exists(resolve)) {
                                List<String> addresses = Files.readAllLines(resolve);
                                Collections.shuffle(addresses, random());

                                sw.writeStartElement("item");
                                {
                                    sw.writeStartElement("reservationId");
                                    sw.writeCharacters(UUID.randomUUID().toString());
                                    sw.writeEndElement();

                                    sw.writeStartElement("instancesSet");
                                    {
                                        sw.writeStartElement("item");
                                        {
                                            sw.writeStartElement("instanceId");
                                            sw.writeCharacters(UUID.randomUUID().toString());
                                            sw.writeEndElement();

                                            sw.writeStartElement("imageId");
                                            sw.writeCharacters(UUID.randomUUID().toString());
                                            sw.writeEndElement();

                                            sw.writeStartElement("instanceState");
                                            {
                                                sw.writeStartElement("code");
                                                sw.writeCharacters("16");
                                                sw.writeEndElement();

                                                sw.writeStartElement("name");
                                                sw.writeCharacters("running");
                                                sw.writeEndElement();
                                            }
                                            sw.writeEndElement();

                                            sw.writeStartElement("privateDnsName");
                                            sw.writeCharacters(addresses.get(0));
                                            sw.writeEndElement();

                                            sw.writeStartElement("dnsName");
                                            sw.writeCharacters(addresses.get(0));
                                            sw.writeEndElement();

                                            sw.writeStartElement("instanceType");
                                            sw.writeCharacters("m1.medium");
                                            sw.writeEndElement();

                                            sw.writeStartElement("placement");
                                            {
                                                sw.writeStartElement("availabilityZone");
                                                sw.writeCharacters("use-east-1e");
                                                sw.writeEndElement();

                                                sw.writeEmptyElement("groupName");

                                                sw.writeStartElement("tenancy");
                                                sw.writeCharacters("default");
                                                sw.writeEndElement();
                                            }
                                            sw.writeEndElement();

                                            sw.writeStartElement("privateIpAddress");
                                            sw.writeCharacters(addresses.get(0));
                                            sw.writeEndElement();

                                            sw.writeStartElement("ipAddress");
                                            sw.writeCharacters(addresses.get(0));
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
                Loggers.getLogger(Ec2DiscoveryClusterFormationTests.class).error("Failed serializing XML", e);
                throw new RuntimeException(e);
            }
        });

        httpServer.start();
    }

    @AfterClass
    public static void stopHttpd() throws IOException {
        for (int i = 0; i < internalCluster().size(); i++) {
            // shut them all down otherwise we get spammed with connection refused exceptions
            internalCluster().stopRandomDataNode();
        }
        httpServer.stop(0);
        httpServer = null;
        logDir = null;
    }

    public void testJoin() throws ExecutionException, InterruptedException {
        // only wait for the cluster to form
        assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(2)).get());
        // add one more node and wait for it to join
        internalCluster().startDataOnlyNode();
        assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(3)).get());
    }
}
