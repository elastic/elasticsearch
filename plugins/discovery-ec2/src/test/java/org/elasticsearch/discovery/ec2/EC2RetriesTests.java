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

import com.amazonaws.http.HttpMethodName;
import com.sun.net.httpserver.HttpServer;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.discovery.SeedHostsProvider;
import org.elasticsearch.discovery.SeedHostsResolver;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.nio.MockNioTransport;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import javax.xml.XMLConstants;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import java.io.IOException;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.is;

@SuppressForbidden(reason = "use a http server")
public class EC2RetriesTests extends ESTestCase {

    private HttpServer httpServer;

    private ThreadPool threadPool;

    private MockTransportService transportService;

    private NetworkService networkService = new NetworkService(Collections.emptyList());

    @Before
    public void setUp() throws Exception {
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.start();
        threadPool = new TestThreadPool(EC2RetriesTests.class.getName());
        final MockNioTransport transport = new MockNioTransport(Settings.EMPTY, Version.CURRENT, threadPool, networkService,
            PageCacheRecycler.NON_RECYCLING_INSTANCE, new NamedWriteableRegistry(Collections.emptyList()),
            new NoneCircuitBreakerService());
        transportService =
            new MockTransportService(Settings.EMPTY, transport, threadPool, TransportService.NOOP_TRANSPORT_INTERCEPTOR, null);
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        try {
            IOUtils.close(transportService, () -> terminate(threadPool), () -> httpServer.stop(0));
        } finally {
            super.tearDown();
        }
    }

    public void testEC2DiscoveryRetriesOnRateLimiting() throws IOException {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/51685", inFipsJvm());
        final String accessKey = "ec2_access";
        final List<String> hosts = List.of("127.0.0.1:9000");
        final Map<String, Integer> failedRequests = new ConcurrentHashMap<>();
        // retry the same request 5 times at most
        final int maxRetries = randomIntBetween(1, 5);
        httpServer.createContext("/", exchange -> {
            if (exchange.getRequestMethod().equals(HttpMethodName.POST.name())) {
                final String request = new String(exchange.getRequestBody().readAllBytes(), UTF_8);
                final String userAgent = exchange.getRequestHeaders().getFirst("User-Agent");
                if (userAgent != null && userAgent.startsWith("aws-sdk-java")) {
                    final String auth = exchange.getRequestHeaders().getFirst("Authorization");
                    if (auth == null || auth.contains(accessKey) == false) {
                        throw new IllegalArgumentException("wrong access key: " + auth);
                    }
                    if (failedRequests.compute(exchange.getRequestHeaders().getFirst("Amz-sdk-invocation-id"),
                        (requestId, count) -> Objects.requireNonNullElse(count, 0) + 1) < maxRetries) {
                        exchange.sendResponseHeaders(HttpStatus.SC_SERVICE_UNAVAILABLE, -1);
                        return;
                    }
                    // Simulate an EC2 DescribeInstancesResponse
                    byte[] responseBody = null;
                    for (NameValuePair parse : URLEncodedUtils.parse(request, UTF_8)) {
                        if ("Action".equals(parse.getName())) {
                            responseBody = generateDescribeInstancesResponse(hosts);
                            break;
                        }
                    }
                    responseBody = responseBody == null ? new byte[0] : responseBody;
                    exchange.getResponseHeaders().set("Content-Type", "text/xml; charset=UTF-8");
                    exchange.sendResponseHeaders(HttpStatus.SC_OK, responseBody.length);
                    exchange.getResponseBody().write(responseBody);
                    return;
                }
            }
            fail("did not send response");
        });

        final InetSocketAddress address = httpServer.getAddress();
        final String endpoint = "http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort();
        final MockSecureSettings mockSecure = new MockSecureSettings();
        mockSecure.setString(Ec2ClientSettings.ACCESS_KEY_SETTING.getKey(), accessKey);
        mockSecure.setString(Ec2ClientSettings.SECRET_KEY_SETTING.getKey(), "ec2_secret");
        try (Ec2DiscoveryPlugin plugin = new Ec2DiscoveryPlugin(
            Settings.builder().put(Ec2ClientSettings.ENDPOINT_SETTING.getKey(), endpoint).setSecureSettings(mockSecure).build())) {
            final SeedHostsProvider seedHostsProvider = plugin.getSeedHostProviders(transportService, networkService).get("ec2").get();
            final SeedHostsResolver resolver = new SeedHostsResolver("test", Settings.EMPTY, transportService, seedHostsProvider);
            resolver.start();
            final List<TransportAddress> addressList = seedHostsProvider.getSeedAddresses(resolver);
            assertThat(addressList, Matchers.hasSize(1));
            assertThat(addressList.get(0).toString(), is(hosts.get(0)));
            assertThat(failedRequests, aMapWithSize(1));
            assertThat(failedRequests.values().iterator().next(), is(maxRetries));
        }
    }

    /**
     * Generates a XML response that describe the EC2 instances
     * TODO: org.elasticsearch.discovery.ec2.AmazonEC2Fixture uses pretty much the same code. We should dry up that test fixture.
     */
    private byte[] generateDescribeInstancesResponse(List<String> nodes) {
        final XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newFactory();
        xmlOutputFactory.setProperty(XMLOutputFactory.IS_REPAIRING_NAMESPACES, true);

        final StringWriter out = new StringWriter();
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
                    for (String address : nodes) {
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
                                    sw.writeCharacters(address);
                                    sw.writeEndElement();

                                    sw.writeStartElement("dnsName");
                                    sw.writeCharacters(address);
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
                                    sw.writeCharacters(address);
                                    sw.writeEndElement();

                                    sw.writeStartElement("ipAddress");
                                    sw.writeCharacters(address);
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

                sw.writeEndDocument();
                sw.flush();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return out.toString().getBytes(UTF_8);
    }
}
