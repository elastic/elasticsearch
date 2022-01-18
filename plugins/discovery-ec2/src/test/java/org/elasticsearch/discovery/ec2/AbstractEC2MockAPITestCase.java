/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.discovery.ec2;

import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Tag;
import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import javax.xml.XMLConstants;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressForbidden(reason = "use a http server")
public abstract class AbstractEC2MockAPITestCase extends ESTestCase {

    protected HttpServer httpServer;

    protected ThreadPool threadPool;

    protected MockTransportService transportService;

    protected NetworkService networkService = new NetworkService(Collections.emptyList());

    @Before
    public void setUp() throws Exception {
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.start();
        threadPool = new TestThreadPool(EC2RetriesTests.class.getName());
        transportService = createTransportService();
        super.setUp();
    }

    protected abstract MockTransportService createTransportService();

    protected Settings buildSettings(String accessKey) {
        final InetSocketAddress address = httpServer.getAddress();
        final String endpoint = "http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort();
        final MockSecureSettings mockSecure = new MockSecureSettings();
        mockSecure.setString(Ec2ClientSettings.ACCESS_KEY_SETTING.getKey(), accessKey);
        mockSecure.setString(Ec2ClientSettings.SECRET_KEY_SETTING.getKey(), "ec2_secret_key");
        return Settings.builder().put(Ec2ClientSettings.ENDPOINT_SETTING.getKey(), endpoint).setSecureSettings(mockSecure).build();
    }

    @After
    public void tearDown() throws Exception {
        try {
            IOUtils.close(transportService, () -> terminate(threadPool), () -> httpServer.stop(0));
        } finally {
            super.tearDown();
        }
    }

    /**
     * Generates a XML response that describe the EC2 instances
     * TODO: org.elasticsearch.discovery.ec2.AmazonEC2Fixture uses pretty much the same code. We should dry up that test fixture.
     */
    static byte[] generateDescribeInstancesResponse(List<Instance> instances) {
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
                    for (Instance instance : instances) {
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
                                    sw.writeCharacters(instance.getInstanceId());
                                    sw.writeEndElement();

                                    sw.writeStartElement("imageId");
                                    sw.writeCharacters(instance.getImageId());
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
                                    sw.writeCharacters(instance.getPrivateDnsName());
                                    sw.writeEndElement();

                                    sw.writeStartElement("dnsName");
                                    sw.writeCharacters(instance.getPublicDnsName());
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
                                    sw.writeCharacters(instance.getPrivateIpAddress());
                                    sw.writeEndElement();

                                    sw.writeStartElement("ipAddress");
                                    sw.writeCharacters(instance.getPublicIpAddress());
                                    sw.writeEndElement();

                                    sw.writeStartElement("tagSet");
                                    for (Tag tag : instance.getTags()) {
                                        sw.writeStartElement("item");
                                        {
                                            sw.writeStartElement("key");
                                            sw.writeCharacters(tag.getKey());
                                            sw.writeEndElement();

                                            sw.writeStartElement("value");
                                            sw.writeCharacters(tag.getValue());
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
