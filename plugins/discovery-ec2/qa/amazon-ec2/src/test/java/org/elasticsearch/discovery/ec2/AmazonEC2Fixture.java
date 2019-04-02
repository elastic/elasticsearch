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

import org.apache.http.NameValuePair;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URLEncodedUtils;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.fixture.AbstractHttpFixture;

import javax.xml.XMLConstants;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * {@link AmazonEC2Fixture} is a fixture that emulates an AWS EC2 service.
 */
public class AmazonEC2Fixture extends AbstractHttpFixture {

    private final Path nodes;

    private AmazonEC2Fixture(final String workingDir, final String nodesUriPath) {
        super(workingDir);
        this.nodes = toPath(Objects.requireNonNull(nodesUriPath));
    }

    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 2) {
            throw new IllegalArgumentException("AmazonEC2Fixture <working directory> <nodes transport uri file>");
        }

        final AmazonEC2Fixture fixture = new AmazonEC2Fixture(args[0], args[1]);
        fixture.listen();
    }

    @Override
    protected Response handle(final Request request) throws IOException {
        if ("/".equals(request.getPath()) && (HttpPost.METHOD_NAME.equals(request.getMethod()))) {
            final String userAgent = request.getHeader("User-Agent");
            if (userAgent != null && userAgent.startsWith("aws-sdk-java")) {
                // Simulate an EC2 DescribeInstancesResponse
                byte[] responseBody = EMPTY_BYTE;
                for (NameValuePair parse : URLEncodedUtils.parse(new String(request.getBody(), UTF_8), UTF_8)) {
                    if ("Action".equals(parse.getName())) {
                        responseBody = generateDescribeInstancesResponse();
                        break;
                    }
                }
                return new Response(RestStatus.OK.getStatus(), contentType("text/xml; charset=UTF-8"), responseBody);
            }
        }
        if ("/latest/meta-data/local-ipv4".equals(request.getPath()) && (HttpGet.METHOD_NAME.equals(request.getMethod()))) {
            return new Response(RestStatus.OK.getStatus(), TEXT_PLAIN_CONTENT_TYPE, "127.0.0.1".getBytes(UTF_8));
        }
        return null;
    }

    /**
     * Generates a XML response that describe the EC2 instances
     */
    private byte[] generateDescribeInstancesResponse() {
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
                    if (Files.exists(nodes)) {
                        for (String address : Files.readAllLines(nodes)) {

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

    @SuppressForbidden(reason = "Paths#get is fine - we don't have environment here")
    private static Path toPath(final String dir) {
        return Paths.get(dir);
    }
}
