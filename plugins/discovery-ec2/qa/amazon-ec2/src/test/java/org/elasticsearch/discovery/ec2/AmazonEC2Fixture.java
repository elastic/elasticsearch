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
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.rest.RestStatus;

import javax.xml.XMLConstants;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;

/**
 * {@link AmazonEC2Fixture} is a fixture that emulates an AWS EC2 service.
 * <p>
 * It starts an asynchronous socket server that binds to a random local port.
 */
public class AmazonEC2Fixture {

    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 2) {
            throw new IllegalArgumentException("AmazonEC2Fixture <working directory> <nodes transport uri file>");
        }

        final InetSocketAddress socketAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
        final HttpServer httpServer = MockHttpServer.createHttp(socketAddress, 0);

        try {
            final Path workingDirectory = toPath(args[0]);
            /// Writes the PID of the current Java process in a `pid` file located in the working directory
            writeFile(workingDirectory, "pid", ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);

            final String addressAndPort = addressToString(httpServer.getAddress());
            // Writes the address and port of the http server in a `ports` file located in the working directory
            writeFile(workingDirectory, "ports", addressAndPort);

            httpServer.createContext("/", new ResponseHandler(toPath(args[1])));
            httpServer.start();

            // Wait to be killed
            Thread.sleep(Long.MAX_VALUE);

        } finally {
            httpServer.stop(0);
        }
    }

    @SuppressForbidden(reason = "Paths#get is fine - we don't have environment here")
    private static Path toPath(final String dir) {
        return Paths.get(dir);
    }

    private static void writeFile(final Path dir, final String fileName, final String content) throws IOException {
        final Path tempPidFile = Files.createTempFile(dir, null, null);
        Files.write(tempPidFile, singleton(content));
        Files.move(tempPidFile, dir.resolve(fileName), StandardCopyOption.ATOMIC_MOVE);
    }

    private static String addressToString(final SocketAddress address) {
        final InetSocketAddress inetSocketAddress = (InetSocketAddress) address;
        if (inetSocketAddress.getAddress() instanceof Inet6Address) {
            return "[" + inetSocketAddress.getHostString() + "]:" + inetSocketAddress.getPort();
        } else {
            return inetSocketAddress.getHostString() + ":" + inetSocketAddress.getPort();
        }
    }

    static class ResponseHandler implements HttpHandler {

        private final Path discoveryPath;

        ResponseHandler(final Path discoveryPath) {
            this.discoveryPath = discoveryPath;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            RestStatus responseStatus = RestStatus.INTERNAL_SERVER_ERROR;
            String responseBody = null;
            String responseContentType = "text/plain";

            final String path = exchange.getRequestURI().getRawPath();
            if ("/".equals(path)) {
                final String method = exchange.getRequestMethod();
                final Headers headers = exchange.getRequestHeaders();

                if ("GET".equals(method) && matchingHeader(headers, "User-agent", v -> v.startsWith("Apache Ant"))) {
                    // Replies to the fixture's waiting condition
                    responseStatus = RestStatus.OK;
                    responseBody = "AmazonEC2Fixture";

                } else if ("POST".equals(method) && matchingHeader(headers, "User-agent", v -> v.startsWith("aws-sdk-java"))) {
                    // Simulate an EC2 DescribeInstancesResponse
                    responseStatus = RestStatus.OK;
                    responseContentType = "text/xml; charset=UTF-8";

                    for (NameValuePair parse : URLEncodedUtils.parse(IOUtils.toString(exchange.getRequestBody()), StandardCharsets.UTF_8)) {
                        if ("Action".equals(parse.getName())) {
                            responseBody = generateDescribeInstancesResponse();
                            break;
                        }
                    }
                }
            }

            final byte[] response = responseBody != null ? responseBody.getBytes(StandardCharsets.UTF_8) : new byte[0];
            exchange.sendResponseHeaders(responseStatus.getStatus(), response.length);
            exchange.getResponseHeaders().put("Content-Type", singletonList(responseContentType));
            if (response.length > 0) {
                exchange.getResponseBody().write(response);
            }
            exchange.close();
        }

        /** Checks if the given {@link Headers} contains a header with a given name which has a value that matches a predicate **/
        private boolean matchingHeader(final Headers headers, final String headerName, final Predicate<String> predicate) {
            if (headers != null && headers.isEmpty() == false) {
                final List<String> values = headers.get(headerName);
                if (values != null) {
                    for (String value : values) {
                        if (predicate.test(value)) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        /**
         * Generates a XML response that describe the EC2 instances
         */
        private String generateDescribeInstancesResponse() {
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
                        if (Files.exists(discoveryPath)) {
                            for (String address : Files.readAllLines(discoveryPath)) {

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
            return out.toString();
        }
    }
}
