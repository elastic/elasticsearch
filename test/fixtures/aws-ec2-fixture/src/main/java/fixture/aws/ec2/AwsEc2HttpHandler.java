/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package fixture.aws.ec2;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.apache.http.client.utils.URLEncodedUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Supplier;

import javax.xml.XMLConstants;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import static fixture.aws.AwsCredentialsUtils.checkAuthorization;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.test.ESTestCase.randomIdentifier;
import static org.junit.Assert.assertNull;

/**
 * Minimal HTTP handler that emulates the AWS EC2 endpoint (at least, just the DescribeInstances action therein)
 */
@SuppressForbidden(reason = "this test uses a HttpServer to emulate the AWS EC2 endpoint")
public class AwsEc2HttpHandler implements HttpHandler {

    private final BiPredicate<String, String> authorizationPredicate;
    private final Supplier<List<String>> transportAddressesSupplier;

    public AwsEc2HttpHandler(BiPredicate<String, String> authorizationPredicate, Supplier<List<String>> transportAddressesSupplier) {
        this.authorizationPredicate = Objects.requireNonNull(authorizationPredicate);
        this.transportAddressesSupplier = Objects.requireNonNull(transportAddressesSupplier);
    }

    @Override
    public void handle(final HttpExchange exchange) throws IOException {
        try (exchange) {

            if ("POST".equals(exchange.getRequestMethod()) && "/".equals(exchange.getRequestURI().getPath())) {

                if (checkAuthorization(authorizationPredicate, exchange) == false) {
                    return;
                }

                final var parsedRequest = new HashMap<String, String>();
                for (final var nameValuePair : URLEncodedUtils.parse(new String(exchange.getRequestBody().readAllBytes(), UTF_8), UTF_8)) {
                    assertNull(nameValuePair.getName(), parsedRequest.put(nameValuePair.getName(), nameValuePair.getValue()));
                }

                if ("DescribeInstances".equals(parsedRequest.get("Action")) == false) {
                    throw new UnsupportedOperationException(parsedRequest.toString());
                }

                final var responseBody = generateDescribeInstancesResponse();
                exchange.getResponseHeaders().add("Content-Type", "text/xml; charset=UTF-8");
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), responseBody.length);
                exchange.getResponseBody().write(responseBody);
                return;
            }

            throw new UnsupportedOperationException("can only handle DescribeInstances requests");

        } catch (Exception e) {
            ExceptionsHelper.maybeDieOnAnotherThread(new AssertionError(e));
        }
    }

    private static final String XML_NAMESPACE = "http://ec2.amazonaws.com/doc/2013-02-01/";

    private byte[] generateDescribeInstancesResponse() {
        final XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newFactory();
        xmlOutputFactory.setProperty(XMLOutputFactory.IS_REPAIRING_NAMESPACES, true);

        final StringWriter out = new StringWriter();
        XMLStreamWriter sw;
        try {
            sw = xmlOutputFactory.createXMLStreamWriter(out);
            sw.writeStartDocument();

            sw.setDefaultNamespace(XML_NAMESPACE);
            sw.writeStartElement(XMLConstants.DEFAULT_NS_PREFIX, "DescribeInstancesResponse", XML_NAMESPACE);
            {
                sw.writeStartElement("requestId");
                sw.writeCharacters(randomIdentifier());
                sw.writeEndElement();

                sw.writeStartElement("reservationSet");
                {
                    for (final var address : transportAddressesSupplier.get()) {

                        sw.writeStartElement("item");
                        {
                            sw.writeStartElement("reservationId");
                            sw.writeCharacters(randomIdentifier());
                            sw.writeEndElement();

                            sw.writeStartElement("instancesSet");
                            {
                                sw.writeStartElement("item");
                                {
                                    sw.writeStartElement("instanceId");
                                    sw.writeCharacters(randomIdentifier());
                                    sw.writeEndElement();

                                    sw.writeStartElement("imageId");
                                    sw.writeCharacters(randomIdentifier());
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
                                    sw.writeCharacters("m1.medium"); // TODO randomize
                                    sw.writeEndElement();

                                    sw.writeStartElement("placement");
                                    {
                                        sw.writeStartElement("availabilityZone");
                                        sw.writeCharacters(randomIdentifier());
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
            ExceptionsHelper.maybeDieOnAnotherThread(new AssertionError(e));
            throw new RuntimeException(e);
        }
        return out.toString().getBytes(UTF_8);
    }
}
