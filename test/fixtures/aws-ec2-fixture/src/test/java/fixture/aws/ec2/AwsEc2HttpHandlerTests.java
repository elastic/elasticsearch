/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package fixture.aws.ec2;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpPrincipal;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

public class AwsEc2HttpHandlerTests extends ESTestCase {

    public void testDescribeInstances() throws IOException, XMLStreamException {
        final List<String> addresses = randomList(
            1,
            10,
            () -> "10.0." + between(1, 254) + "." + between(1, 254) + ":" + between(1025, 65535)
        );

        final var handler = new AwsEc2HttpHandler((ignored1, ignored2) -> true, () -> addresses);

        final var response = handleRequest(handler);
        assertEquals(RestStatus.OK, response.status());

        final var unseenAddressesInTags = Stream.of("privateDnsName", "dnsName", "privateIpAddress", "ipAddress")
            .collect(
                Collectors.toMap(
                    localName -> new QName("http://ec2.amazonaws.com/doc/2013-02-01/", localName),
                    localName -> new HashSet<>(addresses)
                )
            );

        final var xmlStreamReader = XMLInputFactory.newDefaultFactory().createXMLStreamReader(response.body().streamInput());
        try {
            for (; xmlStreamReader.getEventType() != XMLStreamConstants.END_DOCUMENT; xmlStreamReader.next()) {
                if (xmlStreamReader.getEventType() == XMLStreamConstants.START_ELEMENT) {
                    final var unseenAddresses = unseenAddressesInTags.get(xmlStreamReader.getName());
                    if (unseenAddresses != null) {
                        xmlStreamReader.next();
                        assertEquals(XMLStreamConstants.CHARACTERS, xmlStreamReader.getEventType());
                        final var currentAddress = xmlStreamReader.getText();
                        assertTrue(currentAddress, unseenAddresses.remove(currentAddress));
                    }
                }
            }
        } finally {
            xmlStreamReader.close();
        }

        assertTrue(unseenAddressesInTags.toString(), unseenAddressesInTags.values().stream().allMatch(HashSet::isEmpty));
    }

    private record TestHttpResponse(RestStatus status, BytesReference body) {}

    private static TestHttpResponse handleRequest(AwsEc2HttpHandler handler) {
        final var httpExchange = new TestHttpExchange(
            "POST",
            "/",
            new BytesArray("Action=DescribeInstances"),
            TestHttpExchange.EMPTY_HEADERS
        );
        try {
            handler.handle(httpExchange);
        } catch (IOException e) {
            fail(e);
        }
        assertNotEquals(0, httpExchange.getResponseCode());
        return new TestHttpResponse(RestStatus.fromCode(httpExchange.getResponseCode()), httpExchange.getResponseBodyContents());
    }

    private static class TestHttpExchange extends HttpExchange {

        private static final Headers EMPTY_HEADERS = new Headers();

        private final String method;
        private final URI uri;
        private final BytesReference requestBody;
        private final Headers requestHeaders;

        private final Headers responseHeaders = new Headers();
        private final BytesStreamOutput responseBody = new BytesStreamOutput();
        private int responseCode;

        TestHttpExchange(String method, String uri, BytesReference requestBody, Headers requestHeaders) {
            this.method = method;
            this.uri = URI.create(uri);
            this.requestBody = requestBody;
            this.requestHeaders = requestHeaders;
        }

        @Override
        public Headers getRequestHeaders() {
            return requestHeaders;
        }

        @Override
        public Headers getResponseHeaders() {
            return responseHeaders;
        }

        @Override
        public URI getRequestURI() {
            return uri;
        }

        @Override
        public String getRequestMethod() {
            return method;
        }

        @Override
        public HttpContext getHttpContext() {
            return null;
        }

        @Override
        public void close() {}

        @Override
        public InputStream getRequestBody() {
            try {
                return requestBody.streamInput();
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public OutputStream getResponseBody() {
            return responseBody;
        }

        @Override
        public void sendResponseHeaders(int rCode, long responseLength) {
            this.responseCode = rCode;
        }

        @Override
        public InetSocketAddress getRemoteAddress() {
            return null;
        }

        @Override
        public int getResponseCode() {
            return responseCode;
        }

        public BytesReference getResponseBodyContents() {
            return responseBody.bytes();
        }

        @Override
        public InetSocketAddress getLocalAddress() {
            return null;
        }

        @Override
        public String getProtocol() {
            return "HTTP/1.1";
        }

        @Override
        public Object getAttribute(String name) {
            return null;
        }

        @Override
        public void setAttribute(String name, Object value) {
            fail("setAttribute not implemented");
        }

        @Override
        public void setStreams(InputStream i, OutputStream o) {
            fail("setStreams not implemented");
        }

        @Override
        public HttpPrincipal getPrincipal() {
            fail("getPrincipal not implemented");
            throw new UnsupportedOperationException("getPrincipal not implemented");
        }
    }

}
