/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.equalTo;

@SuppressForbidden(reason = "uses httpserver by design")
public class HttpClientTests extends ESTestCase {

    private static HttpServer server;

    @BeforeClass
    public static void startServer() throws Throwable {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/hello/", exchange -> {
            try {
                String response = "hello world";
                exchange.sendResponseHeaders(200, response.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            } catch (Exception e) {
                fail(e);
            }
        });
        server.createContext("/404/", exchange -> {
            try {
                exchange.sendResponseHeaders(404, 0);
            } catch (Exception e) {
                fail(e);
            }
        });
        server.createContext("/redirect", exchange -> {
            // path is either like this: /redirect/count/destination/
            // or just: /redirect
            try {
                final String path = exchange.getRequestURI().getPath();
                int count;
                String destination;
                if (path.lastIndexOf("/") > 0) {
                    // path is /redirect/count/destination/, so pull out the bits
                    String[] bits = path.split("/");
                    count = Integer.parseInt(bits[2]);
                    destination = bits[3];
                } else {
                    // path is just /redirect
                    count = -1;
                    destination = "hello";
                }

                if (count == -1) {
                    // send a relative redirect, i.e. just "hello/"
                    exchange.getResponseHeaders().add("Location", destination + "/");
                } else if (count > 0) {
                    // decrement the count and send a redirect to either a full url ("http://...")
                    // or to an absolute url on this same server ("/...")
                    count--;
                    String location = "/redirect/" + count + "/" + destination + "/";
                    if (count % 2 == 0) {
                        location = url(location); // do the full url
                    }
                    exchange.getResponseHeaders().add("Location", location);
                } else {
                    // the count has hit zero, so ship them off to the destination
                    exchange.getResponseHeaders().add("Location", "/" + destination + "/");
                }
                exchange.sendResponseHeaders(302, 0);
            } catch (Exception e) {
                fail(e);
            }
        });
        server.start();
    }

    @AfterClass
    public static void stopServer() {
        server.stop(0);
    }

    private static String url(final String path) {
        String hostname = server.getAddress().getHostString();
        int port = server.getAddress().getPort();
        return "http://" + hostname + ":" + port + path;
    }

    private static String bytesToString(final byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public void testGetBytes() throws Exception {
        HttpClient client = new HttpClient();
        String u = url("/hello/");
        String response = bytesToString(client.getBytes(u));
        assertThat(response, equalTo("hello world"));
    }

    public void testGetBytes404() {
        HttpClient client = new HttpClient();
        String u = url("/404/");
        Exception e = expectThrows(ResourceNotFoundException.class, () -> client.getBytes(u));
        assertThat(e.getMessage(), equalTo(u + " not found"));
    }

    public void testRedirect() throws Exception {
        HttpClient client = new HttpClient();
        String u = url("/redirect/3/hello/");
        String response = bytesToString(client.getBytes(u));
        assertThat(response, equalTo("hello world"));
    }

    public void testRelativeRedirect() throws Exception {
        HttpClient client = new HttpClient();
        String u = url("/redirect");
        String response = bytesToString(client.getBytes(u));
        assertThat(response, equalTo("hello world"));
    }

    public void testRedirectTo404() {
        HttpClient client = new HttpClient();
        String u = url("/redirect/5/404/");
        Exception e = expectThrows(ResourceNotFoundException.class, () -> client.getBytes(u));
        assertThat(e.getMessage(), equalTo(u + " not found"));
    }

    public void testTooManyRedirects() {
        HttpClient client = new HttpClient();
        String u = url("/redirect/100/hello/");
        Exception e = expectThrows(IllegalStateException.class, () -> client.getBytes(u));
        assertThat(e.getMessage(), equalTo("too many redirects connection to [" + u + "]"));
    }
}
