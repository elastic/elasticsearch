/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import com.sun.net.httpserver.BasicAuthenticator;
import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.equalTo;

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
                    os.write(response.getBytes(StandardCharsets.UTF_8));
                }
            } catch (Exception e) {
                fail(e);
            }
        });
        server.createContext("/404/", exchange -> {
            try {
                exchange.sendResponseHeaders(404, 0);
                exchange.close();
            } catch (Exception e) {
                fail(e);
            }
        });
        server.createContext("/auth/", exchange -> {
            try {
                String response = "super secret hello world";
                exchange.sendResponseHeaders(200, response.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes(StandardCharsets.UTF_8));
                }
            } catch (Exception e) {
                fail(e);
            }
        }).setAuthenticator(new BasicAuthenticator("some realm") {
            @Override
            public boolean checkCredentials(String username, String password) {
                return "user".equals(username) && "pass".equals(password);
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
                exchange.close();
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

    public void testGetBytes401() {
        HttpClient client = new HttpClient();
        String u = url("/auth/");
        {
            Exception e = expectThrows(ElasticsearchStatusException.class, () -> client.getBytes(u));
            assertThat(e.getMessage(), equalTo("error during downloading " + u));
        }
        {
            PasswordAuthentication auth = client.auth("bad", "credentials");
            Exception e = expectThrows(ElasticsearchStatusException.class, () -> client.getBytes(auth, u));
            assertThat(e.getMessage(), equalTo("error during downloading " + u));
        }
    }

    public void testGetBytesWithAuth() throws Exception {
        HttpClient client = new HttpClient();
        String u = url("/auth/");
        PasswordAuthentication auth = client.auth("user", "pass");
        String response = bytesToString(client.getBytes(auth, u));
        assertThat(response, equalTo("super secret hello world"));
    }

    public void testRedirectToAuth() throws Exception {
        HttpClient client = new HttpClient();
        String u = url("/redirect/3/auth/");
        {
            Exception e = expectThrows(ElasticsearchStatusException.class, () -> client.getBytes(u));
            assertThat(e.getMessage(), equalTo("error during downloading " + u));
        }
        {
            PasswordAuthentication auth = client.auth("bad", "credentials");
            Exception e = expectThrows(ElasticsearchStatusException.class, () -> client.getBytes(auth, u));
            assertThat(e.getMessage(), equalTo("error during downloading " + u));
        }
        {
            PasswordAuthentication auth = client.auth("user", "pass");
            String response = bytesToString(client.getBytes(auth, u));
            assertThat(response, equalTo("super secret hello world"));
        }
    }
}
