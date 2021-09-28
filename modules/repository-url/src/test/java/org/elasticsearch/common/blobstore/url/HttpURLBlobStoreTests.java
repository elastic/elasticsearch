/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore.url;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.url.http.URLHttpClient;
import org.elasticsearch.common.blobstore.url.http.URLHttpClientSettings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.rest.RestStatus;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressForbidden(reason = "use http server")
public class HttpURLBlobStoreTests extends AbstractURLBlobStoreTests {
    private static final Pattern RANGE_PATTERN = Pattern.compile("bytes=(\\d+)-(\\d+)$");
    private static HttpServer httpServer;
    private static String blobName;
    private static byte[] content;
    private static URLHttpClient httpClient;
    private static URLHttpClient.Factory httpClientFactory;

    private URLBlobStore urlBlobStore;

    @BeforeClass
    public static void startHttp() throws Exception {
        content = randomByteArrayOfLength(randomIntBetween(512, 2048));
        blobName = randomAlphaOfLength(8);

        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress().getHostAddress(), 0), 0);

        httpServer.createContext("/indices/" + blobName, exchange -> {
            try {
                Streams.readFully(exchange.getRequestBody());

                Headers requestHeaders = exchange.getRequestHeaders();
                final String range = requestHeaders.getFirst("Range");
                if (range == null) {
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), content.length);
                    OutputStream responseBody = exchange.getResponseBody();
                    responseBody.write(content);
                    return;
                }

                final Matcher rangeMatcher = RANGE_PATTERN.matcher(range);
                if (rangeMatcher.matches() == false) {
                    exchange.sendResponseHeaders(RestStatus.REQUESTED_RANGE_NOT_SATISFIED.getStatus(), -1);
                    return;
                }

                int lowerBound = Integer.parseInt(rangeMatcher.group(1));
                int upperBound = Math.min(Integer.parseInt(rangeMatcher.group(2)), content.length - 1);
                int rangeLength = upperBound - lowerBound + 1;
                if (lowerBound >= content.length || lowerBound > upperBound || rangeLength > content.length) {
                    exchange.sendResponseHeaders(RestStatus.REQUESTED_RANGE_NOT_SATISFIED.getStatus(), -1);
                    return;
                }

                exchange.getResponseHeaders().add("Content-Range", "bytes " + lowerBound + "-" + upperBound + "/" + content.length);
                exchange.sendResponseHeaders(RestStatus.PARTIAL_CONTENT.getStatus(), rangeLength);
                OutputStream responseBody = exchange.getResponseBody();
                responseBody.write(content, lowerBound, rangeLength);
            } finally {
                exchange.close();
            }
        });

        httpServer.start();

        httpClientFactory = new URLHttpClient.Factory();
        httpClient = httpClientFactory.create(URLHttpClientSettings.fromSettings(Settings.EMPTY));
    }

    @AfterClass
    public static void stopHttp() throws IOException {
        httpServer.stop(0);
        httpServer = null;
        httpClient.close();
        httpClientFactory.close();
    }

    @Before
    public void storeSetup() throws MalformedURLException {
        final URLHttpClientSettings httpClientSettings = URLHttpClientSettings.fromSettings(Settings.EMPTY);
        urlBlobStore = new URLBlobStore(Settings.EMPTY, new URL(getEndpointForServer()), httpClient, httpClientSettings);
    }

    @Override
    BytesArray getOriginalData() {
        return new BytesArray(content);
    }

    @Override
    BlobContainer getBlobContainer() {
        return urlBlobStore.blobContainer(BlobPath.EMPTY.add("indices"));
    }

    @Override
    String getBlobName() {
        return blobName;
    }

    public void testRangeReadOutsideOfLegalRange() {
        BlobContainer container = getBlobContainer();
        expectThrows(IllegalArgumentException.class, () -> container.readBlob(blobName, -1, content.length).read());
        expectThrows(IOException.class, () -> container.readBlob(blobName, content.length + 1, content.length).read());
    }

    private String getEndpointForServer() {
        InetSocketAddress address = httpServer.getAddress();
        return "http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort() + "/";
    }
}
