/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-process S3: path-style, region from the SigV4 credential scope, HEAD-bucket always 200
 * with {@code x-amz-bucket-region} (the global-endpoint 200 that races the SDK decorator).
 */
@SuppressForbidden(reason = "an in-process HTTP server is the only way to return region-scoped S3 responses to the SDK")
final class RegionalS3Fake implements AutoCloseable {

    private final HttpServer server;
    private final Map<String, String> homeRegion = new ConcurrentHashMap<>();
    private final Map<String, byte[]> objects = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> headBucketCounts = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> wrongRegionGets = new ConcurrentHashMap<>();
    private final AtomicInteger totalGets = new AtomicInteger();
    private final AtomicInteger headFailuresRemaining = new AtomicInteger();
    private volatile boolean sendRegionHeader = true;
    private volatile boolean rejectWrongScope = true;
    private volatile CountDownLatch headBucketEntered;
    private volatile CountDownLatch headBucketRelease;

    RegionalS3Fake() throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", this::handle);
        server.start();
    }

    URI endpoint() {
        InetSocketAddress address = server.getAddress();
        return URI.create("http://" + address.getAddress().getHostAddress() + ":" + address.getPort());
    }

    void putBucket(String bucket, String region) {
        homeRegion.put(bucket, region);
    }

    void putObject(String bucket, String key, byte[] body) {
        objects.put(bucket + "/" + key, body);
    }

    void setHome(String bucket, String region) {
        homeRegion.put(bucket, region);
    }

    void sendRegionHeader(boolean send) {
        sendRegionHeader = send;
    }

    void rejectWrongScope(boolean reject) {
        rejectWrongScope = reject;
    }

    void failNextHeadBuckets(int count) {
        headFailuresRemaining.set(count);
    }

    /** Park the next HeadBucket until {@code release} counts down; count {@code entered} when the probe arrives. */
    void holdHeadBucket(CountDownLatch entered, CountDownLatch release) {
        headBucketEntered = entered;
        headBucketRelease = release;
    }

    int headBucketCount(String bucket) {
        AtomicInteger count = headBucketCounts.get(bucket);
        return count == null ? 0 : count.get();
    }

    int wrongRegionGets(String bucket) {
        AtomicInteger count = wrongRegionGets.get(bucket);
        return count == null ? 0 : count.get();
    }

    int totalGets() {
        return totalGets.get();
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getPath();
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        int slash = path.indexOf('/');
        String bucket = slash < 0 ? path : path.substring(0, slash);
        String key = slash < 0 ? "" : path.substring(slash + 1);
        String home = homeRegion.getOrDefault(bucket, "us-east-1");
        String signedRegion = regionFromAuthorization(exchange.getRequestHeaders().getFirst("Authorization"));

        if ("HEAD".equals(method) && key.isEmpty()) {
            handleHeadBucket(exchange, bucket, home);
            return;
        }
        boolean wrongRegion = rejectWrongScope && signedRegion != null && signedRegion.equals(home) == false;
        if ("GET".equals(method) && key.isEmpty() == false) {
            totalGets.incrementAndGet();
            if (wrongRegion) {
                wrongRegionGets.computeIfAbsent(bucket, b -> new AtomicInteger()).incrementAndGet();
                sendRedirect(exchange, home);
                return;
            }
            byte[] body = objects.get(bucket + "/" + key);
            if (body == null) {
                sendXml(exchange, 404, "NoSuchKey", "The specified key does not exist.", home);
                return;
            }
            String range = exchange.getRequestHeaders().getFirst("Range");
            if (range != null && range.startsWith("bytes=")) {
                sendRange(exchange, body, range.substring("bytes=".length()));
                return;
            }
            exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
            return;
        }
        if ("GET".equals(method) && key.isEmpty()) {
            if (wrongRegion) {
                sendRedirect(exchange, home);
                return;
            }
            sendXml(exchange, 200, null, null, home);
            return;
        }
        if ("HEAD".equals(method)) {
            if (wrongRegion) {
                sendRedirect(exchange, home);
                return;
            }
            byte[] body = objects.get(bucket + "/" + key);
            exchange.sendResponseHeaders(body == null ? 404 : 200, -1);
            exchange.close();
            return;
        }
        exchange.sendResponseHeaders(405, -1);
        exchange.close();
    }

    private void handleHeadBucket(HttpExchange exchange, String bucket, String home) throws IOException {
        headBucketCounts.computeIfAbsent(bucket, b -> new AtomicInteger()).incrementAndGet();
        CountDownLatch entered = headBucketEntered;
        CountDownLatch release = headBucketRelease;
        if (entered != null) {
            entered.countDown();
        }
        if (release != null) {
            try {
                release.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (headFailuresRemaining.getAndUpdate(v -> v > 0 ? v - 1 : 0) > 0) {
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
            return;
        }
        if (sendRegionHeader) {
            exchange.getResponseHeaders().add("x-amz-bucket-region", home);
        }
        exchange.sendResponseHeaders(200, -1);
        exchange.close();
    }

    private void sendRedirect(HttpExchange exchange, String home) throws IOException {
        if (sendRegionHeader) {
            exchange.getResponseHeaders().add("x-amz-bucket-region", home);
        }
        sendXml(
            exchange,
            301,
            "PermanentRedirect",
            "The bucket you are attempting to access must be addressed using the specified endpoint.",
            home
        );
    }

    private void sendRange(HttpExchange exchange, byte[] body, String spec) throws IOException {
        int dash = spec.indexOf('-');
        int start = Integer.parseInt(spec.substring(0, dash));
        int end = spec.length() == dash + 1 || spec.charAt(dash + 1) == '\0' ? body.length - 1 : Integer.parseInt(spec.substring(dash + 1));
        end = Math.min(end, body.length - 1);
        int length = end - start + 1;
        byte[] slice = new byte[length];
        System.arraycopy(body, start, slice, 0, length);
        exchange.getResponseHeaders().add("Content-Range", "bytes " + start + "-" + end + "/" + body.length);
        exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
        exchange.sendResponseHeaders(206, slice.length);
        exchange.getResponseBody().write(slice);
        exchange.close();
    }

    private void sendXml(HttpExchange exchange, int status, String code, String message, String home) throws IOException {
        String xml;
        if (code == null) {
            xml = "<?xml version=\"1.0\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                + "<IsTruncated>false</IsTruncated></ListBucketResult>";
        } else {
            xml = "<?xml version=\"1.0\"?><Error><Code>"
                + code
                + "</Code><Message>"
                + message
                + "</Message><Region>"
                + home
                + "</Region></Error>";
        }
        byte[] bytes = xml.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/xml");
        if (sendRegionHeader && status != 200) {
            exchange.getResponseHeaders().add("x-amz-bucket-region", home);
        }
        exchange.sendResponseHeaders(status, bytes.length);
        if ("HEAD".equals(exchange.getRequestMethod()) == false) {
            exchange.getResponseBody().write(bytes);
        }
        exchange.close();
    }

    static String regionFromAuthorization(String authorization) {
        if (authorization == null) {
            return null;
        }
        int cred = authorization.indexOf("Credential=");
        if (cred < 0) {
            return null;
        }
        int start = cred + "Credential=".length();
        int comma = authorization.indexOf(',', start);
        int end = comma < 0 ? authorization.length() : comma;
        String credential = authorization.substring(start, end);
        // AKID/date/region/s3/aws4_request
        int first = credential.indexOf('/');
        int second = first < 0 ? -1 : credential.indexOf('/', first + 1);
        int third = second < 0 ? -1 : credential.indexOf('/', second + 1);
        if (second < 0 || third < 0) {
            return null;
        }
        return credential.substring(second + 1, third);
    }
}
