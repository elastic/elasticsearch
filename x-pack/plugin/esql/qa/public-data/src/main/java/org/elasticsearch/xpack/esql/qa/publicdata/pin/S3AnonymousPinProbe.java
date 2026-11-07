/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.pin;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Pin probe for anonymously readable S3 buckets: unsigned {@code HEAD} and {@code ListObjectsV2}
 * over the bucket's virtual-hosted HTTPS endpoint. Deliberately raw HTTP — no AWS SDK, no
 * {@code repository-s3} test dependency — because listing XML is trivial to scan and the probe
 * must stay runnable from a bare {@code JavaExec}. Never issues a GET for an object body:
 * {@code list()} is a listing request, {@code head()} a metadata request.
 *
 * <p>Accepts {@code s3://bucket/key-or-prefix} URIs; the region is supplied at construction
 * (buckets live in one region and the catalog records it per variant).
 */
public class S3AnonymousPinProbe implements PinProbe {

    private final String region;
    private final HttpClient client;

    public S3AnonymousPinProbe(String region) {
        this.region = region;
        this.client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).connectTimeout(Duration.ofSeconds(20)).build();
    }

    @Override
    public ObjectMetadata head(String uri) throws IOException {
        S3Location location = S3Location.parse(uri);
        String url = "https://" + location.bucket() + ".s3." + region + ".amazonaws.com/" + location.key();
        ObjectMetadata viaHttp = PinRetry.withRetries("HEAD " + url, PinRetry.DEFAULT_MAX_ATTEMPTS, () -> httpHead(url));
        return new ObjectMetadata(location.key(), viaHttp.etag(), viaHttp.sizeBytes(), viaHttp.lastModified());
    }

    @Override
    public List<ObjectMetadata> list(String uri, int maxKeys) throws IOException {
        S3Location location = S3Location.parse(uri);
        List<ObjectMetadata> results = new ArrayList<>();
        String continuationToken = null;
        do {
            String url = "https://"
                + location.bucket()
                + ".s3."
                + region
                + ".amazonaws.com/?list-type=2&prefix="
                + URLEncoder.encode(location.key(), StandardCharsets.UTF_8)
                + "&max-keys="
                + Math.min(1000, maxKeys - results.size())
                + (continuationToken == null ? "" : "&continuation-token=" + URLEncoder.encode(continuationToken, StandardCharsets.UTF_8));
            String xml = PinRetry.withRetries("LIST " + url, PinRetry.DEFAULT_MAX_ATTEMPTS, () -> httpGet(url));
            results.addAll(parseListing(xml));
            continuationToken = tagValue(xml, "NextContinuationToken", 0);
        } while (continuationToken != null && results.size() < maxKeys);
        return results;
    }

    private ObjectMetadata httpHead(String url) throws IOException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
            .method("HEAD", HttpRequest.BodyPublishers.noBody())
            .timeout(Duration.ofSeconds(30))
            .build();
        HttpResponse<Void> response = send(request, HttpResponse.BodyHandlers.discarding());
        if (response.statusCode() != 200) {
            throw new PinRetry.HttpStatusException("HEAD " + url + " returned " + response.statusCode(), response.statusCode());
        }
        String etag = response.headers().firstValue("ETag").map(HttpsPinProbe::stripQuotes).orElse(null);
        long size = response.headers().firstValueAsLong("Content-Length").orElse(-1);
        String lastModified = response.headers().firstValue("Last-Modified").orElse(null);
        return new ObjectMetadata(url, etag, size, lastModified);
    }

    private String httpGet(String url) throws IOException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url)).GET().timeout(Duration.ofSeconds(30)).build();
        HttpResponse<String> response = send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new PinRetry.HttpStatusException("GET " + url + " returned " + response.statusCode(), response.statusCode());
        }
        return response.body();
    }

    private <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> handler) throws IOException {
        try {
            return client.send(request, handler);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during " + request.method() + " " + request.uri(), e);
        }
    }

    /**
     * Extracts {@code Contents} entries from a ListObjectsV2 response. Hand-rolled tag scanning
     * instead of a DOM/SAX parser: the response schema is fixed and flat, and this keeps the probe
     * free of XML-parser configuration concerns. Package-private for tests.
     */
    static List<ObjectMetadata> parseListing(String xml) {
        List<ObjectMetadata> results = new ArrayList<>();
        int cursor = 0;
        while (true) {
            int contentsStart = xml.indexOf("<Contents>", cursor);
            if (contentsStart < 0) {
                return results;
            }
            int contentsEnd = xml.indexOf("</Contents>", contentsStart);
            String entry = xml.substring(contentsStart, contentsEnd);
            String sizeValue = tagValue(entry, "Size", 0);
            results.add(
                new ObjectMetadata(
                    xmlUnescape(tagValue(entry, "Key", 0)),
                    HttpsPinProbe.stripQuotes(xmlUnescape(tagValue(entry, "ETag", 0))),
                    sizeValue == null ? -1 : Long.parseLong(sizeValue),
                    tagValue(entry, "LastModified", 0)
                )
            );
            cursor = contentsEnd;
        }
    }

    private static String tagValue(String xml, String tag, int from) {
        int start = xml.indexOf("<" + tag + ">", from);
        if (start < 0) {
            return null;
        }
        int end = xml.indexOf("</" + tag + ">", start);
        return xml.substring(start + tag.length() + 2, end);
    }

    private static String xmlUnescape(String value) {
        return value == null
            ? null
            : value.replace("&quot;", "\"").replace("&lt;", "<").replace("&gt;", ">").replace("&#39;", "'").replace("&amp;", "&");
    }

    /** An {@code s3://bucket/key} location. */
    record S3Location(String bucket, String key) {
        static S3Location parse(String uri) {
            if (uri.startsWith("s3://") == false) {
                throw new IllegalArgumentException("Not an s3:// URI: " + uri);
            }
            String rest = uri.substring("s3://".length());
            int slash = rest.indexOf('/');
            if (slash < 0) {
                return new S3Location(rest, "");
            }
            return new S3Location(rest.substring(0, slash), rest.substring(slash + 1));
        }
    }
}
