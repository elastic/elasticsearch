/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import fixture.s3.S3ConsistencyModel;
import fixture.s3.S3HttpFixture;

import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static fixture.aws.AwsCredentialsUtils.checkAuthorization;
import static fixture.aws.AwsCredentialsUtils.fixedAccessKey;
import static org.hamcrest.Matchers.containsString;

/**
 * Listing pages as they arrive on the wire. A real {@link S3StorageProvider} lists against a fixture that answers
 * ListObjectsV2 from scripted XML, so the page shapes go through the SDK's XML unmarshaller rather than through
 * hand-built response objects.
 */
public class S3ListingWireFormatTests extends ESTestCase {

    private static final String ACCESS_KEY = "test-access-key";
    private static final String SECRET_KEY = "test-secret-key";
    private static final String BUCKET = "paged";

    private static final Map<String, String> PAGES = new ConcurrentHashMap<>();
    private static final String FIRST = "first";

    @ClassRule
    public static S3HttpFixture s3Fixture = new ScriptedListingFixture();

    private static S3StorageProvider provider;

    @BeforeClass
    public static void setupProvider() {
        S3Configuration config = S3Configuration.fromFields(ACCESS_KEY, SECRET_KEY, s3Fixture.getAddress(), "us-east-1");
        provider = new S3StorageProvider(config);
    }

    @AfterClass
    public static void cleanupProvider() throws Exception {
        if (provider != null) {
            provider.close();
            provider = null;
        }
        PAGES.clear();
    }

    public void testEmptyTruncatedPageOnTheWireDoesNotEndListing() throws IOException {
        PAGES.clear();
        PAGES.put(FIRST, page(List.of("data/a.csv"), "true", "token-1"));
        PAGES.put("token-1", page(List.of(), "true", "token-2"));
        PAGES.put("token-2", page(List.of("data/b.csv"), "false", null));

        assertEquals(List.of("a.csv", "b.csv"), listNames());
    }

    public void testPageWithoutIsTruncatedElementListsItsKeys() throws IOException {
        PAGES.clear();
        PAGES.put(FIRST, page(List.of("data/a.csv"), null, null));

        assertEquals(List.of("a.csv"), listNames());
    }

    public void testTruncatedPageWithoutContinuationTokenOnTheWireFails() {
        PAGES.clear();
        PAGES.put(FIRST, page(List.of("data/a.csv"), "true", null));

        UncheckedIOException thrown = expectThrows(UncheckedIOException.class, S3ListingWireFormatTests::listNames);

        assertThat(thrown.getMessage(), containsString("without a continuation token"));
    }

    private static List<String> listNames() throws IOException {
        List<String> names = new ArrayList<>();
        try (StorageIterator iterator = provider.listObjects(StoragePath.of("s3://" + BUCKET + "/data"), true)) {
            while (iterator.hasNext()) {
                names.add(iterator.next().path().objectName());
            }
        }
        return names;
    }

    private static String page(List<String> keys, String truncated, String nextToken) {
        StringBuilder xml = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        xml.append("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
        xml.append("<Name>").append(BUCKET).append("</Name><Prefix>data/</Prefix>");
        xml.append("<KeyCount>").append(keys.size()).append("</KeyCount><MaxKeys>1000</MaxKeys>");
        if (truncated != null) {
            xml.append("<IsTruncated>").append(truncated).append("</IsTruncated>");
        }
        for (String key : keys) {
            xml.append("<Contents><Key>").append(key).append("</Key>");
            xml.append("<LastModified>2026-01-01T00:00:00.000Z</LastModified><Size>1</Size>");
            xml.append("<StorageClass>STANDARD</StorageClass></Contents>");
        }
        if (nextToken != null) {
            xml.append("<NextContinuationToken>").append(nextToken).append("</NextContinuationToken>");
        }
        return xml.append("</ListBucketResult>").toString();
    }

    @SuppressForbidden(reason = "overrides S3HttpFixture.createHandler which returns com.sun.net.httpserver.HttpHandler")
    private static final class ScriptedListingFixture extends S3HttpFixture {
        ScriptedListingFixture() {
            super(true, () -> S3ConsistencyModel.STRONG_MPUS);
        }

        @Override
        protected HttpHandler createHandler() {
            var auth = fixedAccessKey(ACCESS_KEY, () -> "us-east-1", "s3");
            return exchange -> {
                try (exchange) {
                    if (checkAuthorization(auth, exchange) == false) {
                        return;
                    }
                    String query = exchange.getRequestURI().getRawQuery();
                    String token = FIRST;
                    if (query != null) {
                        for (String param : query.split("&")) {
                            if (param.startsWith("continuation-token=")) {
                                token = java.net.URLDecoder.decode(param.substring("continuation-token=".length()), StandardCharsets.UTF_8);
                            }
                        }
                    }
                    String body = PAGES.get(token);
                    if (body == null) {
                        exchange.sendResponseHeaders(404, -1);
                        return;
                    }
                    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/xml");
                    exchange.sendResponseHeaders(200, bytes.length);
                    exchange.getResponseBody().write(bytes);
                }
            };
        }
    }
}
