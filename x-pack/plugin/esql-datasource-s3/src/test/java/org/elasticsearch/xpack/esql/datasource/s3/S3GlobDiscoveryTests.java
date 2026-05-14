/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import fixture.s3.S3ConsistencyModel;
import fixture.s3.S3HttpFixture;
import fixture.s3.S3HttpHandler;

import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static fixture.aws.AwsCredentialsUtils.checkAuthorization;
import static fixture.aws.AwsCredentialsUtils.fixedAccessKey;

/**
 * Tests S3StorageProvider.listObjects() and glob-style filtering against an in-process
 * S3HttpFixture populated with empty blobs. Lives in the s3 datasource module so the
 * netty-commons classes referenced by S3StorageProvider are present on the test classpath
 * (via the compileOnly dependency in this module's build).
 */
public class S3GlobDiscoveryTests extends ESTestCase {

    private static final String ACCESS_KEY = "test-access-key";
    private static final String SECRET_KEY = "test-secret-key";
    private static final String BUCKET = "test-bucket";
    private static final String DISCOVER_PREFIX = "warehouse/discover";

    @ClassRule
    public static HandlerExposingS3HttpFixture s3Fixture = new HandlerExposingS3HttpFixture();

    private static S3StorageProvider provider;

    @BeforeClass
    public static void setupProvider() {
        // Upload empty blobs for discovery. The .bin/.txt extensions are arbitrary string suffixes
        // exercised by the regex-based glob filtering below; this test does not care about format.
        addBlob(DISCOVER_PREFIX + "/flat/a.bin", new byte[0]);
        addBlob(DISCOVER_PREFIX + "/flat/b.bin", new byte[0]);
        addBlob(DISCOVER_PREFIX + "/flat/c.txt", new byte[0]);
        addBlob(DISCOVER_PREFIX + "/nested/x/d.bin", new byte[0]);
        addBlob(DISCOVER_PREFIX + "/nested/y/e.bin", new byte[0]);

        S3Configuration config = S3Configuration.fromFields(ACCESS_KEY, SECRET_KEY, s3Fixture.getAddress(), "us-east-1");
        provider = new S3StorageProvider(config);
    }

    @AfterClass
    public static void cleanupProvider() throws Exception {
        if (provider != null) {
            provider.close();
            provider = null;
        }
    }

    public void testS3FlatListing() throws IOException {
        StoragePath prefix = StoragePath.of("s3://" + BUCKET + "/" + DISCOVER_PREFIX + "/flat");
        List<StorageEntry> entries = collectAll(provider.listObjects(prefix, false));

        List<String> names = entries.stream().map(e -> e.path().objectName()).sorted().toList();
        assertEquals(List.of("a.bin", "b.bin", "c.txt"), names);
    }

    public void testS3FlatGlobFiltering() throws IOException {
        StoragePath prefix = StoragePath.of("s3://" + BUCKET + "/" + DISCOVER_PREFIX + "/flat");
        List<StorageEntry> entries = collectAll(provider.listObjects(prefix, false));

        Pattern binPattern = Pattern.compile("[^/]*\\.bin");
        List<StorageEntry> matched = new ArrayList<>();
        for (StorageEntry e : entries) {
            if (binPattern.matcher(e.path().objectName()).matches()) {
                matched.add(e);
            }
        }

        assertEquals(2, matched.size());
    }

    public void testS3RecursiveGlobFiltering() throws IOException {
        // S3 is flat - listing with a prefix returns all objects under it.
        StoragePath prefix = StoragePath.of("s3://" + BUCKET + "/" + DISCOVER_PREFIX);
        List<StorageEntry> entries = collectAll(provider.listObjects(prefix, true));

        String prefixStr = "s3://" + BUCKET + "/" + DISCOVER_PREFIX + "/";
        List<StorageEntry> matched = new ArrayList<>();
        for (StorageEntry e : entries) {
            String fullPath = e.path().toString();
            String relativePath = fullPath.startsWith(prefixStr) ? fullPath.substring(prefixStr.length()) : e.path().objectName();
            if (relativePath.endsWith(".bin")) {
                matched.add(e);
            }
        }

        assertEquals(4, matched.size());
    }

    public void testS3NoMatchReturnsEmpty() throws IOException {
        StoragePath prefix = StoragePath.of("s3://" + BUCKET + "/" + DISCOVER_PREFIX + "/flat");
        List<StorageEntry> entries = collectAll(provider.listObjects(prefix, false));

        Pattern jsonPattern = Pattern.compile("[^/]*\\.json");
        List<StorageEntry> matched = new ArrayList<>();
        for (StorageEntry e : entries) {
            if (jsonPattern.matcher(e.path().objectName()).matches()) {
                matched.add(e);
            }
        }

        assertEquals(0, matched.size());
    }

    public void testS3BraceAlternativesFiltering() throws IOException {
        StoragePath prefix = StoragePath.of("s3://" + BUCKET + "/" + DISCOVER_PREFIX + "/flat");
        List<StorageEntry> entries = collectAll(provider.listObjects(prefix, false));

        Pattern bracePattern = Pattern.compile("[^/]*\\.(?:bin|txt)");
        List<StorageEntry> matched = new ArrayList<>();
        for (StorageEntry e : entries) {
            if (bracePattern.matcher(e.path().objectName()).matches()) {
                matched.add(e);
            }
        }

        assertEquals(3, matched.size());
    }

    private static void addBlob(String key, byte[] content) {
        s3Fixture.handler().blobs().put("/" + BUCKET + "/" + key, new BytesArray(content));
    }

    private static List<StorageEntry> collectAll(StorageIterator iterator) throws IOException {
        List<StorageEntry> entries = new ArrayList<>();
        try (iterator) {
            while (iterator.hasNext()) {
                entries.add(iterator.next());
            }
        }
        return entries;
    }

    /**
     * Local S3HttpFixture variant that captures the S3HttpHandler so tests can inject blobs directly.
     * Mirrors the DataSourcesS3HttpFixture from x-pack/plugin/esql/qa:server (kept inline here to avoid
     * dragging the heavyweight qa:server dependencies into this module's test classpath).
     */
    @SuppressForbidden(reason = "overrides S3HttpFixture.createHandler which returns com.sun.net.httpserver.HttpHandler")
    public static final class HandlerExposingS3HttpFixture extends S3HttpFixture {

        private S3HttpHandler handler;

        public HandlerExposingS3HttpFixture() {
            super(true, () -> S3ConsistencyModel.STRONG_MPUS);
        }

        @Override
        protected HttpHandler createHandler() {
            handler = new S3HttpHandler(BUCKET, null, S3ConsistencyModel.STRONG_MPUS);
            var auth = fixedAccessKey(ACCESS_KEY, () -> "us-east-1", "s3");
            return exchange -> {
                if (checkAuthorization(auth, exchange)) {
                    handler.handle(exchange);
                }
            };
        }

        S3HttpHandler handler() {
            return handler;
        }
    }
}
