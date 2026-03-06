/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.parquet;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasource.s3.S3Configuration;
import org.elasticsearch.xpack.esql.datasource.s3.S3StorageProvider;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.DataSourcesS3HttpFixture;
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

import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.ACCESS_KEY;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.BUCKET;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.SECRET_KEY;

/**
 * S3 discovery tests using S3HttpFixture with empty blobs.
 * Validates that S3StorageProvider.listObjects() returns correct entries
 * and that glob-style filtering works against S3 listings.
 */
public class S3GlobDiscoveryIT extends ESTestCase {

    @ClassRule
    public static DataSourcesS3HttpFixture s3Fixture = new DataSourcesS3HttpFixture();

    private static S3StorageProvider provider;

    private static final String DISCOVER_PREFIX = "warehouse/discover";

    @BeforeClass
    public static void setupProvider() {
        // Upload empty blobs for discovery
        S3FixtureUtils.addBlobToFixture(s3Fixture.getHandler(), DISCOVER_PREFIX + "/flat/a.parquet", new byte[0]);
        S3FixtureUtils.addBlobToFixture(s3Fixture.getHandler(), DISCOVER_PREFIX + "/flat/b.parquet", new byte[0]);
        S3FixtureUtils.addBlobToFixture(s3Fixture.getHandler(), DISCOVER_PREFIX + "/flat/c.csv", new byte[0]);
        S3FixtureUtils.addBlobToFixture(s3Fixture.getHandler(), DISCOVER_PREFIX + "/nested/x/d.parquet", new byte[0]);
        S3FixtureUtils.addBlobToFixture(s3Fixture.getHandler(), DISCOVER_PREFIX + "/nested/y/e.parquet", new byte[0]);

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
        assertEquals(List.of("a.parquet", "b.parquet", "c.csv"), names);
    }

    public void testS3FlatGlobFiltering() throws IOException {
        StoragePath prefix = StoragePath.of("s3://" + BUCKET + "/" + DISCOVER_PREFIX + "/flat");
        List<StorageEntry> entries = collectAll(provider.listObjects(prefix, false));

        // Simulate *.parquet glob filtering
        Pattern parquetPattern = Pattern.compile("[^/]*\\.parquet");
        List<StorageEntry> matched = new ArrayList<>();
        for (StorageEntry e : entries) {
            if (parquetPattern.matcher(e.path().objectName()).matches()) {
                matched.add(e);
            }
        }

        assertEquals(2, matched.size());
    }

    public void testS3RecursiveGlobFiltering() throws IOException {
        // S3 is flat — listing with a prefix returns all objects under it
        StoragePath prefix = StoragePath.of("s3://" + BUCKET + "/" + DISCOVER_PREFIX);
        List<StorageEntry> entries = collectAll(provider.listObjects(prefix, true));

        // Simulate **/*.parquet: match any .parquet file at any depth
        String prefixStr = "s3://" + BUCKET + "/" + DISCOVER_PREFIX + "/";
        List<StorageEntry> matched = new ArrayList<>();
        for (StorageEntry e : entries) {
            String fullPath = e.path().toString();
            String relativePath = fullPath.startsWith(prefixStr) ? fullPath.substring(prefixStr.length()) : e.path().objectName();
            if (relativePath.endsWith(".parquet")) {
                matched.add(e);
            }
        }

        assertEquals(4, matched.size());
    }

    public void testS3NoMatchReturnsEmpty() throws IOException {
        StoragePath prefix = StoragePath.of("s3://" + BUCKET + "/" + DISCOVER_PREFIX + "/flat");
        List<StorageEntry> entries = collectAll(provider.listObjects(prefix, false));

        // Simulate *.json glob filtering — no matches expected
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

        // Simulate *.{parquet,csv} glob filtering
        Pattern bracePattern = Pattern.compile("[^/]*\\.(?:parquet|csv)");
        List<StorageEntry> matched = new ArrayList<>();
        for (StorageEntry e : entries) {
            if (bracePattern.matcher(e.path().objectName()).matches()) {
                matched.add(e);
            }
        }

        assertEquals(3, matched.size());
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
}
