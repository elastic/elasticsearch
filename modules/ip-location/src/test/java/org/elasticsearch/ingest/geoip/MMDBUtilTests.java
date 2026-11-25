/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPOutputStream;

import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.resolveSharedDatabase;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasLength;
import static org.hamcrest.Matchers.is;

public class MMDBUtilTests extends ESTestCase {

    /**
     * Class-scoped read-only directory backing every {@code .mmdb} this suite reads. Hoisted out of
     * {@code @Before} so the same database is not re-copied from the test classpath on every test method
     * (and every {@code -Dtests.iters=N} rerun).
     */
    private static Path sharedDbDir;

    @BeforeClass
    public static void setupSharedDbDir() {
        sharedDbDir = createTempDir();
    }

    public void testGetDatabaseTypeGeoIP2City() throws IOException {
        Path database = resolveSharedDatabase(sharedDbDir, "GeoIP2-City-Test.mmdb");
        String type = MMDBUtil.getDatabaseType(database);
        assertThat(type, is("GeoIP2-City"));
    }

    public void testGetDatabaseTypeGeoLite2City() throws IOException {
        Path database = resolveSharedDatabase(sharedDbDir, "GeoLite2-City-Test.mmdb");
        String type = MMDBUtil.getDatabaseType(database);
        assertThat(type, is("GeoLite2-City"));
    }

    public void testSmallFileWithALongDescription() throws IOException {
        Path database = resolveSharedDatabase(sharedDbDir, "test-description.mmdb");

        // it was once the case that we couldn't read a database_type that was 29 characters or longer
        String type = MMDBUtil.getDatabaseType(database);
        assertThat(type, endsWith("long database_type"));
        assertThat(type, hasLength(60)); // 60 is >= 29, ;)

        // it was once the case that we couldn't process an mmdb that was smaller than 512 bytes
        assertThat(Files.size(database), is(444L)); // 444 is <512
    }

    public void testIsGzip() throws IOException {
        Path database = resolveSharedDatabase(sharedDbDir, "GeoLite2-City-Test.mmdb");

        // gzip output is a per-test artifact, so write into a fresh per-method dir
        Path gzipDatabase = createTempDir().resolve("GeoLite2-City.mmdb.gz");
        try (OutputStream out = new GZIPOutputStream(Files.newOutputStream(gzipDatabase))) {
            Files.copy(database, out);
        }

        assertThat(MMDBUtil.isGzip(database), is(false));
        assertThat(MMDBUtil.isGzip(gzipDatabase), is(true));
    }
}
