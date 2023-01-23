/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import static org.hamcrest.Matchers.equalTo;

public class DatabaseReaderLazyLoaderTests extends ESTestCase {

    public void testClosure() throws Exception {
        {
            Path db = getNewDB();
            DatabaseReaderLazyLoader dbReader = new DatabaseReaderLazyLoader(new GeoIpCache(1000), db, null);
            // Called before provided to processor
            assertTrue(dbReader.preLookup());
            assertThat(dbReader.current(), equalTo(1));
            // Called after lookup is completed
            dbReader.postLookup();
            assertThat(dbReader.current(), equalTo(0));
            dbReader.close(true);
            assertFalse(Files.exists(db));
        }
        {
            Path db = getNewDB();
            DatabaseReaderLazyLoader dbReader = new DatabaseReaderLazyLoader(new GeoIpCache(1000), db, null);
            // Called before provided to processor
            assertTrue(dbReader.preLookup());
            assertThat(dbReader.current(), equalTo(1));
            // post look up is called multiple times when looking up multiple documents
            dbReader.postLookup();
            assertThat(dbReader.current(), equalTo(0));
            // If an even number of invocations ocurrs then the current value is set above zero
            dbReader.postLookup();
            assertThat(dbReader.current(), equalTo(1));
            // Ensure that close removes the file correctly
            dbReader.close(true);
            assertFalse(Files.exists(db));
        }
    }

    private Path getNewDB() throws IOException, URISyntaxException {
        // Provision a temp dir to hold the database file so that we can verify it is cleaned up
        URL sourceDbURL = DatabaseReaderLazyLoaderTests.class.getResource("/GeoIP2-City-Test.mmdb");
        assertNotNull("Missing test database", sourceDbURL);
        Path sourceDbPath = PathUtils.get(sourceDbURL.toURI());
        Path tempDir = createTempDir();
        Path tempDbPath = tempDir.resolve("GeoIP2-City-Test.mmdb");
        return Files.copy(sourceDbPath, tempDbPath, StandardCopyOption.REPLACE_EXISTING);
    }
}
