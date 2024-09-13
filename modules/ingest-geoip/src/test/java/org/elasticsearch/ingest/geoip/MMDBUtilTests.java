/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;

import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.copyDatabase;
import static org.hamcrest.Matchers.is;

public class MMDBUtilTests extends ESTestCase {

    // a temporary directory that mmdb files can be copied to and read from
    Path tmpDir;

    @Before
    public void setup() {
        tmpDir = createTempDir();
    }

    @After
    public void cleanup() throws IOException {
        IOUtils.rm(tmpDir);
    }

    public void testGetDatabaseTypeGeoIP2City() throws IOException {
        Path database = tmpDir.resolve("GeoIP2-City.mmdb");
        copyDatabase("GeoIP2-City-Test.mmdb", database);

        String type = MMDBUtil.getDatabaseType(database);
        assertThat(type, is("GeoIP2-City"));
    }

    public void testGetDatabaseTypeGeoLite2City() throws IOException {
        Path database = tmpDir.resolve("GeoLite2-City.mmdb");
        copyDatabase("GeoLite2-City-Test.mmdb", database);

        String type = MMDBUtil.getDatabaseType(database);
        assertThat(type, is("GeoLite2-City"));
    }
}
