/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.copyDatabase;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasLength;
import static org.hamcrest.Matchers.is;

public class MMDBUtilTests extends ESTestCase {

    // a temporary directory that mmdb files can be copied to and read from
    private Path tmpDir;

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

    public void testSmallFileWithALongDescription() throws IOException {
        Path database = tmpDir.resolve("test-description.mmdb");
        copyDatabase("test-description.mmdb", database);

        // it was once the case that we couldn't read a database_type that was 29 characters or longer
        String type = MMDBUtil.getDatabaseType(database);
        assertThat(type, endsWith("long database_type"));
        assertThat(type, hasLength(60)); // 60 is >= 29, ;)

        // it was once the case that we couldn't process an mmdb that was smaller than 512 bytes
        assertThat(Files.size(database), is(444L)); // 444 is <512
    }

    public void testDatabaseTypeParsing() throws IOException {
        // this test is a little bit overloaded -- it's testing that we're getting the expected sorts of
        // database_type strings from these files, *and* it's also testing that we dispatch on those strings
        // correctly and associated those files with the correct high-level Elasticsearch Database type.
        // down the road it would probably make sense to split these out and find a better home for some of the
        // logic, but for now it's probably more valuable to have the test *somewhere* than to get especially
        // pedantic about where precisely it should be.

        copyDatabase("GeoLite2-City-Test.mmdb", tmpDir);
        copyDatabase("GeoLite2-Country-Test.mmdb", tmpDir);
        copyDatabase("GeoLite2-ASN-Test.mmdb", tmpDir);
        copyDatabase("GeoIP2-Anonymous-IP-Test.mmdb", tmpDir);
        copyDatabase("GeoIP2-City-Test.mmdb", tmpDir);
        copyDatabase("GeoIP2-Country-Test.mmdb", tmpDir);
        copyDatabase("GeoIP2-Connection-Type-Test.mmdb", tmpDir);
        copyDatabase("GeoIP2-Domain-Test.mmdb", tmpDir);
        copyDatabase("GeoIP2-Enterprise-Test.mmdb", tmpDir);
        copyDatabase("GeoIP2-ISP-Test.mmdb", tmpDir);

        assertThat(parseDatabaseFromType("GeoLite2-City-Test.mmdb"), is(Database.City));
        assertThat(parseDatabaseFromType("GeoLite2-Country-Test.mmdb"), is(Database.Country));
        assertThat(parseDatabaseFromType("GeoLite2-ASN-Test.mmdb"), is(Database.Asn));
        assertThat(parseDatabaseFromType("GeoIP2-Anonymous-IP-Test.mmdb"), is(Database.AnonymousIp));
        assertThat(parseDatabaseFromType("GeoIP2-City-Test.mmdb"), is(Database.City));
        assertThat(parseDatabaseFromType("GeoIP2-Country-Test.mmdb"), is(Database.Country));
        assertThat(parseDatabaseFromType("GeoIP2-Connection-Type-Test.mmdb"), is(Database.ConnectionType));
        assertThat(parseDatabaseFromType("GeoIP2-Domain-Test.mmdb"), is(Database.Domain));
        assertThat(parseDatabaseFromType("GeoIP2-Enterprise-Test.mmdb"), is(Database.Enterprise));
        assertThat(parseDatabaseFromType("GeoIP2-ISP-Test.mmdb"), is(Database.Isp));
    }

    private Database parseDatabaseFromType(String databaseFile) throws IOException {
        return Database.getDatabase(MMDBUtil.getDatabaseType(tmpDir.resolve(databaseFile)), null);
    }
}
