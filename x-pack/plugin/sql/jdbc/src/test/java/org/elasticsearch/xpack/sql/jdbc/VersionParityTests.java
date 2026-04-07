/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.root.MainResponse;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.sql.client.ClientVersion;
import org.elasticsearch.xpack.sql.proto.SqlVersion;
import org.elasticsearch.xpack.sql.proto.SqlVersions;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.sql.proto.VersionCompatibility.INTRODUCING_VERSION_COMPATIBILITY;

/**
 * Test class for JDBC-ES server versions checks.
 *
 * It's using a {@code MockWebServer} to be able to create a response just like the one an ES instance
 * would create for a request to "/", where the ES version used is configurable.
 */
public class VersionParityTests extends WebServerTestCase {

    public void testExceptionThrownOnIncompatibleVersions() throws IOException, SQLException {
        String url = JdbcConfiguration.URL_PREFIX + webServerAddress();
        for (var version = SqlVersions.getFirstVersion(); version.onOrAfter(INTRODUCING_VERSION_COMPATIBILITY) == false; version =
            SqlVersions.getNextVersion(version)) {
            logger.info("Checking exception is thrown for version {}", version);

            prepareResponse(version);

            SQLException ex = expectThrows(
                SQLException.class,
                () -> new JdbcHttpClient(new JdbcConnection(JdbcConfiguration.create(url, null, 0), false))
            );
            assertEquals(
                "This version of the JDBC driver is only compatible with Elasticsearch version "
                    + ClientVersion.CURRENT.majorMinorToString()
                    + " or newer; attempting to connect to a server "
                    + "version "
                    + version,
                ex.getMessage()
            );
        }
    }

    public void testNoExceptionThrownForCompatibleVersions() throws IOException {
        String url = JdbcConfiguration.URL_PREFIX + webServerAddress();
        List<SqlVersion> afterVersionCompatibility = SqlVersions.getAllVersions()
            .stream()
            .filter(v -> v.onOrAfter(INTRODUCING_VERSION_COMPATIBILITY))
            .collect(Collectors.toCollection(ArrayList::new));
        afterVersionCompatibility.add(VersionTests.current());
        for (var version : afterVersionCompatibility) {
            try {
                prepareResponse(version);
                new JdbcHttpClient(new JdbcConnection(JdbcConfiguration.create(url, null, 0), false));
            } catch (SQLException sqle) {
                fail("JDBC driver version and Elasticsearch server version should be compatible. Error: " + sqle);
            }
        }
    }

    void prepareResponse(SqlVersion version) throws IOException {
        MainResponse response = version == null ? createCurrentVersionMainResponse() : createMainResponse(version);
        webServer().enqueue(
            new MockResponse().setResponseCode(200)
                .addHeader("Content-Type", "application/json")
                .setBody(XContentHelper.toXContent(response, XContentType.JSON, false).utf8ToString())
        );
    }
}
