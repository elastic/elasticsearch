/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.root.MainResponse;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.sql.client.ClientVersion;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Test class for JDBC-ES server versions checks.
 *
 * It's using a {@code MockWebServer} to be able to create a response just like the one an ES instance
 * would create for a request to "/", where the ES version used is configurable.
 */
public class VersionParityTests extends WebServerTestCase {

    public void testExceptionThrownOnIncompatibleVersions() throws IOException, SQLException {
        String url = JdbcConfiguration.URL_PREFIX + webServerAddress();
        TransportVersion firstVersion = TransportVersionUtils.getFirstVersion();
        TransportVersion version = TransportVersions.V_7_7_0;
        do {
            version = TransportVersionUtils.getPreviousVersion(version);
            logger.info("Checking exception is thrown for version {}", version);

            prepareResponse(version);
            // Client's version is wired up to patch level, excluding the qualifier => generate the test version as the server does it.
            String versionString = version.toReleaseVersion();

            SQLException ex = expectThrows(
                SQLException.class,
                () -> new JdbcHttpClient(new JdbcConnection(JdbcConfiguration.create(url, null, 0), false))
            );
            assertEquals(
                "This version of the JDBC driver is only compatible with Elasticsearch version "
                    + ClientVersion.CURRENT.majorMinorToString()
                    + " or newer; attempting to connect to a server "
                    + "version "
                    + versionString,
                ex.getMessage()
            );
        } while (version.compareTo(firstVersion) > 0);
    }

    public void testNoExceptionThrownForCompatibleVersions() throws IOException {
        String url = JdbcConfiguration.URL_PREFIX + webServerAddress();
        TransportVersion version = TransportVersion.current();
        try {
            do {
                prepareResponse(version);
                new JdbcHttpClient(new JdbcConnection(JdbcConfiguration.create(url, null, 0), false));
                version = TransportVersionUtils.getPreviousVersion(version);
            } while (version.compareTo(TransportVersions.V_7_7_0) >= 0);
        } catch (SQLException sqle) {
            fail("JDBC driver version and Elasticsearch server version should be compatible. Error: " + sqle);
        }
    }

    void prepareResponse(TransportVersion version) throws IOException {
        MainResponse response = version == null ? createCurrentVersionMainResponse() : createMainResponse(version);
        webServer().enqueue(
            new MockResponse().setResponseCode(200)
                .addHeader("Content-Type", "application/json")
                .setBody(XContentHelper.toXContent(response, XContentType.JSON, false).utf8ToString())
        );
    }
}
