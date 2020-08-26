/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.Version;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.xpack.sql.client.ClientVersion;
import org.elasticsearch.xpack.sql.proto.SqlVersion;

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
        Version firstVersion = VersionUtils.getFirstVersion();
        Version version = Version.V_7_7_0;
        do {
            version = VersionUtils.getPreviousVersion(version);
            logger.info("Checking exception is thrown for version {}", version);

            prepareResponse(version);
            // Client's version is wired up to patch level, excluding the qualifier => generate the test version as the server does it.
            String versionString = SqlVersion.fromString(version.toString()).toString();

            SQLException ex = expectThrows(SQLException.class, () -> new JdbcHttpClient(JdbcConfiguration.create(url, null, 0)));
            assertEquals("This version of the JDBC driver is only compatible with Elasticsearch version " +
                ClientVersion.CURRENT.majorMinorToString() + " or newer; attempting to connect to a server " +
                "version " + versionString, ex.getMessage());
        } while (version.compareTo(firstVersion) > 0);
    }
    
    public void testNoExceptionThrownForCompatibleVersions() throws IOException {
        String url = JdbcConfiguration.URL_PREFIX + webServerAddress();
        Version version = Version.CURRENT;
        try {
            do {
                prepareResponse(version);
                new JdbcHttpClient(JdbcConfiguration.create(url, null, 0));
                version = VersionUtils.getPreviousVersion(version);
            } while (version.compareTo(Version.V_7_7_0) >= 0);
        } catch (SQLException sqle) {
            fail("JDBC driver version and Elasticsearch server version should be compatible. Error: " + sqle);
        }
    }
    
    void prepareResponse(Version version) throws IOException {
        MainResponse response = version == null ? createCurrentVersionMainResponse() : createMainResponse(version);        
        webServer().enqueue(new MockResponse().setResponseCode(200).addHeader("Content-Type", "application/json").setBody(
                XContentHelper.toXContent(response, XContentType.JSON, false).utf8ToString()));
    }
}
