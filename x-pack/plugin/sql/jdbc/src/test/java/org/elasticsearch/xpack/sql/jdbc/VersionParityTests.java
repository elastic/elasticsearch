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
        Version version = VersionUtils.randomVersionBetween(random(), null, VersionUtils.getPreviousVersion());
        logger.info("Checking exception is thrown for version {}", version);
        prepareResponse(version);
        
        String url = JdbcConfiguration.URL_PREFIX + webServerAddress();
        SQLException ex = expectThrows(SQLException.class, () -> new JdbcHttpClient(JdbcConfiguration.create(url, null, 0)));
        assertEquals("This version of the JDBC driver is only compatible with Elasticsearch version "
                + org.elasticsearch.xpack.sql.client.Version.CURRENT.toString()
                + ", attempting to connect to a server version " + version.toString(), ex.getMessage());
    }
    
    public void testNoExceptionThrownForCompatibleVersions() throws IOException {
        prepareResponse(null);
        
        String url = JdbcConfiguration.URL_PREFIX + webServerAddress();
        try {
            new JdbcHttpClient(JdbcConfiguration.create(url, null, 0));
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
