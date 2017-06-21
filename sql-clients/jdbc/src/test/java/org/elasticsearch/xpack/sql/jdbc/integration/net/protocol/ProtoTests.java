/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.integration.net.protocol;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.sql.jdbc.integration.server.JdbcHttpServer;
import org.elasticsearch.xpack.sql.jdbc.integration.util.JdbcTemplate;
import org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcConfiguration;
import org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcDriver;
import org.elasticsearch.xpack.sql.jdbc.net.client.HttpJdbcClient;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.InfoResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaColumnInfo;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;


public class ProtoTests extends ESTestCase {
    // NOCOMMIT investigate switching to ESRestTestCase and making an integration test.

    private static Client esClient;
    private static JdbcHttpServer server;
    private static HttpJdbcClient client;
    private static JdbcDriver driver;
    private static String jdbcUrl;
    private static JdbcTemplate j;

    @BeforeClass
    public static void setUpServer() throws Exception {
        if (esClient == null) {
            esClient = new PreBuiltTransportClient(Settings.EMPTY)
                    .addTransportAddress(new TransportAddress(InetAddress.getLoopbackAddress(), 9300));
        }
        if (server == null) {
            server = new JdbcHttpServer(esClient);
            server.start(0);
        }

        if (client == null) {
            jdbcUrl = server.url();
            JdbcConfiguration ci = new JdbcConfiguration(jdbcUrl, new Properties());
            client = new HttpJdbcClient(ci);
        }

        if (driver == null) {
            driver = new JdbcDriver();
        }
        
        j = new JdbcTemplate(ProtoTests::con);
    }

    @AfterClass
    public static void tearDownServer() {
        if (server != null) {
            server.stop();
            server = null;
        }

        if (client != null) {
            client.close();
            client = null;
        }

        if (driver != null) {
            driver.close();
            driver = null;
        }

        if (esClient != null) {
            esClient.close();
            esClient = null;
        }
    }

    private static Connection con() throws SQLException {
        return driver.connect(jdbcUrl, new Properties());
    }

    public void test01Ping() throws Exception {
        assertThat(client.ping((int) TimeUnit.SECONDS.toMillis(5)), equalTo(true));
    }

    public void testInfoAction() throws Exception {
        InfoResponse esInfo = client.serverInfo();
        assertThat(esInfo, notNullValue());
        assertThat(esInfo.cluster, is("elasticsearch"));
        assertThat(esInfo.node, not(isEmptyOrNullString()));
        assertThat(esInfo.versionHash, not(isEmptyOrNullString()));
        assertThat(esInfo.versionString, startsWith("5."));
        assertThat(esInfo.majorVersion, is(5));
        //assertThat(esInfo.minorVersion(), is(0));
    }

    public void testInfoTable() throws Exception {
        List<String> tables = client.metaInfoTables("emp*");
        assertThat(tables.size(), greaterThanOrEqualTo(1));
        assertThat(tables, hasItem("emp.emp"));
    }

    public void testInfoColumn() throws Exception {
        List<MetaColumnInfo> info = client.metaInfoColumns("em*", null);
        for (MetaColumnInfo i : info) {
            // NOCOMMIT test these
            logger.info(i);
        }
    }

    public void testBasicJdbc() throws Exception {
        j.consume(c -> {
            assertThat(c.isClosed(), is(false));
            assertThat(c.isReadOnly(), is(true));
        });

        j.queryToConsole("SHOW TABLES");
    }

    public void testBasicSelect() throws Exception {
        j.consume(c -> {
            assertThat(c.isClosed(), is(false));
            assertThat(c.isReadOnly(), is(true));
        });

        j.queryToConsole("SELECT * from \"emp.emp\" ");
    }

    public void testBasicDemo() throws Exception {
        j.consume(c -> {
            assertThat(c.isClosed(), is(false));
            assertThat(c.isReadOnly(), is(true));
        });

        RuntimeException e = expectThrows(RuntimeException.class, () ->
            j.queryToConsole("SELECT name, postalcode, last_score, last_score_date FROM doesnot.exist"));
        assertEquals("asdfasd", e.getMessage());
    }

    public void testMetadataGetProcedures() throws Exception {
        j.consume(c -> {
            DatabaseMetaData metaData = c.getMetaData();
            ResultSet results = metaData.getProcedures(null, null, null);
            assertThat(results, is(notNullValue()));
            assertThat(results.next(), is(false));
            assertThat(results.getMetaData().getColumnCount(), is(9));
        });
    }

    public void testMetadataGetProcedureColumns() throws Exception {
        j.consume(c -> {
            DatabaseMetaData metaData = c.getMetaData();
            ResultSet results = metaData.getProcedureColumns(null, null, null, null);
            assertThat(results, is(notNullValue()));
            assertThat(results.next(), is(false));
            assertThat(results.getMetaData().getColumnCount(), is(20));
        });
    }

    public void testMetadataGetTables() throws Exception {
        j.consume(c -> {
            DatabaseMetaData metaData = c.getMetaData();
            ResultSet results = metaData.getTables("elasticsearch", "", "%", null);
            assertThat(results, is(notNullValue()));
            assertThat(results.next(), is(true));
            assertThat(results.getMetaData().getColumnCount(), is(10));
        });
    }

    public void testMetadataColumns() throws Exception {
        RuntimeException e = expectThrows(RuntimeException.class, () -> j.consume(c -> {
            DatabaseMetaData metaData = c.getMetaData();
            ResultSet results = metaData.getColumns("elasticsearch", "", "dep.dep", "%");
            assertThat(results, is(notNullValue()));
            assertThat(results.next(), is(true));
            assertThat(results.getMetaData().getColumnCount(), is(24));
        }));
        assertEquals("adsf", e.getMessage());
    }
}