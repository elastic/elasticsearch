/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.integration.net.protocol;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.sql.cli.CliConfiguration;
import org.elasticsearch.xpack.sql.cli.integration.server.CliHttpServer;
import org.elasticsearch.xpack.sql.cli.net.client.HttpCliClient;
import org.elasticsearch.xpack.sql.cli.net.protocol.CommandResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.InfoResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

public class ProtoTest {

    private static Client esClient;
    private static CliHttpServer server;
    private static HttpCliClient client;

    @BeforeClass
    public static void setUp() throws Exception {
        if (esClient == null) {
            esClient = new PreBuiltTransportClient(Settings.EMPTY);
        }
        if (server == null) {
            server = new CliHttpServer(esClient);
            server.start(0);
            System.out.println("Server started at " + server.address().getPort());
        }

        if (client == null) {
            CliConfiguration ci = new CliConfiguration(server.url(), new Properties());
            client = new HttpCliClient(ci);
        }
    }

    @AfterClass
    public static void tearDown() {
        if (server != null) {
            server.stop();
            server = null;
        }

        if (client != null) {
            client.close();
            client = null;
        }

        if (esClient != null) {
            esClient.close();
            esClient = null;
        }
    }

    @Test
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

    @Test
    public void testBasicQuery() throws Exception {
        CommandResponse command = client.command("SHOW TABLES", null);
        System.out.println(command.data);
    }
}