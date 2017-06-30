/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.integration.util.framework;

import java.sql.Connection;
import java.util.function.Supplier;

import org.elasticsearch.xpack.sql.jdbc.integration.util.EsDataLoader;
import org.elasticsearch.xpack.sql.jdbc.integration.util.EsJdbcServer;
import org.elasticsearch.xpack.sql.jdbc.integration.util.JdbcTemplate;
import org.junit.ClassRule;

import static org.junit.Assert.assertNotNull;

public class EsInfra {

    //
    // REMOTE ACCESS
    //
    private static boolean REMOTE = true;

    @ClassRule
    public static EsJdbcServer ES_JDBC_SERVER = new EsJdbcServer(REMOTE, false);

    private static JdbcTemplate ES_JDBC;

    public static void setupDB() throws Exception {
        //ES_CON = new JdbcTemplate(ES_JDBC_SERVER);

        if (!REMOTE) {
            setupES();
        }
    }

    private static void setupES() throws Exception {
        EsDataLoader.loadData();
    }

    public static Supplier<Connection> esCon() {
        return ES_JDBC_SERVER;
    }

    public static JdbcTemplate es() {
        assertNotNull("ES connection null - make sure the suite is ran", ES_JDBC);
        return ES_JDBC;
    }
}