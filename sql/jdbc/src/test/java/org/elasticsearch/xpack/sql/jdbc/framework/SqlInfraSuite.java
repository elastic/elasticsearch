/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.integration.util.framework;

import java.sql.Connection;
import java.util.function.Supplier;

import org.elasticsearch.xpack.sql.jdbc.integration.util.H2;
import org.elasticsearch.xpack.sql.jdbc.integration.util.JdbcTemplate;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import static org.junit.Assert.assertNotNull;

@RunWith(Suite.class)
public abstract class SqlInfraSuite extends EsInfra {

    private static String REMOTE_H2 = "jdbc:h2:tcp://localhost/./essql";

    @ClassRule
    public static H2 H2 = new H2(null);

    private static JdbcTemplate H2_JDBC;

    @BeforeClass
    public static void setupDB() throws Exception {
        H2_JDBC = new JdbcTemplate(H2);
        setupH2();
        EsInfra.setupDB();
    }

    private static void setupH2() throws Exception {
        h2().execute("RUNSCRIPT FROM 'classpath:org/elasticsearch/sql/jdbc/integration/h2-setup.sql'");
    }

    public static Supplier<Connection> h2Con() {
        return H2;
    }

    public static JdbcTemplate h2() {
        assertNotNull("H2 connection null - make sure the suite is ran", H2_JDBC);
        return H2_JDBC;
    }
}