/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.integration.query;

import java.sql.Connection;
import java.util.function.Supplier;

import org.elasticsearch.xpack.sql.jdbc.integration.query.filter.FilterSpecTest;
import org.elasticsearch.xpack.sql.jdbc.integration.query.function.aggregate.AggSpecTest;
import org.elasticsearch.xpack.sql.jdbc.integration.query.function.scalar.datetime.DateTimeSpecTest;
import org.elasticsearch.xpack.sql.jdbc.integration.query.function.scalar.math.MathSpecTest;
import org.elasticsearch.xpack.sql.jdbc.integration.util.EsDataLoader;
import org.elasticsearch.xpack.sql.jdbc.integration.util.EsJdbcServer;
import org.elasticsearch.xpack.sql.jdbc.integration.util.H2;
import org.elasticsearch.xpack.sql.jdbc.integration.util.JdbcTemplate;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import static org.junit.Assert.assertNotNull;

@RunWith(Suite.class)
@SuiteClasses({ SelectSpecTest.class, FilterSpecTest.class, AggSpecTest.class, MathSpecTest.class, DateTimeSpecTest.class })
//@SuiteClasses({ DebugSpecTest.class })
//@SuiteClasses({ AggSpecTest.class })
//@SuiteClasses({ DateTimeSpecTest.class })
//@SuiteClasses({ MathSpecTest.class })
public class QuerySuite {

    //
    // REMOTE ACCESS
    //

    private static boolean REMOTE = true;

    private static String REMOTE_H2 = "jdbc:h2:tcp://localhost/./essql";

    @ClassRule
    public static H2 H2 = new H2(null);

    @ClassRule
    public static EsJdbcServer ES_JDBC_SERVER = new EsJdbcServer(REMOTE, true);

    private static JdbcTemplate H2_JDBC, ES_JDBC;

    @BeforeClass
    public static void setupDB() throws Exception {
        H2_JDBC = new JdbcTemplate(H2);
        //ES_CON = new JdbcTemplate(ES_JDBC_SERVER);

        setupH2();
        if (!REMOTE) {
            setupH2();
            setupES();
        }
    }

    private static void setupH2() throws Exception {
        h2().execute("RUNSCRIPT FROM 'classpath:org/elasticsearch/sql/jdbc/integration/h2-setup.sql'");
    }

    private static void setupES() throws Exception {
        EsDataLoader.loadData();
    }

    public static Supplier<Connection> h2Con() {
        return H2;
    }

    public static Supplier<Connection> esCon() {
        return ES_JDBC_SERVER;
    }

    public static JdbcTemplate h2() {
        assertNotNull("H2 connection null - make sure the suite is ran", H2_JDBC);
        return H2_JDBC;
    }

    public static JdbcTemplate es() {
        assertNotNull("ES connection null - make sure the suite is ran", H2_JDBC);
        return ES_JDBC;
    }
}