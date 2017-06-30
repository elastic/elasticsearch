/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.integration.util.framework;

import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

import org.elasticsearch.xpack.sql.jdbc.integration.util.JdbcTemplate.JdbcSupplier;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.relique.jdbc.csv.CsvDriver;

@RunWith(Suite.class)
public abstract class CsvInfraSuite extends EsInfra {

    private static CsvDriver DRIVER = new CsvDriver();
    public static final Map<Connection, Reader> CSV_READERS = new LinkedHashMap<>();

    @BeforeClass
    public static void setupDB() throws Exception {
        EsInfra.setupDB();
    }

    @AfterClass
    public static void cleanup() throws Exception {
        CSV_READERS.clear();
    }

    public static Supplier<Connection> csvCon(Properties props, Reader reader) {
        return new JdbcSupplier<Connection>() {
            @Override
            public Connection jdbc() throws SQLException {
                Connection con = DRIVER.connect("jdbc:relique:csv:class:" + CsvSpecTableReader.class.getName(), props);
                CSV_READERS.put(con, reader);
                return con;
            }
        };
    }
}