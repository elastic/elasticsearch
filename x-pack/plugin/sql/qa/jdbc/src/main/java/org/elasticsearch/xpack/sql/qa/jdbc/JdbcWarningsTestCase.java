/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.jdbc;

import org.junit.Before;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static org.elasticsearch.xpack.sql.proto.VersionCompatibility.INTRODUCING_WARNING_HANDLING;
import static org.elasticsearch.xpack.sql.qa.jdbc.JdbcTestUtils.JDBC_DRIVER_VERSION;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;

public abstract class JdbcWarningsTestCase extends JdbcIntegrationTestCase {

    @Before
    public void setupData() throws IOException {
        index("test_data", b -> b.field("foo", 1));
    }

    public void testNoWarnings() throws SQLException {
        try (Connection connection = esJdbc(); Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery("SELECT * FROM test_data");
            assertNull(rs.getWarnings());
        }
    }

    public void testSingleDeprecationWarning() throws SQLException {
        assumeWarningHandlingDriverVersion();

        try (Connection connection = esJdbc(); Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery("SELECT * FROM FROZEN test_data");
            SQLWarning warning = rs.getWarnings();
            assertThat(warning.getMessage(), containsString("[FROZEN] syntax is deprecated because frozen indices have been deprecated."));
            assertNull(warning.getNextWarning());
        }
    }

    public void testMultipleDeprecationWarnings() throws SQLException {
        assumeWarningHandlingDriverVersion();

        Properties props = connectionProperties();
        props.setProperty("index.include.frozen", "true");

        try (Connection connection = esJdbc(props); Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery("SELECT * FROM FROZEN test_data");
            List<String> warnings = new LinkedList<>();
            SQLWarning warning = rs.getWarnings();
            while (warning != null) {
                warnings.add(warning.getMessage());
                warning = warning.getNextWarning();
            }

            assertThat(
                warnings,
                containsInAnyOrder(
                    containsString("[FROZEN] syntax is deprecated because frozen indices have been deprecated."),
                    containsString("[index_include_frozen] parameter is deprecated because frozen indices have been deprecated.")
                )
            );
        }
    }

    public void testClearWarnings() throws SQLException {
        assumeWarningHandlingDriverVersion();

        try (Connection connection = esJdbc(); Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery("SELECT * FROM FROZEN test_data");
            assertNotNull(rs.getWarnings());

            rs.clearWarnings();
            assertNull(rs.getWarnings());
        }
    }

    private void assumeWarningHandlingDriverVersion() {
        assumeTrue("Driver does not yet handle deprecation warnings", JDBC_DRIVER_VERSION.onOrAfter(INTRODUCING_WARNING_HANDLING));
    }

}
