/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.single_node;

import org.elasticsearch.xpack.sql.qa.jdbc.JdbcIntegrationTestCase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class JdbcWarningsIT extends JdbcIntegrationTestCase {

    public void testDeprecationWarningsDoNotReachJdbcDriver() throws Exception {
        index("test_data", b -> b.field("foo", 1));

        try (Connection connection = esJdbc(); Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery("SELECT * FROM FROZEN \"test_*\"");
            assertNull(rs.getWarnings());
        }
    }

}
