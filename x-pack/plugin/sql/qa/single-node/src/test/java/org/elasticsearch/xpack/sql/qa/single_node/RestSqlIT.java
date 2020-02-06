/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.qa.single_node;

import org.elasticsearch.xpack.sql.qa.rest.RestSqlTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

/**
 * Integration test for the rest sql action. The one that speaks json directly to a
 * user rather than to the JDBC driver or CLI.
 */
public class RestSqlIT extends RestSqlTestCase {


    public void testErrorMessageForTranslatingQueryWithWhereEvaluatingToFalse() throws IOException {
        index("{\"foo\":1}");
        expectBadRequest(() -> runTranslateSql("{\"query\":\"SELECT * FROM test WHERE foo = 1 AND foo = 2\"}"),
            containsString("Cannot generate a query DSL for an SQL query that either its WHERE clause evaluates " +
                "to FALSE or doesn't operate on a table (missing a FROM clause), sql statement: " +
                "[SELECT * FROM test WHERE foo = 1 AND foo = 2]"));
    }

    public void testErrorMessageForTranslatingQueryWithLocalExecution() throws IOException {
        index("{\"foo\":1}");
        expectBadRequest(() -> runTranslateSql("{\"query\":\"SELECT SIN(PI())\"}"),
            containsString("Cannot generate a query DSL for an SQL query that either its WHERE clause evaluates " +
                "to FALSE or doesn't operate on a table (missing a FROM clause), sql statement: [SELECT SIN(PI())]"));
    }

    public void testErrorMessageForTranslatingSQLCommandStatement() throws IOException {
        index("{\"foo\":1}");
        expectBadRequest(() -> runTranslateSql("{\"query\":\"SHOW FUNCTIONS\"}"),
            containsString("Cannot generate a query DSL for a special SQL command " +
                "(e.g.: DESCRIBE, SHOW), sql statement: [SHOW FUNCTIONS]"));
    }
}
