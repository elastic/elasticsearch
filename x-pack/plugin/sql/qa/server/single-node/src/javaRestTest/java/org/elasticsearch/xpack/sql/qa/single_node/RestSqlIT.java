/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.single_node;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
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
        expectBadRequest(
            () -> runTranslateSql(query("SELECT * FROM test WHERE foo = 1 AND foo = 2").toString()),
            containsString(
                "Cannot generate a query DSL for an SQL query that either its WHERE clause evaluates "
                    + "to FALSE or doesn't operate on a table (missing a FROM clause), sql statement: "
                    + "[SELECT * FROM test WHERE foo = 1 AND foo = 2]"
            )
        );
    }

    public void testErrorMessageForTranslatingQueryWithLocalExecution() throws IOException {
        index("{\"foo\":1}");
        expectBadRequest(
            () -> runTranslateSql(query("SELECT SIN(PI())").toString()),
            containsString(
                "Cannot generate a query DSL for an SQL query that either its WHERE clause evaluates "
                    + "to FALSE or doesn't operate on a table (missing a FROM clause), sql statement: [SELECT SIN(PI())]"
            )
        );
    }

    public void testErrorMessageForTranslatingSQLCommandStatement() throws IOException {
        index("{\"foo\":1}");
        expectBadRequest(
            () -> runTranslateSql(query("SHOW FUNCTIONS").toString()),
            containsString(
                "Cannot generate a query DSL for a special SQL command " + "(e.g.: DESCRIBE, SHOW), sql statement: [SHOW FUNCTIONS]"
            )
        );
    }

    public void testErrorMessageForInvalidParamDataType() throws IOException {
        // proto.Mode not available
        expectBadRequest(
            () -> runTranslateSql(
                query("SELECT null WHERE 0 = ?").mode("odbc").params("[{\"type\":\"invalid\", \"value\":\"irrelevant\"}]").toString()
            ),
            containsString("Invalid parameter data type [invalid]")
        );
    }

    public void testErrorMessageForInvalidParamSpec() throws IOException {
        expectBadRequest(
            () -> runTranslateSql(
                query("SELECT null WHERE 0 = ?").mode("odbc").params("[{\"type\":\"SHAPE\", \"value\":false}]").toString()
            ),
            containsString("Cannot cast value [false] of type [BOOLEAN] to parameter type [SHAPE]")
        );

    }

    public void testIncorrectAcceptHeader() throws IOException {
        index("{\"foo\":1}");
        Request request = new Request("POST", SQL_QUERY_REST_ENDPOINT);
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Accept", "application/fff");
        request.setOptions(options);
        StringEntity stringEntity = new StringEntity(query("select * from test").toString(), ContentType.APPLICATION_JSON);
        request.setEntity(stringEntity);
        expectBadRequest(
            () -> toMap(client().performRequest(request), "plain"),
            containsString("Invalid request content type: Accept=[application/fff]")
        );
    }
}
