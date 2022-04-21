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
import org.elasticsearch.xpack.sql.qa.rest.BaseRestSqlTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.sql.qa.rest.RestSqlTestCase.SQL_QUERY_REST_ENDPOINT;

public class RestSqlDeprecationIT extends BaseRestSqlTestCase {

    public void testIndexIncludeParameterIsDeprecated() throws IOException {
        testDeprecationWarning(
            query("SELECT * FROM test").mode(randomMode()).indexIncludeFrozen(randomBoolean()),
            "[index_include_frozen] parameter is deprecated because frozen indices have been deprecated. Consider cold or frozen tiers "
                + "in place of frozen indices."
        );
    }

    public void testIncludeFrozenSyntaxIsDeprecatedInShowTables() throws IOException {
        testFrozenSyntaxIsDeprecated("SHOW TABLES INCLUDE FROZEN", "INCLUDE FROZEN");
    }

    public void testIncludeFrozenSyntaxIsDeprecatedInShowColumns() throws IOException {
        testFrozenSyntaxIsDeprecated("SHOW COLUMNS INCLUDE FROZEN IN test", "INCLUDE FROZEN");
    }

    public void testIncludeFrozenSyntaxIsDeprecatedInDescribeTable() throws IOException {
        testFrozenSyntaxIsDeprecated("DESCRIBE INCLUDE FROZEN test", "INCLUDE FROZEN");
    }

    public void testFrozenSyntaxIsDeprecatedInFromClause() throws IOException {
        testFrozenSyntaxIsDeprecated("SELECT * FROM FROZEN test", "FROZEN");
    }

    private void testFrozenSyntaxIsDeprecated(String query, String syntax) throws IOException {
        testDeprecationWarning(
            query(query).mode(randomMode()),
            "["
                + syntax
                + "] syntax is deprecated because frozen indices have been deprecated. "
                + "Consider cold or frozen tiers in place of frozen indices."
        );
    }

    private void testDeprecationWarning(RequestObjectBuilder query, String warning) throws IOException {
        index("{\"foo\": 1}");

        Request request = new Request("POST", SQL_QUERY_REST_ENDPOINT);
        request.setEntity(new StringEntity(query.toString(), ContentType.APPLICATION_JSON));
        request.setOptions(expectWarnings(warning));
    }

}
