/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.sql.jdbc.integration.util.JdbcTemplate;
import org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcDriver;
import org.junit.Before;

import java.io.IOException;
import java.sql.DriverManager;

import static java.util.Collections.singletonMap;

public class JdbcIntegrationTestCase extends ESRestTestCase {
    static {
        // Initialize the jdbc driver
        JdbcDriver.jdbcMajorVersion();
    }

    protected JdbcTemplate j;

    @Before
    public void setupJdbcTemplate() throws Exception {
        j = new JdbcTemplate(() -> DriverManager.getConnection("jdbc:es://" + System.getProperty("tests.rest.cluster")));
    }

    protected void index(String index, CheckedConsumer<XContentBuilder, IOException> body) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        body.accept(builder);
        builder.endObject();
        HttpEntity doc = new StringEntity(builder.string(), ContentType.APPLICATION_JSON);
        client().performRequest("PUT", "/" + index + "/doc/1", singletonMap("refresh", "true"), doc);
    }
}
