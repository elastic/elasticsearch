/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.client.Request;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.test.rest.ObjectPath;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Base class for string field types that support ignore_above. Provides query verification that handles ignored values.
 */
public abstract class AbstractStringTypeWithIgnoreAboveLogsdbRollingUpgradeTestCase extends AbstractStringTypeLogsdbRollingUpgradeTestCase {

    private final Mapper.IgnoreAbove ignoreAbove;

    protected AbstractStringTypeWithIgnoreAboveLogsdbRollingUpgradeTestCase(
        String dataStreamName,
        String template,
        Mapper.IgnoreAbove ignoreAbove
    ) {
        super(dataStreamName, template);
        this.ignoreAbove = ignoreAbove;
    }

    @Override
    void query() throws Exception {
        var queryRequest = new Request("POST", "/_query");
        queryRequest.addParameter("pretty", "true");
        queryRequest.setJsonEntity("""
            {
                "query": "FROM $ds | STATS max(length), max(factor) BY message | LIMIT 1000"
            }
            """.replace("$ds", getDataStreamName()));

        var response = client().performRequest(queryRequest);
        assertOK(response);

        // parse response
        var responseBody = entityAsMap(response);
        logger.info("{}", responseBody);

        // verify column names
        String column1 = ObjectPath.evaluate(responseBody, "columns.0.name");
        assertThat(column1, equalTo("max(length)"));
        String column2 = ObjectPath.evaluate(responseBody, "columns.1.name");
        assertThat(column2, equalTo("max(factor)"));
        String column3 = ObjectPath.evaluate(responseBody, "columns.2.name");
        assertThat(column3, equalTo("message"));

        // extract all values from the response and verify each row
        List<List<Object>> values = ObjectPath.evaluate(responseBody, "values");
        List<String> queryMessages = new ArrayList<>();
        for (List<Object> row : values) {
            // verify that values are non-null
            Long maxRx = (Long) row.get(0);
            assertThat(maxRx, notNullValue());
            Double maxTx = (Double) row.get(1);
            assertThat(maxTx, notNullValue());

            // collect message for later verification
            String message = (String) row.get(2);

            // block loaders return null for ignored fields, so we must filter those out
            if (message != null) {
                queryMessages.add(message);
            }
        }

        // block loaders do not return ignored fields, so we need to filter out all generates messages that would've been ignored
        List<String> messages = getMessages();
        List<String> expectedMessages = ignoreAbove.isSet()
            ? messages.stream().filter(msg -> ignoreAbove.isIgnored(msg) == false).toList()
            : messages;

        // verify that every message in the messages list is present in the query response
        assertThat(
            "Expected messages: " + expectedMessages + "\nActual messages: " + queryMessages,
            queryMessages,
            containsInAnyOrder(expectedMessages.toArray())
        );
    }
}
