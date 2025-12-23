/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.client.Request;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.test.rest.ObjectPath;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class KeywordRollingUpgradeIT extends AbstractStringTypeLogsdbRollingUpgradeTestCase {

    private static final String DATA_STREAM_NAME_PREFIX = "logs-keyword-bwc-test";

    private static final String TEMPLATE = """
        {
            "mappings": {
              "properties": {
                "@timestamp" : {
                  "type": "date"
                },
                "length": {
                  "type": "long"
                },
                "factor": {
                  "type": "double"
                },
                "message": {
                  "type": "keyword"
                }
              }
            }
        }""";

    private static final String TEMPLATE_WITH_IGNORE_ABOVE = """
        {
            "mappings": {
              "properties": {
                "@timestamp" : {
                  "type": "date"
                },
                "length": {
                  "type": "long"
                },
                "factor": {
                  "type": "double"
                },
                "message": {
                  "type": "keyword",
                  "ignore_above": 50
                }
              }
            }
        }""";

    private final String dataStreamName;
    private final Mapper.IgnoreAbove ignoreAbove;

    public KeywordRollingUpgradeIT(String template, String testScenario, Mapper.IgnoreAbove ignoreAbove) {
        super(DATA_STREAM_NAME_PREFIX + "." + testScenario, template);
        this.dataStreamName = DATA_STREAM_NAME_PREFIX + "." + testScenario;
        this.ignoreAbove = ignoreAbove;
    }

    @ParametersFactory
    public static Iterable<Object[]> data() {
        return Arrays.asList(
            new Object[][] {
                { TEMPLATE, "basic", new Mapper.IgnoreAbove(null, IndexMode.LOGSDB) },
                { TEMPLATE_WITH_IGNORE_ABOVE, "with-ignore-above", new Mapper.IgnoreAbove(50, IndexMode.LOGSDB) } }
        );
    }

    /**
     * We override this because the behavior around ignored keyword fields is unique. More specifically, block loaders will return null for
     * ignored values, so we must change how verifications work.
     */
    @Override
    protected void query() throws Exception {
        var queryRequest = new Request("POST", "/_query");
        queryRequest.addParameter("pretty", "true");
        queryRequest.setJsonEntity("""
            {
                "query": "FROM $ds | STATS max(length), max(factor) BY message | LIMIT 1000"
            }
            """.replace("$ds", dataStreamName));

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
            // block loaders return null for ignored keyword fields, so we must filter those out
            if (message != null) {
                queryMessages.add(message);
            }
        }

        // block loaders do not return ignored keyword fields, so we need to filter them out
        List<String> messages = getMessages();
        List<String> nonIgnoredMessage = ignoreAbove.isSet()
            ? messages.stream().filter(msg -> ignoreAbove.isIgnored(msg) == false).toList()
            : messages;

        // verify that every message in the messages list is present in the query response
        assertThat(
            "Expected messages: " + nonIgnoredMessage + "\nActual messages: " + queryMessages,
            queryMessages,
            containsInAnyOrder(nonIgnoredMessage.toArray())
        );
    }

}
