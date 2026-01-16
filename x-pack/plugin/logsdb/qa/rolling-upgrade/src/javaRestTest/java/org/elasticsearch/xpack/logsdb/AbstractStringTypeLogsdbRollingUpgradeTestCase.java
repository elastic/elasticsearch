/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public abstract class AbstractStringTypeLogsdbRollingUpgradeTestCase extends AbstractLogsdbRollingUpgradeTestCase {

    // template for individual log items
    private static final String ITEM_TEMPLATE = """
        { "create": {} }
        { "@timestamp": "$now", "message": "$message", "length": $length, "factor": $factor }
        """;

    private final String dataStreamName;
    private final String template;
    private final List<String> messages = new ArrayList<>();
    private final int numNodes;

    private String templateId;

    public AbstractStringTypeLogsdbRollingUpgradeTestCase(String dataStreamName, String template) {
        this.dataStreamName = dataStreamName;
        this.template = template;
        this.numNodes = Integer.parseInt(System.getProperty("tests.num_nodes", "3"));
    }

    @Before
    public void createIndex() throws Exception {
        checkRequiredFeatures();
        LogsdbIndexingRollingUpgradeIT.maybeEnableLogsdbByDefault();

        // data stream name should already be reflective of whats being tested, so template id can be random
        templateId = UUID.randomUUID().toString();
        LogsdbIndexingRollingUpgradeIT.createTemplate(dataStreamName, templateId, template);
    }

    /**
     * Override this method to add feature checks that must pass before the test runs.
     * Use {@code assumeTrue} to skip the test if required features are not available.
     */
    protected void checkRequiredFeatures() throws Exception {
        // Default: no additional feature requirements
    }

    protected List<String> getMessages() {
        return messages;
    }

    public void testIndexing() throws Exception {
        // before upgrading
        indexDocumentsAndVerifyResults();

        // verify that logsdb and synthetic source are enabled before proceeding
        // note, we must index at least one document to create the data stream (data streams are created lazily on first index)
        String firstBackingIndex = getDataStreamBackingIndexNames(dataStreamName).getFirst();
        var settings = (Map<?, ?>) getIndexSettings(firstBackingIndex, true).get(firstBackingIndex);
        assertThat(((Map<?, ?>) settings.get("settings")).get("index.mode"), equalTo("logsdb"));
        assertThat(((Map<?, ?>) settings.get("defaults")).get("index.mapping.source.mode"), equalTo("SYNTHETIC"));

        // during upgrade
        for (int i = 0; i < numNodes; i++) {
            upgradeNode(i);
            indexDocumentsAndVerifyResults();
        }

        // after everything is upgraded
        indexDocumentsAndVerifyResults();
    }

    private void indexDocumentsAndVerifyResults() throws Exception {
        // given - implicitly start from the previous state

        // when - index some documents
        indexDocuments(1, 5);

        // then

        // verify that the data stream is healthy and still as expected
        assertDataStream();

        // performs some searches and queries, expect everything to pass
        search();
        query();
    }

    /**
     * Verifies that we're still using the expected data stream and thats its healthy.
     */
    protected void assertDataStream() throws IOException {
        var getDataStreamsRequest = new Request("GET", "/_data_stream/" + dataStreamName);
        var getDataStreamResponse = client().performRequest(getDataStreamsRequest);

        assertOK(getDataStreamResponse);
        var dataStreams = entityAsMap(getDataStreamResponse);

        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.name"), equalTo(dataStreamName));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.indices"), hasSize(1));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.template"), equalTo(templateId));

        ensureGreen(dataStreamName);
    }

    /**
     * Generates a string containing a random number of tokens. Tokens are either
     * random length alpha sequences or random integers and are delimited by spaces.
     */
    private static String randomTokensDelimitedBySpace(int maxTokens, int minCodeUnits, int maxCodeUnits) {
        int numTokens = randomIntBetween(1, maxTokens);
        List<String> tokens = new ArrayList<>(numTokens);

        for (int i = 0; i < numTokens; i++) {
            if (randomBoolean()) {
                // alpha token
                tokens.add(randomAlphaOfLengthBetween(minCodeUnits, maxCodeUnits));
            } else {
                // numeric token
                tokens.add(Integer.toString(randomInt()));
            }
        }
        return String.join(" ", tokens);
    }

    /**
     * Create an arbitrary document containing random values and index it.
     */
    protected void indexDocuments(int numRequests, int numDocs) throws Exception {
        for (int i = 0; i < numRequests; i++) {
            // create the request
            Request request = new Request("POST", "/" + dataStreamName + "/_bulk");
            request.setJsonEntity(createRequestBody(numDocs, Instant.now()));
            request.addParameter("refresh", "true");

            // send the request and receive response
            var response = client().performRequest(request);
            var responseBody = entityAsMap(response);

            // assert response is ok
            assertOK(response);
            assertThat("errors in response:\n " + responseBody, responseBody.get("errors"), equalTo(false));
        }
    }

    private String createRequestBody(int numDocs, Instant startTime) {
        StringBuilder requestBody = new StringBuilder();

        for (int i = 0; i < numDocs; i++) {
            // generate payload
            long length = randomLong();
            double factor = randomDouble();
            String message = randomTokensDelimitedBySpace(10, 1, 15);

            // record each message for later verification
            messages.add(message);

            requestBody.append(
                ITEM_TEMPLATE.replace("$now", formatInstant(startTime))
                    .replace("$length", Long.toString(length))
                    .replace("$factor", Double.toString(factor))
                    .replace("$message", message)
            );
            requestBody.append("\n");

            startTime = startTime.plusMillis(1);
        }

        return requestBody.toString();
    }

    @SuppressWarnings("unchecked")
    private void search() throws IOException {
        Request searchRequest = new Request("GET", "/" + dataStreamName + "/_search");
        searchRequest.setJsonEntity("""
            {
            "query": { "match_all": {} },
            "size": 500
            }
            """);

        Response response = client().performRequest(searchRequest);
        assertOK(response);

        // parse the response
        Map<String, Object> responseMap = entityAsMap(response);

        // verify that the number of entries in the response matches the number of messages indexed
        Integer totalCount = ObjectPath.evaluate(responseMap, "hits.total.value");
        assertThat(totalCount, equalTo(messages.size()));

        // verify that each indexed message appears in the response
        List<String> values = ((List<Map<String, Object>>) ObjectPath.evaluate(responseMap, "hits.hits")).stream()
            .map(map -> (Map<String, Object>) map.get("_source"))
            .map(source -> {
                assertThat(source.get("message"), notNullValue());
                // The value of FIELD_NAME is now a single String, not a List<String>
                return (String) source.get("message");
            })
            .toList();
        assertThat(values, containsInAnyOrder(messages.toArray()));
    }

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
            queryMessages.add(message);
        }

        // verify that every message in the messages list is present in the query response
        assertThat(
            "Expected messages: " + messages + "\nActual messages: " + queryMessages,
            queryMessages,
            containsInAnyOrder(messages.toArray())
        );
    }

}
