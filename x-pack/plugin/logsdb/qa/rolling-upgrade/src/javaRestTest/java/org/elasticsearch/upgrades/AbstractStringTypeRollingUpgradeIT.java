/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.upgrades.StandardToLogsDbIndexModeRollingUpgradeIT.enableLogsdbByDefault;
import static org.elasticsearch.upgrades.StandardToLogsDbIndexModeRollingUpgradeIT.getWriteBackingIndex;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public abstract class AbstractStringTypeRollingUpgradeIT extends AbstractRollingUpgradeWithSecurityTestCase {

    private static final String DATA_STREAM = "logs-bwc-test";

    private static final int IGNORE_ABOVE_MAX = 256;
    private static final int NUM_REQUESTS = 4;
    private static final int NUM_DOCS_PER_REQUEST = 1024;

    static String BULK_ITEM_TEMPLATE =
        """
            { "create": {} }
            {"@timestamp": "$now", "host.name": "$host", "method": "$method", "ip": "$ip", "message": "$message", "length": $length, "factor": $factor}
            """;

    private static final String TEMPLATE = """
        {
            "mappings": {
              "properties": {
                "@timestamp" : {
                  "type": "date"
                },
                "method": {
                  "type": "keyword"
                },
                "message": $MESSAGE_MAPPING,
                "ip": {
                  "type": "ip"
                },
                "length": {
                  "type": "long"
                },
                "factor": {
                  "type": "double"
                }
              }
            }
        }""";

    // when sorted, this message will appear at the top and hence can be used to validate query results
    private static Map<String, String> smallestMessageMap = new TreeMap<>();

    public AbstractStringTypeRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    abstract String stringType();

    public void testIndexing() throws Exception {
        testIndexing(false);
    }

    public void testIndexingWithMultifield() throws Exception {
        testIndexing(true);
    }

    protected void testIndexing(boolean shouldIncludeKeywordMultifield) throws Exception {
        String dataStreamName = DATA_STREAM + (shouldIncludeKeywordMultifield ? "-multifield" : "");
        if (isOldCluster()) {
            // given - reset data stream since it could've been used by other tests
            smallestMessageMap.remove(dataStreamName);

            // given - enable logsdb and create a template
            startTrial();
            enableLogsdbByDefault();
            String templateId = getClass().getSimpleName().toLowerCase(Locale.ROOT) + (shouldIncludeKeywordMultifield ? "-multifield" : "");
            createTemplate(dataStreamName, templateId, prepareTemplate(shouldIncludeKeywordMultifield));

            // when - index some documents
            bulkIndex(dataStreamName, NUM_REQUESTS, NUM_DOCS_PER_REQUEST);

            // then - verify that logsdb and synthetic source are both enabled
            String firstBackingIndex = getWriteBackingIndex(client(), dataStreamName, 0);
            var settings = (Map<?, ?>) getIndexSettingsWithDefaults(firstBackingIndex).get(firstBackingIndex);
            assertThat(((Map<?, ?>) settings.get("settings")).get("index.mode"), equalTo("logsdb"));
            assertThat(((Map<?, ?>) settings.get("defaults")).get("index.mapping.source.mode"), equalTo("SYNTHETIC"));

            // then continued - verify that the created data stream using the created template
            LogsdbIndexingRollingUpgradeIT.assertDataStream(dataStreamName, templateId);

            // when/then - run some queries and verify results
            ensureGreen(dataStreamName);
            search(dataStreamName);
            phraseSearch(dataStreamName);
            query(dataStreamName);
        } else if (isMixedCluster()) {
            // when
            bulkIndex(dataStreamName, NUM_REQUESTS, NUM_DOCS_PER_REQUEST);

            // when/then
            ensureGreen(dataStreamName);
            search(dataStreamName);
            phraseSearch(dataStreamName);
            query(dataStreamName);
        } else if (isUpgradedCluster()) {
            // when/then
            ensureGreen(dataStreamName);
            bulkIndex(dataStreamName, NUM_REQUESTS, NUM_DOCS_PER_REQUEST);
            search(dataStreamName);
            phraseSearch(dataStreamName);
            query(dataStreamName);

            // when/then continued - force merge all shard segments into one
            var forceMergeRequest = new Request("POST", "/" + dataStreamName + "/_forcemerge");
            forceMergeRequest.addParameter("max_num_segments", "1");
            assertOK(client().performRequest(forceMergeRequest));

            // then continued
            ensureGreen(dataStreamName);
            search(dataStreamName);
            query(dataStreamName);
        }
    }

    private String prepareTemplate(boolean shouldIncludeKeywordMultifield) throws IOException {
        XContentBuilder b = XContentFactory.jsonBuilder();
        b.startObject();
        b.field("type", stringType());

        if (shouldIncludeKeywordMultifield) {
            b.startObject("fields").startObject("keyword");
            b.field("type", "keyword");
            boolean shouldSetIgnoreAbove = randomBoolean();
            if (shouldSetIgnoreAbove) {
                b.field("ignore_above", randomInt(IGNORE_ABOVE_MAX));
            }
            b.endObject().endObject();
        }
        b.endObject();

        return TEMPLATE.replace("$MESSAGE_MAPPING", Strings.toString(b));
    }

    static void createTemplate(String dataStreamName, String id, String template) throws IOException {
        final String INDEX_TEMPLATE = """
            {
                "priority": 500,
                "index_patterns": ["$DATASTREAM"],
                "template": $TEMPLATE,
                "data_stream": {
                }
            }""";
        var putIndexTemplateRequest = new Request("POST", "/_index_template/" + id);
        putIndexTemplateRequest.setJsonEntity(INDEX_TEMPLATE.replace("$TEMPLATE", template).replace("$DATASTREAM", dataStreamName));
        assertOK(client().performRequest(putIndexTemplateRequest));
    }

    private void bulkIndex(String dataStreamName, int numRequest, int numDocs) throws Exception {
        String firstIndex = null;
        Instant startTime = Instant.now().minusSeconds(60 * 60);

        for (int i = 0; i < numRequest; i++) {
            var bulkRequest = new Request("POST", "/" + dataStreamName + "/_bulk");
            bulkRequest.setJsonEntity(bulkIndexRequestBody(dataStreamName, numDocs, startTime));
            bulkRequest.addParameter("refresh", "true");

            var response = client().performRequest(bulkRequest);
            var responseBody = entityAsMap(response);

            assertOK(response);
            assertThat("errors in response:\n " + responseBody, responseBody.get("errors"), equalTo(false));
            if (firstIndex == null) {
                firstIndex = (String) ((Map<?, ?>) ((Map<?, ?>) ((List<?>) responseBody.get("items")).get(0)).get("create")).get("_index");
            }
        }
    }

    private String bulkIndexRequestBody(String dataStreamName, int numDocs, Instant startTime) {
        StringBuilder requestBody = new StringBuilder();

        for (int j = 0; j < numDocs; j++) {
            String hostName = "host" + j % 50; // Not realistic, but makes asserting search / query response easier.
            String methodName = "method" + j % 5;
            String ip = NetworkAddress.format(randomIp(true));
            String message = randomAlphasDelimitedBySpace(10, 1, 15);
            recordSmallestMessage(dataStreamName, message);
            long length = randomLong();
            double factor = randomDouble();

            requestBody.append(
                BULK_ITEM_TEMPLATE.replace("$now", formatInstant(startTime))
                    .replace("$host", hostName)
                    .replace("$method", methodName)
                    .replace("$ip", ip)
                    .replace("$message", message)
                    .replace("$length", Long.toString(length))
                    .replace("$factor", Double.toString(factor))
            );
            requestBody.append('\n');

            startTime = startTime.plusMillis(1);
        }

        return requestBody.toString();
    }

    /**
     * Generates a string containing a random number of random length alphas, all delimited by space.
     */
    private static String randomAlphasDelimitedBySpace(int maxAlphas, int minCodeUnits, int maxCodeUnits) {
        int numAlphas = randomIntBetween(1, maxAlphas);
        List<String> alphas = new ArrayList<>(numAlphas);
        for (int i = 0; i < numAlphas; i++) {
            alphas.add(randomAlphaOfLengthBetween(minCodeUnits, maxCodeUnits));
        }
        return String.join(" ", alphas);
    }

    private void recordSmallestMessage(final String dataStreamName, final String message) {
        if (smallestMessageMap.containsKey(dataStreamName) == false || message.compareTo(smallestMessageMap.get(dataStreamName)) < 0) {
            smallestMessageMap.put(dataStreamName, message);
        }
    }

    private void search(String dataStreamName) throws Exception {
        var searchRequest = new Request("POST", "/" + dataStreamName + "/_search");
        searchRequest.addParameter("pretty", "true");
        searchRequest.setJsonEntity("""
            {
                "size": 500
            }
            """);
        var response = client().performRequest(searchRequest);
        assertOK(response);
        var responseBody = entityAsMap(response);
        logger.info("{}", responseBody);

        Integer totalCount = ObjectPath.evaluate(responseBody, "hits.total.value");
        assertThat(totalCount, greaterThanOrEqualTo(NUM_REQUESTS * NUM_DOCS_PER_REQUEST));
    }

    private void phraseSearch(String dataStreamName) throws Exception {
        var searchRequest = new Request("POST", "/" + dataStreamName + "/_search");
        searchRequest.addParameter("pretty", "true");
        searchRequest.setJsonEntity("""
                {
                    "query": {
                        "match_phrase": {
                            "message": "$smallestMessage"
                        }
                    }
                }
            """.replace("$smallestMessage", smallestMessageMap.get(dataStreamName)));
        var response = client().performRequest(searchRequest);
        assertOK(response);
        var responseBody = entityAsMap(response);
        logger.info("{}", responseBody);
        assertThat(ObjectPath.evaluate(responseBody, "hits.total.value"), greaterThanOrEqualTo(1));
    }

    private void query(String dataStreamName) throws Exception {
        var queryRequest = new Request("POST", "/_query");
        queryRequest.addParameter("pretty", "true");
        queryRequest.setJsonEntity("""
            {
                "query": "FROM $ds | STATS max(length), max(factor) BY message | SORT message | LIMIT 5"
            }
            """.replace("$ds", dataStreamName));
        var response = client().performRequest(queryRequest);
        assertOK(response);
        var responseBody = entityAsMap(response);
        logger.info("{}", responseBody);

        String column1 = ObjectPath.evaluate(responseBody, "columns.0.name");
        assertThat(column1, equalTo("max(length)"));
        String column2 = ObjectPath.evaluate(responseBody, "columns.1.name");
        assertThat(column2, equalTo("max(factor)"));
        String column3 = ObjectPath.evaluate(responseBody, "columns.2.name");
        assertThat(column3, equalTo("message"));

        Long maxRx = ObjectPath.evaluate(responseBody, "values.0.0");
        assertThat(maxRx, notNullValue());
        Double maxTx = ObjectPath.evaluate(responseBody, "values.0.1");
        assertThat(maxTx, notNullValue());
        String key = ObjectPath.evaluate(responseBody, "values.0.2");
        assertThat(key, equalTo(smallestMessageMap.get(dataStreamName)));
    }

    protected static void startTrial() throws IOException {
        Request startTrial = new Request("POST", "/_license/start_trial");
        startTrial.addParameter("acknowledge", "true");
        try {
            assertOK(client().performRequest(startTrial));
        } catch (ResponseException e) {
            var responseBody = entityAsMap(e.getResponse());
            String error = ObjectPath.evaluate(responseBody, "error_message");
            assertThat(error, containsString("Trial was already activated."));
        }
    }

    static Map<String, Object> getIndexSettingsWithDefaults(String index) throws IOException {
        Request request = new Request("GET", "/" + index + "/_settings");
        request.addParameter("flat_settings", "true");
        request.addParameter("include_defaults", "true");
        Response response = client().performRequest(request);
        try (InputStream is = response.getEntity().getContent()) {
            return XContentHelper.convertToMap(
                XContentType.fromMediaType(response.getEntity().getContentType().getValue()).xContent(),
                is,
                true
            );
        }
    }

    static String formatInstant(Instant instant) {
        return DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(instant);
    }

}
