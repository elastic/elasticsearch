/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.backingIndexEqualTo;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.dataStreamIndexEqualTo;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class FailureStoreUpgradeIT extends AbstractRollingUpgradeWithSecurityTestCase {

    public FailureStoreUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    private static TemporaryFolder repoDirectory = new TemporaryFolder();
    private static ElasticsearchCluster cluster = DefaultClusters.buildCluster(repoDirectory);
    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(repoDirectory).around(cluster);

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    final String INDEX_TEMPLATE = """
        {
            "index_patterns": ["$PATTERN"],
            "data_stream": {},
            "template": {
                "mappings":{
                    "properties": {
                        "@timestamp" : {
                            "type": "date"
                        },
                        "numeral": {
                            "type": "long"
                        }
                    }
                }
            }
        }""";

    private static final String VALID_DOC = """
        {"@timestamp": "$now", "numeral": 0}
        """;

    private static final String INVALID_DOC = """
        {"@timestamp": "$now", "numeral": "foobar"}
        """;

    private static final String BULK = """
        {"create": {}}
        {"@timestamp": "$now", "numeral": 0}
        {"create": {}}
        {"@timestamp": "$now", "numeral": 1}
        {"create": {}}
        {"@timestamp": "$now", "numeral": 2}
        {"create": {}}
        {"@timestamp": "$now", "numeral": 3}
        {"create": {}}
        {"@timestamp": "$now", "numeral": 4}
        """;

    private static final String ENABLE_FAILURE_STORE_OPTIONS = """
        {
          "failure_store": {
            "enabled": true
          }
        }
        """;

    public void testFailureStoreOnPreviouslyExistingDataStream() throws Exception {
        assumeFalse(
            "testing migration from data streams created before failure store feature existed",
            oldClusterHasFeature(DataStream.DATA_STREAM_FAILURE_STORE_FEATURE)
        );
        String dataStreamName = "fs-ds-upgrade-test";
        String failureStoreName = dataStreamName + "::failures";
        String templateName = "fs-ds-template";
        if (isOldCluster()) {
            // Create template
            var putIndexTemplateRequest = new Request("POST", "/_index_template/" + templateName);
            putIndexTemplateRequest.setJsonEntity(INDEX_TEMPLATE.replace("$PATTERN", dataStreamName));
            assertOK(client().performRequest(putIndexTemplateRequest));

            // Initialize data stream
            executeBulk(dataStreamName);

            // Ensure document failure
            indexDoc(dataStreamName, INVALID_DOC, false);

            // Check data stream state
            var dataStreams = getDataStream(dataStreamName);
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams"), hasSize(1));
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.name"), equalTo(dataStreamName));
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.generation"), equalTo(1));
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.template"), equalTo(templateName));
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.indices"), hasSize(1));
            String firstBackingIndex = ObjectPath.evaluate(dataStreams, "data_streams.0.indices.0.index_name");
            assertThat(firstBackingIndex, backingIndexEqualTo(dataStreamName, 1));

            assertDocCount(client(), dataStreamName, 5);
        } else if (isMixedCluster()) {
            ensureHealth(dataStreamName, request -> request.addParameter("wait_for_status", "yellow"));
            if (isFirstMixedCluster()) {
                indexDoc(dataStreamName, VALID_DOC, true);
                indexDoc(dataStreamName, INVALID_DOC, false);
            }
            assertDocCount(client(), dataStreamName, 6);
        } else if (isUpgradedCluster()) {
            ensureGreen(dataStreamName);

            // Ensure correct default failure store state for upgraded data stream
            var dataStreams = getDataStream(dataStreamName);
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams"), hasSize(1));
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.name"), equalTo(dataStreamName));
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.failure_store"), notNullValue());
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.failure_store.enabled"), equalTo(false));
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.failure_store.indices"), is(empty()));
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.failure_store.rollover_on_write"), equalTo(true));

            // Ensure invalid document is not indexed
            indexDoc(dataStreamName, INVALID_DOC, false);

            // Enable failure store on upgraded data stream
            var putOptionsRequest = new Request("PUT", "/_data_stream/" + dataStreamName + "/_options");
            putOptionsRequest.setJsonEntity(ENABLE_FAILURE_STORE_OPTIONS);
            assertOK(client().performRequest(putOptionsRequest));

            // Ensure correct enabled failure store state
            dataStreams = getDataStream(dataStreamName);
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams"), hasSize(1));
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.name"), equalTo(dataStreamName));
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.failure_store"), notNullValue());
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.failure_store.enabled"), equalTo(true));
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.failure_store.indices"), is(empty()));
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.failure_store.rollover_on_write"), equalTo(true));

            // Initialize failure store
            int expectedFailureDocuments = 0;
            if (randomBoolean()) {
                // Index a failure via a mapping exception
                indexDoc(dataStreamName, INVALID_DOC, true);
                expectedFailureDocuments = 1;
            } else {
                // Manually rollover failure store to force initialization
                var failureStoreRolloverRequest = new Request("POST", "/" + failureStoreName + "/_rollover");
                assertOK(client().performRequest(failureStoreRolloverRequest));
            }

            // Ensure correct initialized failure store state
            dataStreams = getDataStream(dataStreamName);
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams"), hasSize(1));
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.name"), equalTo(dataStreamName));
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.failure_store"), notNullValue());
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.failure_store.enabled"), equalTo(true));
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.failure_store.indices"), is(not(empty())));
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.failure_store.rollover_on_write"), equalTo(false));

            String failureIndexName = ObjectPath.evaluate(dataStreams, "data_streams.0.failure_store.indices.0.index_name");
            assertThat(failureIndexName, dataStreamIndexEqualTo(dataStreamName, 2, true));

            assertDocCount(client(), dataStreamName, 6);
            assertDocCount(client(), failureStoreName, expectedFailureDocuments);
        }
    }

    private static void indexDoc(String dataStreamName, String docBody, boolean expectSuccess) throws IOException {
        var indexRequest = new Request("POST", "/" + dataStreamName + "/_doc");
        indexRequest.addParameter("refresh", "true");
        indexRequest.setJsonEntity(docBody.replace("$now", formatInstant(Instant.now())));
        Response response = null;
        try {
            response = client().performRequest(indexRequest);
        } catch (ResponseException re) {
            response = re.getResponse();
        }
        assertNotNull(response);
        if (expectSuccess) {
            assertOK(response);
        } else {
            assertThat(response.getStatusLine().getStatusCode(), not(anyOf(equalTo(200), equalTo(201))));
        }
    }

    private static void executeBulk(String dataStreamName) throws IOException {
        var bulkRequest = new Request("POST", "/" + dataStreamName + "/_bulk");
        bulkRequest.setJsonEntity(BULK.replace("$now", formatInstant(Instant.now())));
        bulkRequest.addParameter("refresh", "true");
        Response response = null;
        try {
            response = client().performRequest(bulkRequest);
        } catch (ResponseException re) {
            response = re.getResponse();
        }
        assertNotNull(response);
        var responseBody = entityAsMap(response);
        assertOK(response);
        assertThat("errors in response:\n " + responseBody, responseBody.get("errors"), equalTo(false));
    }

    static String formatInstant(Instant instant) {
        return DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(instant);
    }

    private static Map<String, Object> getDataStream(String dataStreamName) throws IOException {
        var getDataStreamsRequest = new Request("GET", "/_data_stream/" + dataStreamName);
        var response = client().performRequest(getDataStreamsRequest);
        assertOK(response);
        return entityAsMap(response);
    }
}
