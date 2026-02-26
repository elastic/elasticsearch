/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.TestFeatureService;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractLogsdbRollingUpgradeTestCase extends ESRestTestCase {
    private static final String USER = "admin-user";
    private static final String PASS = "x-pack-test-password";

    private static TestFeatureService oldClusterTestFeatureService;

    @Before
    public void retainOldClusterTestFeatureService() {
        if (oldClusterTestFeatureService == null) {
            assert testFeatureServiceInitialized() : "testFeatureService must be initialized, see ESRestTestCase#initClient";
            oldClusterTestFeatureService = testFeatureService;
        }
    }

    protected static boolean oldClusterHasFeature(String featureId) {
        assert oldClusterTestFeatureService != null
            : "testFeatureService of old cluster cannot be accessed before initialization is complete";
        return oldClusterTestFeatureService.clusterHasFeature(featureId);
    }

    protected static boolean oldClusterHasFeature(NodeFeature feature) {
        return oldClusterHasFeature(feature.id());
    }

    @ClassRule
    public static final ElasticsearchCluster cluster = Clusters.oldVersionCluster(USER, PASS);

    @Override
    protected String getTestRestCluster() {
        return getCluster().getHttpAddresses();
    }

    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    protected void upgradeNode(int n) throws IOException {
        closeClients();

        var serverlessBwcStackVersion = System.getProperty("tests.serverless.bwc_stack_version");
        var bwcTag = System.getProperty("tests.bwc.tag");
        var newClusterVersion = System.getProperty("tests.new_cluster_version");
        logger.info("serverlessBwcStackVersion={}, bwcTag={}, newClusterVersion={}", serverlessBwcStackVersion, bwcTag, newClusterVersion);

        var upgradeVersion = newClusterVersion != null ? Version.fromString(newClusterVersion) : Version.CURRENT;
        logger.info("Upgrading node {} to version {}", n, upgradeVersion);
        getCluster().upgradeNodeToVersion(n, upgradeVersion);
        initClient();
    }

    protected ElasticsearchCluster getCluster() {
        return cluster;
    }

    static String formatInstant(Instant instant) {
        return DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(instant);
    }

    static String bulkIndex(
        String dataStreamName,
        int numRequest,
        int numDocs,
        Instant startTime,
        BiFunction<Instant, Integer, String> docSupplier
    ) throws Exception {
        String firstIndex = null;
        for (int i = 0; i < numRequest; i++) {
            var bulkRequest = new Request("POST", "/" + dataStreamName + "/_bulk");
            StringBuilder requestBody = new StringBuilder();
            for (int j = 0; j < numDocs; j++) {
                requestBody.append("{\"create\": {}}");
                requestBody.append('\n');
                requestBody.append(docSupplier.apply(startTime, j));
                requestBody.append('\n');

                startTime = startTime.plusMillis(1);
            }
            bulkRequest.setJsonEntity(requestBody.toString());
            bulkRequest.addParameter("refresh", "true");
            var response = client().performRequest(bulkRequest);
            assertOK(response);
            var responseBody = entityAsMap(response);
            assertThat("errors in response:\n " + responseBody, responseBody.get("errors"), equalTo(false));
            if (firstIndex == null) {
                firstIndex = (String) ((Map<?, ?>) ((Map<?, ?>) ((List<?>) responseBody.get("items")).get(0)).get("create")).get("_index");
            }
        }
        return firstIndex;
    }

    static void createTemplate(String dataStreamName, String id, String template) throws IOException {
        final String INDEX_TEMPLATE = """
            {
                "priority": 200,
                "index_patterns": ["$DATASTREAM"],
                "template": $TEMPLATE,
                "data_stream": {
                }
            }""";
        var putIndexTemplateRequest = new Request("POST", "/_index_template/" + id);
        putIndexTemplateRequest.setJsonEntity(INDEX_TEMPLATE.replace("$TEMPLATE", template).replace("$DATASTREAM", dataStreamName));
        assertOK(client().performRequest(putIndexTemplateRequest));
    }
}
