/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.Version;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.test.SecuritySettingsSourceField;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SystemIndicesUpgradeIT extends AbstractUpgradeTestCase {
    private static final Logger log = LogManager.getLogger(SystemIndicesUpgradeIT.class);
    private static final String BASIC_AUTH_VALUE = basicAuthHeaderValue(
        "test_user",
        new SecureString(SecuritySettingsSourceField.TEST_PASSWORD)
    );

    @Override
    protected Settings restAdminSettings() {
        // Note that we are both superuser here and provide a product origin
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE)
            .put(ThreadContext.PREFIX + "." + Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER, "fleet")
            .build();
    }

    public void testUpgradeSystemIndexAndDataStream() throws Exception {
        String dataStreamName = ".fleet-actions-results";
        String indexName = ".fleet-actions";
        if (CLUSTER_TYPE == AbstractUpgradeTestCase.ClusterType.OLD) {
            addDataTo(dataStreamName);
            addDataTo(indexName);
            verifyDataStream(dataStreamName);
            verifyAccessToIndex(dataStreamName);
            verifyAccessToIndex(indexName);
        } else if (CLUSTER_TYPE == AbstractUpgradeTestCase.ClusterType.UPGRADED) {
            upgradeSystemIndices();
            verifyDataStream(dataStreamName);
            verifyIndex(indexName);
            verifyAccessToIndex(dataStreamName);
            verifyAccessToIndex(indexName);
        }
    }

    private void verifyDataStream(String dataStreamName) throws IOException {
        Map<String, Map<String, Map<String, Object>>> metadata = getMetadata(dataStreamName);
        assertThat(getProperty(metadata, List.of("data_stream", "data_stream", dataStreamName, "system")), equalTo("true"));

        Map<String, Map<String, Object>> upgradedIndicesMetadata = metadata.get("indices");
        for (Map.Entry<String, Map<String, Object>> indexEntry : upgradedIndicesMetadata.entrySet()) {
            Map<String, Object> indexProperties = indexEntry.getValue();
            verifySystemIndexProperties(indexProperties);
        }
    }

    private static void verifyAccessToIndex(String aliasOrDataStreamName) throws IOException {
        Request fleetCountRequest = new Request("GET", aliasOrDataStreamName + "/_count");
        Response fleetCountResponse = adminClient().performRequest(fleetCountRequest);
        assertOK(fleetCountResponse);
        assertThat(
            XContentHelper.convertToMap(JsonXContent.jsonXContent, fleetCountResponse.getEntity().getContent(), false).get("count"),
            equalTo(1)
        );
    }

    private void addDataTo(String indexName) throws IOException {
        Request request = new Request("POST", indexName + "/_doc");
        request.addParameter("refresh", "true");
        request.setJsonEntity("{\"@timestamp\": 0}");
        assertOK(adminClient().performRequest(request));
    }

    private void verifyIndex(String indexName) throws IOException {
        Map<String, Map<String, Object>> indexMetadata = getIndexMetadata(indexName);
        assertThat(indexMetadata, aMapWithSize(1));
        Map<String, Object> indexProperties = indexMetadata.values().iterator().next();
        verifySystemIndexProperties(indexProperties);
    }

    private static void verifySystemIndexProperties(Map<String, Object> indexProperties) {
        assertThat(getProperty(indexProperties, List.of("system")), equalTo("true"));
    }

    @SuppressWarnings("unchecked")
    private static String getProperty(Map<String, ?> properties, List<String> propertyPath) {
        for (int i = 0; i < propertyPath.size() - 1; i++) {
            Object o = properties.get(propertyPath.get(i));
            assertThat(o, instanceOf(Map.class));
            properties = (Map<String, Object>) o;
        }
        return String.valueOf(properties.get(propertyPath.get(propertyPath.size() - 1)));
    }

    private void upgradeSystemIndices() throws Exception {
        String upgradeUser = "upgrade_user";
        String upgradeUserPassword = "x-pack-test-password";
        createRole("upgrade_role");
        createUser(upgradeUser, upgradeUserPassword, "upgrade_role");

        try (RestClient upgradeUserClient = getClient(upgradeUser, upgradeUserPassword)) {
            boolean upgradeRequired = Version.fromString(UPGRADE_FROM_VERSION).before(SystemIndices.NO_UPGRADE_REQUIRED_VERSION);
            String expectedStatus = (upgradeRequired) ? "MIGRATION_NEEDED" : "NO_MIGRATION_NEEDED";

            assertThat(
                XContentHelper.convertToMap(
                    JsonXContent.jsonXContent,
                    upgradeUserClient.performRequest(new Request("GET", "/_migration/system_features")).getEntity().getContent(),
                    false
                ).get("migration_status"),
                equalTo(expectedStatus)
            );

            if (upgradeRequired) {
                Request upgradeRequest = new Request("POST", "/_migration/system_features");
                Response upgradeResponse = upgradeUserClient.performRequest(upgradeRequest);
                assertOK(upgradeResponse);
                assertBusy(() -> {
                    Response featureResponse = upgradeUserClient.performRequest(new Request("GET", "/_migration/system_features"));
                    assertThat(
                        XContentHelper.convertToMap(JsonXContent.jsonXContent, featureResponse.getEntity().getContent(), false)
                            .get("migration_status"),
                        equalTo("NO_MIGRATION_NEEDED")
                    );
                }, 30, TimeUnit.SECONDS);
            }
        }
    }

    private void createUser(String name, String password, String role) throws IOException {
        Request request = new Request("PUT", "/_security/user/" + name);
        request.setJsonEntity("{ \"password\": \"" + password + "\", \"roles\": [ \"" + role + "\"] }");
        assertOK(adminClient().performRequest(request));
    }

    private void createRole(String name) throws IOException {
        Request request = new Request("PUT", "/_security/role/" + name);
        request.setJsonEntity(
            "{ \"cluster\": [\"cluster:admin/migration/post_system_feature\", \"cluster:admin/migration/get_system_feature\"] }"
        );
        assertOK(adminClient().performRequest(request));
    }

    private RestClient getClient(String user, String passwd) throws IOException {
        RestClientBuilder builder = RestClient.builder(adminClient().getNodes().toArray(new Node[0]));
        String token = basicAuthHeaderValue(user, new SecureString(passwd.toCharArray()));
        configureClient(builder, Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build());
        builder.setStrictDeprecationMode(true);
        return builder.build();
    }

    private Map<String, Map<String, Object>> getIndexMetadata(String aliasName) throws IOException {
        Map<String, Map<String, Map<String, Object>>> metadata = getMetadata(aliasName);
        return metadata.get("indices");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Map<String, Map<String, Object>>> getMetadata(String dataStreamOrAlias) throws IOException {
        Request getClusterStateRequest = new Request("GET", "/_cluster/state/metadata/" + dataStreamOrAlias);
        Response clusterStateResponse = client().performRequest(getClusterStateRequest);
        Map<String, Object> clusterState = XContentHelper.convertToMap(
            JsonXContent.jsonXContent,
            clusterStateResponse.getEntity().getContent(),
            false
        );
        return (Map<String, Map<String, Map<String, Object>>>) clusterState.get("metadata");
    }
}
