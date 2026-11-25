/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Strings;
import org.elasticsearch.rest.RestStatus;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * auto-migration of deprecated
 * {@code pivot.max_page_search_size} must preserve stored creator security headers so
 * document-level security continues to apply when the transform runs.
 */
public class TransformAutoMigrationDlsIT extends TransformRestTestCase {

    private static final String DLS_USER = "transform_migrate_dls_user";
    private static final String DLS_ROLE = "transform_migrate_dls_role";
    private static final String BASIC_AUTH_DLS_USER = basicAuthHeaderValue(DLS_USER, TEST_PASSWORD_SECURE_STRING);

    private static final String SOURCE_INDEX = "transform-migrate-dls-src";
    private static final String DEST_INDEX = "transform-migrate-dls-dest";
    private static final String TRANSFORM_ID = "transform-migrate-dls-tf";

    @Override
    protected boolean enableWarningsCheck() {
        // Deprecated pivot.max_page_search_size triggers a deprecation warning on PUT.
        return false;
    }

    @Override
    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        RestClientBuilder builder = RestClient.builder(hosts);
        configureClient(builder, settings);
        builder.setStrictDeprecationMode(false);
        return builder.build();
    }

    @Before
    public void setUpDlsFixture() throws IOException {
        createSourceIndex();
        setupDlsRole(DLS_ROLE, SOURCE_INDEX, DEST_INDEX);
        setupUser(DLS_USER, List.of(DLS_ROLE));
    }

    @After
    public void tearDownDlsFixture() throws IOException {
        // delete_dest_index uses the transform's stored headers (DLS user), who lacks delete_index.
        deleteTransform(TRANSFORM_ID, true, false);
        deleteUser(DLS_USER);
        deleteRole(DLS_ROLE);
    }

    public void testPutAutoMigrationPreservesHeadersAndHonorsDls() throws Exception {
        // Negative control: DLS user cannot read the secret tenant from the source.
        Map<String, Object> sourceSearch = searchAs(DLS_USER, SOURCE_INDEX);
        assertThat(XContentMapValues.extractValue("hits.total.value", sourceSearch), equalTo(1));
        assertThat(extractTenants(sourceSearch), contains("public"));

        Request putRequest = createRequestWithAuth("PUT", getTransformEndpoint() + TRANSFORM_ID, BASIC_AUTH_DLS_USER);
        putRequest.setJsonEntity(Strings.format("""
            {
              "source": { "index": "%s" },
              "dest": { "index": "%s" },
              "pivot": {
                "group_by": {
                  "tenant": { "terms": { "field": "tenant" } }
                },
                "aggregations": {
                  "max_amount": { "max": { "field": "amount" } }
                },
                "max_page_search_size": 10
              },
              "settings": { "deduce_mappings": false }
            }""", SOURCE_INDEX, DEST_INDEX));
        Map<String, Object> putResponse = entityAsMap(client().performRequest(putRequest));
        assertThat(putResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        // After PUT-time auto-migration, creator authorization must still be present.
        Map<String, Object> transformConfig = getTransformConfig(TRANSFORM_ID, null);
        assertThat(transformConfig.get("authorization"), notNullValue());
        assertNull(XContentMapValues.extractValue("pivot.max_page_search_size", transformConfig));
        assertThat(XContentMapValues.extractValue("settings.max_page_search_size", transformConfig), equalTo(10));

        startAndWaitForTransform(TRANSFORM_ID, DEST_INDEX, BASIC_AUTH_DLS_USER);

        // Destination must only materialize the public tenant — not the DLS-hidden secret.
        Map<String, Object> destSearch = searchAs(DLS_USER, DEST_INDEX);
        assertThat(XContentMapValues.extractValue("hits.total.value", destSearch), equalTo(1));
        assertThat(extractTenants(destSearch), contains("public"));
        assertThat(extractTenants(destSearch), not(contains("secret")));
    }

    private void createSourceIndex() throws IOException {
        Request createIndex = new Request("PUT", "/" + SOURCE_INDEX);
        createIndex.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "tenant": { "type": "keyword" },
                  "amount": { "type": "double" }
                }
              }
            }""");
        client().performRequest(createIndex);

        Request bulk = new Request("POST", "/_bulk?refresh=true");
        bulk.setJsonEntity(Strings.format("""
            {"index":{"_index":"%s"}}
            {"tenant":"public","amount":1.0}
            {"index":{"_index":"%s"}}
            {"tenant":"secret","amount":999.0}
            """, SOURCE_INDEX, SOURCE_INDEX));
        client().performRequest(bulk);
    }

    private void setupDlsRole(String role, String sourceIndex, String destIndex) throws IOException {
        Request request = new Request("PUT", "/_security/role/" + role);
        // Escaped JSON string form matches the security role API's DLS query field.
        request.setJsonEntity(Strings.format("""
            {
              "cluster": [ "manage_transform" ],
              "indices": [
                {
                  "names": [ "%s" ],
                  "privileges": [ "read", "view_index_metadata" ],
                  "query": "{\\"term\\":{\\"tenant\\":\\"public\\"}}"
                },
                {
                  "names": [ "%s" ],
                  "privileges": [ "read", "view_index_metadata", "create_index", "create", "index", "write" ]
                }
              ]
            }""", sourceIndex, destIndex));
        client().performRequest(request);
    }

    private Map<String, Object> searchAs(String user, String index) throws IOException {
        String auth = basicAuthHeaderValue(user, TEST_PASSWORD_SECURE_STRING);
        Request request = createRequestWithAuth("GET", "/" + index + "/_search", auth);
        // No sort: with deduce_mappings=false the dest maps group_by fields as text.
        request.setJsonEntity("""
            { "size": 10 }""");
        return entityAsMap(client().performRequest(request));
    }

    @SuppressWarnings("unchecked")
    private static List<String> extractTenants(Map<String, Object> searchResult) {
        List<Map<String, Object>> hits = (List<Map<String, Object>>) XContentMapValues.extractValue("hits.hits", searchResult);
        return hits.stream().map(hit -> {
            Map<String, Object> source = (Map<String, Object>) hit.get("_source");
            return (String) source.get("tenant");
        }).collect(Collectors.toList());
    }

    private void deleteUser(String user) throws IOException {
        Request request = new Request("DELETE", "/_security/user/" + user);
        setIgnoredErrorResponseCodes(request, RestStatus.NOT_FOUND);
        client().performRequest(request);
    }

    private void deleteRole(String role) throws IOException {
        Request request = new Request("DELETE", "/_security/role/" + role);
        setIgnoredErrorResponseCodes(request, RestStatus.NOT_FOUND);
        client().performRequest(request);
    }
}
