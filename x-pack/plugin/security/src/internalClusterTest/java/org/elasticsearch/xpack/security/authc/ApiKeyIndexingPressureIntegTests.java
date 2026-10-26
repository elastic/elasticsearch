/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequestBuilder;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.SecuritySettingsSource.ES_TEST_ROOT_USER;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies that the security context a request carries for its lifetime counts toward indexing back-pressure. With a coordinating
 * limit below the size of a large API key's role descriptors, yet ample for a plain user, the same tiny bulk request is rejected
 * for the API key and accepted for the user.
 */
public class ApiKeyIndexingPressureIntegTests extends SecurityIntegTestCase {

    private static final ByteSizeValue COORDINATING_LIMIT = ByteSizeValue.ofKb(32);

    @Override
    protected boolean addMockHttpTransport() {
        return false; // the bulk requests go through the real REST layer
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(IndexingPressure.MAX_COORDINATING_BYTES.getKey(), COORDINATING_LIMIT)
            .build();
    }

    public void testBulkFromApiKeyWithLargeRoleDescriptorsIsRejected() throws Exception {
        final String rootUserAuthorization = basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING);
        final CreateApiKeyResponse apiKey = new CreateApiKeyRequestBuilder(
            client().filterWithHeader(Map.of("Authorization", rootUserAuthorization))
        ).setName("large-role-descriptors")
            .setRoleDescriptors(List.of(largeRoleDescriptor()))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        final String apiKeyAuthorization = "ApiKey "
            + Base64.getEncoder()
                .encodeToString((apiKey.getId() + ":" + new String(apiKey.getKey().getChars())).getBytes(StandardCharsets.UTF_8));

        final ResponseException e = expectThrows(
            ResponseException.class,
            () -> getRestClient().performRequest(bulkRequest(apiKeyAuthorization))
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.TOO_MANY_REQUESTS.getStatus()));
        assertThat(e.getMessage(), containsString("rejected execution of coordinating operation"));

        final Response response = getRestClient().performRequest(bulkRequest(rootUserAuthorization));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    }

    private static Request bulkRequest(String authorization) {
        final Request request = new Request("POST", "/_bulk");
        request.setJsonEntity("{\"index\":{\"_index\":\"idx\"}}\n{\"field\":\"value\"}\n");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", authorization));
        return request;
    }

    /**
     * A role descriptor whose serialized form alone comfortably exceeds the coordinating limit, padded with many long index
     * patterns. It also grants what the bulk request needs.
     */
    private static RoleDescriptor largeRoleDescriptor() {
        final List<RoleDescriptor.IndicesPrivileges> indicesPrivileges = new ArrayList<>();
        indicesPrivileges.add(RoleDescriptor.IndicesPrivileges.builder().indices("idx").privileges("all").build());
        for (int i = 0; i < 100; i++) {
            indicesPrivileges.add(
                RoleDescriptor.IndicesPrivileges.builder().indices("pattern-" + i + "-" + "x".repeat(200)).privileges("read").build()
            );
        }
        return new RoleDescriptor(
            "large",
            new String[] { "monitor" },
            indicesPrivileges.toArray(RoleDescriptor.IndicesPrivileges[]::new),
            null
        );
    }
}
