/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class ApiKeyBackwardsCompatibilityIT extends AbstractUpgradeTestCase {

    public static final Version API_KEY_SUPPORT_REMOTE_INDICES_VERSION = Version.V_8_8_0;
    private static final String REMOTE_CLUSTER_ALIAS = "my_remote_cluster";

    private RestClient oldVersionClient = null;
    private RestClient newVersionClient = null;

    public void testCreatingAndUpdatingApiKeys() throws Exception {
        assumeTrue(
            "The remote_indices for API Keys are not supported before version " + API_KEY_SUPPORT_REMOTE_INDICES_VERSION,
            UPGRADE_FROM_VERSION.before(API_KEY_SUPPORT_REMOTE_INDICES_VERSION)
        );
        switch (CLUSTER_TYPE) {
            case OLD -> {
                // succeed when remote_indices are not provided
                boolean includeRoles = randomBoolean();
                Tuple<String, String> apiKey = createApiKey(includeRoles ? randomRoleDescriptors(false) : "{}");
                authenticateWithApiKey(apiKey.v1(), apiKey.v2());

                // fail if we include remote_indices
                ResponseException e = expectThrows(ResponseException.class, () -> createApiKey(randomRoleDescriptors(true)));
                assertThat(e.getMessage(), containsString("failed to parse role [my_role]. unexpected field [remote_indices]"));
            }
            case MIXED -> {
                try {
                    this.createClientsByVersion();
                    // succeed when remote_indices are not provided
                    boolean includeRoles = randomBoolean();
                    Tuple<String, String> apiKey = createApiKey(includeRoles ? randomRoleDescriptors(false) : "{}");
                    authenticateWithApiKey(oldVersionClient, apiKey.v1(), apiKey.v2());
                    authenticateWithApiKey(newVersionClient, apiKey.v1(), apiKey.v2());

                    // fail when remote_indices are provided:
                    // against old node
                    ResponseException e = expectThrows(
                        ResponseException.class,
                        () -> createApiKey(oldVersionClient, randomRoleDescriptors(true))
                    );
                    assertThat(e.getMessage(), containsString("failed to parse role [my_role]. unexpected field [remote_indices]"));

                    // and against new node
                    e = expectThrows(ResponseException.class, () -> createApiKey(newVersionClient, randomRoleDescriptors(true)));
                    assertThat(
                        e.getMessage(),
                        containsString("all nodes must have version [8060099] or higher to support remote indices privileges")
                    );
                } finally {
                    this.closeClientsByVersion();
                }
            }
            case UPGRADED -> {
                // succeed either way
                boolean includeRoles = randomBoolean();
                Tuple<String, String> apiKey = createApiKey(includeRoles ? randomRoleDescriptors(false) : "{}");
                authenticateWithApiKey(apiKey.v1(), apiKey.v2());

                Tuple<String, String> apiKeyWithRemoteIndices = createApiKey(randomRoleDescriptors(true));
                authenticateWithApiKey(apiKeyWithRemoteIndices.v1(), apiKeyWithRemoteIndices.v2());
            }
        }
    }

    private Tuple<String, String> createApiKey(String roles) throws IOException {
        return createApiKey(client(), roles);
    }

    private Tuple<String, String> createApiKey(RestClient client, String roles) throws IOException {
        final String name = "test-api-key-" + randomAlphaOfLengthBetween(3, 5);
        final Request createApiKeyRequest = new Request("POST", "/_security/api_key");
        createApiKeyRequest.setJsonEntity(Strings.format("""
            {
                "name": "%s",
                "role_descriptors": %s
            }""", name, roles));
        Response response = client.performRequest(createApiKeyRequest);
        assertOK(response);
        final ObjectPath path = ObjectPath.createFromResponse(response);
        final String id = path.evaluate("id");
        final String key = path.evaluate("api_key");
        assertThat(id, notNullValue());
        assertThat(key, notNullValue());
        return Tuple.tuple(id, key);
    }

    private Map<String, Object> authenticateWithApiKey(String id, String key) throws IOException {
        return authenticateWithApiKey(client(), id, key);
    }

    private Map<String, Object> authenticateWithApiKey(RestClient client, String id, String key) throws IOException {
        final Request request = new Request(HttpGet.METHOD_NAME, "/_security/_authenticate");
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", getApiKeyAuthorizationHeaderValue(id, key)).build()
        );
        final Response response = client.performRequest(request);
        assertOK(response);
        ObjectPath path = ObjectPath.createFromResponse(response);
        final String authenticationTypeString = path.evaluate(User.Fields.AUTHENTICATION_TYPE.getPreferredName());
        final Authentication.AuthenticationType authenticationType = Authentication.AuthenticationType.valueOf(
            authenticationTypeString.toUpperCase(Locale.ROOT)
        );
        assertThat(authenticationType, is(Authentication.AuthenticationType.API_KEY));
        assertThat(path.evaluate("api_key.id"), is(id));
        return responseAsMap(response);
    }

    private String getApiKeyAuthorizationHeaderValue(String id, String key) {
        return "ApiKey " + Base64.getEncoder().encodeToString((id + ":" + key).getBytes(StandardCharsets.UTF_8));
    }

    private static String randomRoleDescriptors(boolean includeRemoteIndices) throws IOException {
        Map<String, Object> myRole = new HashMap<>();
        myRole.put("cluster", List.of("all"));
        myRole.put(
            "indices",
            List.of(Map.ofEntries(Map.entry("names", List.of("test-*")), Map.entry("privileges", List.of("read", "read_cross_cluster"))))
        );
        if (includeRemoteIndices) {
            myRole.put(
                "remote_indices",
                List.of(
                    Map.ofEntries(
                        Map.entry("names", List.of("test-*")),
                        Map.entry("privileges", List.of("read", "read_cross_cluster")),
                        Map.entry("clusters", List.of(REMOTE_CLUSTER_ALIAS))
                    )
                )
            );
        }
        return XContentTestUtils.convertToXContent(Map.of("my_role", myRole), XContentType.JSON).utf8ToString();
    }

    private void createClientsByVersion() throws IOException {
        Map<Version, RestClient> clientsByVersion = getRestClientByVersion();
        if (clientsByVersion.size() == 2) {
            for (Map.Entry<Version, RestClient> client : clientsByVersion.entrySet()) {
                if (client.getKey().before(API_KEY_SUPPORT_REMOTE_INDICES_VERSION)) {
                    oldVersionClient = client.getValue();
                } else {
                    newVersionClient = client.getValue();
                }
            }
            assertThat(oldVersionClient, notNullValue());
            assertThat(newVersionClient, notNullValue());
        } else {
            fail("expected 2 versions during rolling upgrade but got: " + clientsByVersion.size());
        }
    }

    private void closeClientsByVersion() throws IOException {
        if (oldVersionClient != null) {
            oldVersionClient.close();
            oldVersionClient = null;
        }
        if (newVersionClient != null) {
            newVersionClient.close();
            newVersionClient = null;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<Version, RestClient> getRestClientByVersion() throws IOException {
        Response response = client().performRequest(new Request("GET", "_nodes"));
        assertOK(response);
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        Map<String, Object> nodesAsMap = objectPath.evaluate("nodes");
        Map<Version, List<HttpHost>> hostsByVersion = new HashMap<>();
        for (Map.Entry<String, Object> entry : nodesAsMap.entrySet()) {
            Map<String, Object> nodeDetails = (Map<String, Object>) entry.getValue();
            Version version = Version.fromString((String) nodeDetails.get("version"));
            Map<String, Object> httpInfo = (Map<String, Object>) nodeDetails.get("http");
            hostsByVersion.computeIfAbsent(version, k -> new ArrayList<>()).add(HttpHost.create((String) httpInfo.get("publish_address")));
        }
        Map<Version, RestClient> clientsByVersion = new HashMap<>();
        for (Map.Entry<Version, List<HttpHost>> entry : hostsByVersion.entrySet()) {
            clientsByVersion.put(entry.getKey(), buildClient(restClientSettings(), entry.getValue().toArray(new HttpHost[0])));
        }
        return clientsByVersion;
    }
}
