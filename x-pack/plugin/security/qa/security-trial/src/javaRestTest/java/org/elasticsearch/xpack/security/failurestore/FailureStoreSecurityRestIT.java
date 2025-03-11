/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.failurestore;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.TestSecurityClient;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class FailureStoreSecurityRestIT extends ESRestTestCase {

    private TestSecurityClient securityClient;

    private Map<String, String> apiKeys = new HashMap<>();

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .apply(SecurityOnTrialLicenseRestTestCase.commonTrialSecurityClusterConfig)
        .feature(FeatureFlag.FAILURE_STORE_ENABLED)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("admin_user", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private static final String ASYNC_SEARCH_TIMEOUT = "30s";

    private static final String DATA_ACCESS = "data_access";
    private static final String STAR_READ_ONLY_ACCESS = "star_read_only";
    private static final String FAILURE_STORE_ACCESS = "failure_store_access";
    private static final String BOTH_ACCESS = "both_access";
    private static final String WRITE_ACCESS = "write_access";
    private static final String MANAGE_ACCESS = "manage_access";
    private static final String MANAGE_FAILURE_STORE_ACCESS = "manage_failure_store_access";
    private static final SecureString PASSWORD = new SecureString("elastic-password");

    public void testGetUserPrivileges() throws IOException {
        Request userRequest = new Request("PUT", "/_security/user/user");
        userRequest.setJsonEntity("""
            {
              "password": "x-pack-test-password",
              "roles": ["role"]
            }
            """);
        assertOK(adminClient().performRequest(userRequest));

        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["read", "read_failure_store"]
                }
              ]
            }
            """, "role");
        expectUserPrivilegesResponse("""
            {
              "cluster": ["all"],
              "global": [],
              "indices": [{
                  "names": ["*"],
                  "privileges": ["read"],
                  "allow_restricted_indices": false
                },
                {
                  "names": ["*"],
                  "privileges": ["read_failure_store"],
                  "allow_restricted_indices": false
                }],
              "applications": [],
              "run_as": []
            }""");

        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["read_failure_store"]
                }
              ]
            }
            """, "role");
        expectUserPrivilegesResponse("""
            {
              "cluster": ["all"],
              "global": [],
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["read_failure_store"],
                  "allow_restricted_indices": false
                }],
              "applications": [],
              "run_as": []
            }""");

        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["all", "read_failure_store"]
                }
              ]
            }
            """, "role");
        expectUserPrivilegesResponse("""
            {
              "cluster": ["all"],
              "global": [],
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["all", "read_failure_store"],
                  "allow_restricted_indices": false
                }],
              "applications": [],
              "run_as": []
            }""");
    }

    @SuppressWarnings("unchecked")
    public void testFailureStoreAccess() throws Exception {
        apiKeys = new HashMap<>();

        createUser(DATA_ACCESS, PASSWORD, DATA_ACCESS);
        upsertRole(Strings.format("""
            {
              "cluster": ["all"],
              "indices": [{"names": ["test*"], "privileges": ["read"]}]
            }"""), DATA_ACCESS);
        createAndStoreApiKey(DATA_ACCESS, randomBoolean() ? null : """
            {
              "role": {
                "cluster": ["all"],
                "indices": [{"names": ["test*"], "privileges": ["read"]}]
              }
            }
            """);

        createUser(STAR_READ_ONLY_ACCESS, PASSWORD, STAR_READ_ONLY_ACCESS);
        upsertRole(Strings.format("""
            {
              "cluster": ["all"],
              "indices": [{"names": ["*"], "privileges": ["read"]}]
            }"""), STAR_READ_ONLY_ACCESS);
        createAndStoreApiKey(STAR_READ_ONLY_ACCESS, randomBoolean() ? null : """
            {
              "role": {
                "cluster": ["all"],
                "indices": [{"names": ["*"], "privileges": ["read"]}]
              }
            }
            """);

        createUser(FAILURE_STORE_ACCESS, PASSWORD, FAILURE_STORE_ACCESS);
        upsertRole(Strings.format("""
            {
              "cluster": ["all"],
              "indices": [{"names": ["test*"], "privileges": ["read_failure_store"]}]
            }"""), FAILURE_STORE_ACCESS);
        createAndStoreApiKey(FAILURE_STORE_ACCESS, randomBoolean() ? null : """
            {
              "role": {
                "cluster": ["all"],
                "indices": [{"names": ["test*"], "privileges": ["read_failure_store"]}]
              }
            }
            """);

        if (randomBoolean()) {
            createUser(BOTH_ACCESS, PASSWORD, BOTH_ACCESS);
            upsertRole(Strings.format("""
                {
                  "cluster": ["all"],
                  "indices": [{"names": ["test*"], "privileges": ["read", "read_failure_store"]}]
                }"""), BOTH_ACCESS);
            createAndStoreApiKey(BOTH_ACCESS, randomBoolean() ? null : """
                {
                  "role": {
                    "cluster": ["all"],
                    "indices": [{"names": ["test*"], "privileges": ["read", "read_failure_store"]}]
                  }
                }
                """);
        } else {
            createUser(BOTH_ACCESS, PASSWORD, DATA_ACCESS, FAILURE_STORE_ACCESS);
            createAndStoreApiKey(BOTH_ACCESS, randomBoolean() ? null : """
                {
                  "role": {
                    "cluster": ["all"],
                    "indices": [{"names": ["test*"], "privileges": ["read", "read_failure_store"]}]
                  }
                }
                """);
        }

        createUser(WRITE_ACCESS, PASSWORD, WRITE_ACCESS);
        upsertRole(Strings.format("""
            {
              "cluster": ["all"],
              "indices": [{"names": ["test*"], "privileges": ["write", "auto_configure"]}]
            }"""), WRITE_ACCESS);
        createAndStoreApiKey(WRITE_ACCESS, randomBoolean() ? null : """
            {
              "role": {
                "cluster": ["all"],
                "indices": [{"names": ["test*"], "privileges": ["write", "auto_configure"]}]
              }
            }
            """);

        createUser(MANAGE_ACCESS, PASSWORD, MANAGE_ACCESS);
        upsertRole(Strings.format("""
            {
              "cluster": ["all"],
              "indices": [{"names": ["test*"], "privileges": ["manage"]}]
            }"""), MANAGE_ACCESS);
        createAndStoreApiKey(MANAGE_ACCESS, randomBoolean() ? null : """
            {
              "role": {
                "cluster": ["all"],
                "indices": [{"names": ["test*"], "privileges": ["manage"]}]
              }
            }
            """);

        createUser(MANAGE_FAILURE_STORE_ACCESS, PASSWORD, MANAGE_FAILURE_STORE_ACCESS);
        upsertRole(Strings.format("""
            {
              "cluster": ["all"],
              "indices": [{"names": ["test*"], "privileges": ["manage_failure_store"]}]
            }"""), MANAGE_FAILURE_STORE_ACCESS);
        createAndStoreApiKey(MANAGE_FAILURE_STORE_ACCESS, randomBoolean() ? null : """
            {
              "role": {
                "cluster": ["all"],
                "indices": [{"names": ["test*"], "privileges": ["manage_failure_store"]}]
              }
            }
            """);

        createTemplates();

        List<String> docIds = populateDataStream();
        assertThat(docIds.size(), equalTo(2));
        assertThat(docIds, hasItem("1"));
        String dataDocId = "1";
        String failuresDocId = docIds.stream().filter(id -> false == id.equals(dataDocId)).findFirst().get();

        Request dataStream = new Request("GET", "/_data_stream/test1");
        Response response = adminClient().performRequest(dataStream);
        Map<String, Object> dataStreams = entityAsMap(response);
        assertEquals(Collections.singletonList("test1"), XContentMapValues.extractValue("data_streams.name", dataStreams));
        List<String> dataIndexNames = (List<String>) XContentMapValues.extractValue("data_streams.indices.index_name", dataStreams);
        assertThat(dataIndexNames.size(), equalTo(1));
        List<String> failureIndexNames = (List<String>) XContentMapValues.extractValue(
            "data_streams.failure_store.indices.index_name",
            dataStreams
        );
        assertThat(failureIndexNames.size(), equalTo(1));

        String dataIndexName = dataIndexNames.get(0);
        String failureIndexName = failureIndexNames.get(0);

        Request aliasRequest = new Request("POST", "/_aliases");
        aliasRequest.setJsonEntity("""
            {
              "actions": [
                {
                  "add": {
                    "index": "test1",
                    "alias": "test-alias"
                  }
                }
              ]
            }
            """);
        assertOK(adminClient().performRequest(aliasRequest));

        // todo also add superuser
        List<String> users = List.of(DATA_ACCESS, FAILURE_STORE_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS);

        // search data
        {
            var request = new Search("test1");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expect(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS:
                        expect(user, request, 403);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test1", "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expect(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS:
                        expect(user, request);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test-alias");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expect(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS:
                        expect(user, request, 403);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test-alias", "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expect(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS:
                        expect(user, request);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test*");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expect(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS:
                        expect(user, request);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("*1");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expect(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS:
                        expect(user, request);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("*");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expect(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS:
                        expect(user, request);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search(".ds*");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expect(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS:
                        expect(user, request);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search(dataIndexName);
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expect(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS:
                        expect(user, request, 403);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search(dataIndexName, "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expect(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS:
                        expect(user, request);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test2");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expect(user, request, 404);
                        break;
                    case FAILURE_STORE_ACCESS:
                        expect(user, request, 403);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test2", "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS, FAILURE_STORE_ACCESS:
                        expect(user, request);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }

        // search failures
        {
            var request = new Search("test1::failures");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS:
                        expect(user, request, 403);
                        break;
                    case FAILURE_STORE_ACCESS, BOTH_ACCESS:
                        expect(user, request, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test1::failures", "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS:
                        expect(user, request);
                        break;
                    case FAILURE_STORE_ACCESS, BOTH_ACCESS:
                        expect(user, request, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test-alias::failures");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS:
                        expect(user, request, 403);
                        break;
                    case FAILURE_STORE_ACCESS, BOTH_ACCESS:
                        expect(user, request, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test-alias::failures", "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS:
                        expect(user, request);
                        break;
                    case FAILURE_STORE_ACCESS, BOTH_ACCESS:
                        expect(user, request, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test*::failures");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS:
                        expect(user, request);
                        break;
                    case FAILURE_STORE_ACCESS, BOTH_ACCESS:
                        expect(user, request, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("*1::failures");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS:
                        expect(user, request);
                        break;
                    case FAILURE_STORE_ACCESS, BOTH_ACCESS:
                        expect(user, request, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("*::failures");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS:
                        expect(user, request);
                        break;
                    case FAILURE_STORE_ACCESS, BOTH_ACCESS:
                        expect(user, request, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search(".fs*");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS:
                        expect(user, request);
                        break;
                    case FAILURE_STORE_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expect(user, request, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search(failureIndexName);
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS:
                        expect(user, request, 403);
                        break;
                    case FAILURE_STORE_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expect(user, request, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search(failureIndexName, "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS:
                        expect(user, request);
                        break;
                    case FAILURE_STORE_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expect(user, request, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test2::failures");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS:
                        expect(user, request, 403);
                        break;
                    case FAILURE_STORE_ACCESS, BOTH_ACCESS:
                        expect(user, request, 404);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test2::failures", "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS, FAILURE_STORE_ACCESS:
                        expect(user, request);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }

        // mixed access
        {
            var request = new Search("test1,test1::failures");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, FAILURE_STORE_ACCESS, STAR_READ_ONLY_ACCESS:
                        expect(user, request, 403);
                        break;
                    case BOTH_ACCESS:
                        expect(user, request, dataDocId, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test1,test1::failures", "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS:
                        expect(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS:
                        expect(user, request, failuresDocId);
                        break;
                    case BOTH_ACCESS:
                        expect(user, request, dataDocId, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test1," + failureIndexName);
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, FAILURE_STORE_ACCESS:
                        expect(user, request, 403);
                        break;
                    case BOTH_ACCESS, STAR_READ_ONLY_ACCESS:
                        expect(user, request, dataDocId, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test1," + failureIndexName, "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS:
                        expect(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS:
                        expect(user, request, failuresDocId);
                        break;
                    case BOTH_ACCESS, STAR_READ_ONLY_ACCESS:
                        expect(user, request, dataDocId, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test1::failures," + dataIndexName);
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, FAILURE_STORE_ACCESS, STAR_READ_ONLY_ACCESS:
                        expect(user, request, 403);
                        break;
                    case BOTH_ACCESS:
                        expect(user, request, dataDocId, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test1::failures," + dataIndexName, "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS:
                        expect(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS:
                        expect(user, request, failuresDocId);
                        break;
                    case BOTH_ACCESS:
                        expect(user, request, dataDocId, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test1,*::failures");
            for (var user : users) {
                switch (user) {
                    case FAILURE_STORE_ACCESS:
                        expect(user, request, 403);
                        break;
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS:
                        expect(user, request, dataDocId);
                        break;
                    case BOTH_ACCESS:
                        expect(user, request, dataDocId, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test1,*::failures", "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case FAILURE_STORE_ACCESS:
                        expect(user, request, failuresDocId);
                        break;
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS:
                        expect(user, request, dataDocId);
                        break;
                    case BOTH_ACCESS:
                        expect(user, request, dataDocId, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test1::failures,*");
            for (var user : users) {
                switch (user) {
                    case FAILURE_STORE_ACCESS:
                        expect(user, request, failuresDocId);
                        break;
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS:
                        expect(user, request, 403);
                        break;
                    case BOTH_ACCESS:
                        expect(user, request, dataDocId, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test1::failures,*", "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case FAILURE_STORE_ACCESS:
                        expect(user, request, failuresDocId);
                        break;
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS:
                        expect(user, request, dataDocId);
                        break;
                    case BOTH_ACCESS:
                        expect(user, request, dataDocId, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("*::failures,*");
            for (var user : users) {
                switch (user) {
                    case FAILURE_STORE_ACCESS:
                        expect(user, request, failuresDocId);
                        break;
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS:
                        expect(user, request, dataDocId);
                        break;
                    case BOTH_ACCESS:
                        expect(user, request, dataDocId, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }

        // write operations below

        // user with manage access to data stream does NOT get direct access to failure index
        expectThrows403(() -> deleteIndex(MANAGE_ACCESS, failureIndexName));
        expectThrows(() -> deleteIndex(MANAGE_ACCESS, dataIndexName), 400);
        // manage_failure_store user COULD delete failure index (not valid because it's a write index, but allow security-wise)
        expectThrows403(() -> deleteIndex(MANAGE_FAILURE_STORE_ACCESS, dataIndexName));
        expectThrows(() -> deleteIndex(MANAGE_FAILURE_STORE_ACCESS, failureIndexName), 400);
        expectThrows403(() -> deleteDataStream(MANAGE_FAILURE_STORE_ACCESS, dataIndexName));

        expectThrows(() -> deleteDataStream(MANAGE_FAILURE_STORE_ACCESS, "test1"), 403);
        expectThrows(() -> deleteDataStream(MANAGE_FAILURE_STORE_ACCESS, "test1::failures"), 403);
        // manage user can delete data stream
        deleteDataStream(MANAGE_ACCESS, "test1");

        expectThrows404(() -> performRequest(BOTH_ACCESS, new Request("GET", "/test1/_search")));
        expectThrows404(() -> adminClient().performRequest(new Request("GET", "/" + dataIndexName + "/_search")));
        expectThrows404(() -> performRequest(BOTH_ACCESS, new Request("GET", "/test1::failures/_search")));
        expectThrows404(() -> adminClient().performRequest(new Request("GET", "/" + failureIndexName + "/_search")));
    }

    private static void expectThrows404(ThrowingRunnable runnable) {
        expectThrows(runnable, 404);
    }

    private static void expectThrows403(ThrowingRunnable runnable) {
        expectThrows(runnable, 403);
    }

    private static void expectThrows(ThrowingRunnable runnable, int statusCode) {
        var ex = expectThrows(ResponseException.class, runnable);
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(statusCode));
    }

    private void expect(String user, Search search, int statusCode) {
        expectThrows(() -> performRequest(user, search.toSearchRequest()), statusCode);
        expectThrows(() -> performRequest(user, search.toAsyncSearchRequest()), statusCode);
    }

    private void expect(String user, Search search, String... docIds) throws Exception {
        expectSearch(user, search.toSearchRequest(), docIds);
        expectAsyncSearch(user, search.toAsyncSearchRequest(), docIds);
    }

    @SuppressWarnings("unchecked")
    private void expectAsyncSearch(String user, Request request, String... docIds) throws IOException {
        Response response = performRequestMaybeUsingApiKey(user, request);
        assertOK(response);
        ObjectPath resp = ObjectPath.createFromResponse(response);
        Boolean isRunning = resp.evaluate("is_running");
        Boolean isPartial = resp.evaluate("is_partial");
        assertThat(isRunning, is(false));
        assertThat(isPartial, is(false));

        List<Object> hits = resp.evaluate("response.hits.hits");
        List<String> actual = hits.stream().map(h -> (String) ((Map<String, Object>) h).get("_id")).toList();

        assertThat(actual, containsInAnyOrder(docIds));
    }

    private void expectSearch(String user, Request request, String... docIds) throws Exception {
        Response response = performRequestMaybeUsingApiKey(user, request);
        assertOK(response);
        final SearchResponse searchResponse = SearchResponseUtils.parseSearchResponse(responseAsParser(response));
        try {
            SearchHit[] hits = searchResponse.getHits().getHits();
            assertThat(hits.length, equalTo(docIds.length));
            List<String> actualDocIds = Arrays.stream(hits).map(SearchHit::getId).toList();
            assertThat(actualDocIds, containsInAnyOrder(docIds));
        } finally {
            searchResponse.decRef();
        }
    }

    private record Search(String searchTarget, String pathParamString) {
        Search(String searchTarget) {
            this(searchTarget, "");
        }

        Request toSearchRequest() {
            return new Request("POST", Strings.format("/%s/_search%s", searchTarget, pathParamString));
        }

        Request toAsyncSearchRequest() {
            var pathParam = pathParamString.isEmpty()
                ? "?wait_for_completion_timeout=" + ASYNC_SEARCH_TIMEOUT
                : pathParamString + "&wait_for_completion_timeout=" + ASYNC_SEARCH_TIMEOUT;
            return new Request("POST", Strings.format("/%s/_async_search%s", searchTarget, pathParam));
        }
    }

    private void createTemplates() throws IOException {
        var componentTemplateRequest = new Request("PUT", "/_component_template/component1");
        componentTemplateRequest.setJsonEntity("""
            {
                "template": {
                    "mappings": {
                        "properties": {
                            "@timestamp": {
                                "type": "date"
                            },
                            "age": {
                                "type": "integer"
                            },
                            "email": {
                                "type": "keyword"
                            },
                            "name": {
                                "type": "text"
                            }
                        }
                    },
                    "data_stream_options": {
                      "failure_store": {
                        "enabled": true
                      }
                    }
                }
            }
            """);
        assertOK(adminClient().performRequest(componentTemplateRequest));

        var indexTemplateRequest = new Request("PUT", "/_index_template/template1");
        indexTemplateRequest.setJsonEntity("""
            {
                "index_patterns": ["test*"],
                "data_stream": {},
                "priority": 500,
                "composed_of": ["component1"]
            }
            """);
        assertOK(adminClient().performRequest(indexTemplateRequest));
    }

    private List<String> populateDataStream() throws IOException {
        return randomBoolean() ? populateDataStreamWithBulkRequest() : populateDataStreamWithDocRequests();
    }

    private List<String> populateDataStreamWithDocRequests() throws IOException {
        List<String> ids = new ArrayList<>();

        var dataStreamName = "test1";
        var docRequest = new Request("PUT", "/" + dataStreamName + "/_doc/1?refresh=true&op_type=create");
        docRequest.setJsonEntity("""
            {
               "@timestamp": 1,
               "age" : 1,
               "name" : "jack",
               "email" : "jack@example.com"
            }
            """);
        Response response = performRequest(WRITE_ACCESS, docRequest);
        assertOK(response);
        Map<String, Object> responseAsMap = responseAsMap(response);
        ids.add((String) responseAsMap.get("_id"));

        docRequest = new Request("PUT", "/" + dataStreamName + "/_doc/2?refresh=true&op_type=create");
        docRequest.setJsonEntity("""
            {
               "@timestamp": 2,
               "age" : "this should be an int",
               "name" : "jack",
               "email" : "jack@example.com"
            }
            """);
        response = performRequest(WRITE_ACCESS, docRequest);
        assertOK(response);
        responseAsMap = responseAsMap(response);
        ids.add((String) responseAsMap.get("_id"));

        return ids;
    }

    @SuppressWarnings("unchecked")
    private List<String> populateDataStreamWithBulkRequest() throws IOException {
        var bulkRequest = new Request("POST", "/_bulk?refresh=true");
        bulkRequest.setJsonEntity("""
            { "create" : { "_index" : "test1", "_id" : "1" } }
            { "@timestamp": 1, "age" : 1, "name" : "jack", "email" : "jack@example.com" }
            { "create" : { "_index" : "test1", "_id" : "2" } }
            { "@timestamp": 2, "age" : "this should be an int", "name" : "jack", "email" : "jack@example.com" }
            """);
        Response response = performRequest(WRITE_ACCESS, bulkRequest);
        assertOK(response);
        // we need this dance because the ID for the failed document is random, **not** 2
        Map<String, Object> stringObjectMap = responseAsMap(response);
        List<Object> items = (List<Object>) stringObjectMap.get("items");
        List<String> ids = new ArrayList<>();
        for (Object item : items) {
            Map<String, Object> itemMap = (Map<String, Object>) item;
            Map<String, Object> create = (Map<String, Object>) itemMap.get("create");
            assertThat(create.get("status"), equalTo(201));
            ids.add((String) create.get("_id"));
        }
        return ids;
    }

    private void deleteDataStream(String user, String dataStreamName) throws IOException {
        assertOK(performRequest(user, new Request("DELETE", "/_data_stream/" + dataStreamName)));
    }

    private void deleteIndex(String user, String indexName) throws IOException {
        assertOK(performRequest(user, new Request("DELETE", "/" + indexName)));
    }

    private Response performRequest(String user, Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(user, PASSWORD)).build());
        return client().performRequest(request);
    }

    private Response performRequestMaybeUsingApiKey(String user, Request request) throws IOException {
        if (randomBoolean()) {
            return performRequest(user, request);
        } else {
            String apiKey = apiKeys.get(user);
            assertThat("expected to have an API key stored for user: " + user, apiKey, is(notNullValue()));
            request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "ApiKey " + apiKey).build());
            return client().performRequest(request);
        }
    }

    private static void expectUserPrivilegesResponse(String userPrivilegesResponse) throws IOException {
        Request request = new Request("GET", "/_security/user/_privileges");
        request.setOptions(
            request.getOptions()
                .toBuilder()
                .addHeader("Authorization", basicAuthHeaderValue("user", new SecureString("x-pack-test-password".toCharArray())))
        );
        Response response = client().performRequest(request);
        assertOK(response);
        assertThat(responseAsMap(response), equalTo(mapFromJson(userPrivilegesResponse)));
    }

    private static Map<String, Object> mapFromJson(String json) {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);
    }

    protected TestSecurityClient getSecurityClient() {
        if (securityClient == null) {
            securityClient = new TestSecurityClient(adminClient());
        }
        return securityClient;
    }

    protected void createUser(String username, SecureString password, String... roles) throws IOException {
        getSecurityClient().putUser(new User(username, roles), password);
    }

    protected void createAndStoreApiKey(String username) throws IOException {
        createAndStoreApiKey(username, null);
    }

    protected void createAndStoreApiKey(String username, @Nullable String roleDescriptors) throws IOException {
        var request = new Request("POST", "/_security/api_key");
        if (roleDescriptors == null) {
            request.setJsonEntity("""
                {
                    "name": "test-api-key"
                }
                """);
        } else {
            request.setJsonEntity(Strings.format("""
                {
                    "name": "test-api-key",
                    "role_descriptors": %s
                }
                """, roleDescriptors));
        }
        Response response = performRequest(username, request);
        assertOK(response);
        Map<String, Object> responseAsMap = responseAsMap(response);
        String encoded = (String) responseAsMap.get("encoded");
        apiKeys.put(username, encoded);
    }

    protected void upsertRole(String roleDescriptor, String roleName) throws IOException {
        Request createRoleRequest = roleRequest(roleDescriptor, roleName);
        Response createRoleResponse = adminClient().performRequest(createRoleRequest);
        assertOK(createRoleResponse);
    }

    protected Request roleRequest(String roleDescriptor, String roleName) {
        Request createRoleRequest;
        if (randomBoolean()) {
            createRoleRequest = new Request(randomFrom(HttpPut.METHOD_NAME, HttpPost.METHOD_NAME), "/_security/role/" + roleName);
            createRoleRequest.setJsonEntity(roleDescriptor);
        } else {
            createRoleRequest = new Request(HttpPost.METHOD_NAME, "/_security/role");
            createRoleRequest.setJsonEntity(org.elasticsearch.core.Strings.format("""
                {"roles": {"%s": %s}}
                """, roleName, roleDescriptor));
        }
        return createRoleRequest;
    }
}
