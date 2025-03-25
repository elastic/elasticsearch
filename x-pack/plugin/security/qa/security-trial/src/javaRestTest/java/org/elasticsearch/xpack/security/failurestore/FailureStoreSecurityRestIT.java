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
import org.elasticsearch.core.Tuple;
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
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

    private static final String ADMIN_USER = "admin_user";

    private static final String DATA_ACCESS = "data_access";
    private static final String BACKING_INDEX_DATA_ACCESS = "backing_index_data_access";
    private static final String BACKING_INDEX_FAILURE_ACCESS = "backing_index_failure_access";
    private static final String FAILURE_INDEX_DATA_ACCESS = "failure_index_data_access";
    private static final String FAILURE_INDEX_FAILURE_ACCESS = "failure_index_failure_access";
    private static final String STAR_READ_ONLY_ACCESS = "star_read_only";
    private static final String FAILURE_STORE_ACCESS = "failure_store_access";
    private static final String BOTH_ACCESS = "both_access";
    private static final String WRITE_ACCESS = "write_access";
    private static final String MANAGE_ACCESS = "manage_access";
    private static final String MANAGE_FAILURE_STORE_ACCESS = "manage_failure_store_access";
    private static final SecureString PASSWORD = new SecureString("admin-password");

    @Before
    public void setup() throws IOException {
        apiKeys = new HashMap<>();
        createUser(WRITE_ACCESS, PASSWORD, WRITE_ACCESS);
        upsertRole(Strings.format("""
            {
              "cluster": ["all"],
              "indices": [{"names": ["test*"], "privileges": ["write", "auto_configure"]}]
            }"""), WRITE_ACCESS);
    }

    public void testGetUserPrivileges() throws IOException {
        createUser("user", PASSWORD, "role");

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

        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["read", "read_failure_store"]
                },
                {
                  "names": ["*"],
                  "privileges": ["write", "manage_failure_store"]
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
                  "privileges": ["read", "write"],
                  "allow_restricted_indices": false
                },
                {
                  "names": ["*"],
                  "privileges": ["manage_failure_store", "read_failure_store"],
                  "allow_restricted_indices": false
                }],
              "applications": [],
              "run_as": []
            }""");
    }

    public void testRoleWithSelectorInIndexPattern() throws Exception {
        setupDataStream();

        createUser("user", PASSWORD, "role");
        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["*::failures"],
                  "privileges": ["read"]
                }
              ]
            }""", "role");
        createAndStoreApiKey("user", null);

        expectThrows("user", new Search("test1::failures"), 403);
        expectSearch("user", new Search("*::failures"));

        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["test1::failures"],
                  "privileges": ["read"]
                }
              ]
            }""", "role");

        expectThrows("user", new Search("test1::failures"), 403);
        expectSearch("user", new Search("*::failures"));

        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["*::failures"],
                  "privileges": ["read_failure_store"]
                }
              ]
            }""", "role");
        expectThrows("user", new Search("test1::failures"), 403);
        expectSearch("user", new Search("*::failures"));

        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["test1::failures"],
                  "privileges": ["read_failure_store"]
                }
              ]
            }""", "role");
        expectThrows("user", new Search("test1::failures"), 403);
        expectSearch("user", new Search("*::failures"));
    }

    @SuppressWarnings("unchecked")
    public void testFailureStoreAccess() throws Exception {
        List<String> docIds = setupDataStream();
        assertThat(docIds.size(), equalTo(2));
        assertThat(docIds, hasItem("1"));
        String dataDocId = "1";
        String failuresDocId = docIds.stream().filter(id -> false == id.equals(dataDocId)).findFirst().get();

        Tuple<String, String> backingIndices = getSingleDataAndFailureIndices("test1");
        String dataIndexName = backingIndices.v1();
        String failureIndexName = backingIndices.v2();

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

        createAndStoreApiKey(WRITE_ACCESS, randomBoolean() ? null : """
            {
              "role": {
                "cluster": ["all"],
                "indices": [{"names": ["test*"], "privileges": ["write", "auto_configure"]}]
              }
            }
            """);

        createUser(BACKING_INDEX_DATA_ACCESS, PASSWORD, BACKING_INDEX_DATA_ACCESS);
        upsertRole(Strings.format("""
            {
              "cluster": ["all"],
              "indices": [{"names": ["%s"], "privileges": ["read"]}]
            }""", dataIndexName), BACKING_INDEX_DATA_ACCESS);
        createAndStoreApiKey(BACKING_INDEX_DATA_ACCESS, null);

        createUser(BACKING_INDEX_FAILURE_ACCESS, PASSWORD, BACKING_INDEX_FAILURE_ACCESS);
        upsertRole(Strings.format("""
            {
              "cluster": ["all"],
              "indices": [{"names": ["%s"], "privileges": ["read_failure_store"]}]
            }""", dataIndexName), BACKING_INDEX_FAILURE_ACCESS);
        createAndStoreApiKey(BACKING_INDEX_FAILURE_ACCESS, null);

        createUser(FAILURE_INDEX_DATA_ACCESS, PASSWORD, FAILURE_INDEX_DATA_ACCESS);
        upsertRole(Strings.format("""
            {
              "cluster": ["all"],
              "indices": [{"names": ["%s"], "privileges": ["read"]}]
            }""", failureIndexName), FAILURE_INDEX_DATA_ACCESS);
        createAndStoreApiKey(FAILURE_INDEX_DATA_ACCESS, null);

        createUser(FAILURE_INDEX_FAILURE_ACCESS, PASSWORD, FAILURE_INDEX_FAILURE_ACCESS);
        upsertRole(Strings.format("""
            {
              "cluster": ["all"],
              "indices": [{"names": ["%s"], "privileges": ["read_failure_store"]}]
            }""", failureIndexName), FAILURE_INDEX_FAILURE_ACCESS);
        createAndStoreApiKey(FAILURE_INDEX_FAILURE_ACCESS, null);

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

        List<String> users = List.of(
            DATA_ACCESS,
            FAILURE_STORE_ACCESS,
            STAR_READ_ONLY_ACCESS,
            BOTH_ACCESS,
            ADMIN_USER,
            BACKING_INDEX_DATA_ACCESS,
            BACKING_INDEX_FAILURE_ACCESS,
            FAILURE_INDEX_DATA_ACCESS,
            FAILURE_INDEX_FAILURE_ACCESS
        );

        // search data
        {
            var request = new Search(randomFrom("test1::data", "test1"));
            for (var user : users) {
                switch (user) {
                    case ADMIN_USER, DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS:
                        expectThrows(user, request, 403);
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
                    case ADMIN_USER, DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
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
                    case ADMIN_USER, DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS:
                        expectThrows(user, request, 403);
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
                    case ADMIN_USER, DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
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
                    case ADMIN_USER, DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
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
                    case ADMIN_USER, DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        // note expand_wildcards does not include hidden here
        for (var request : List.of(new Search("*"), new Search("_all"), new Search(""))) {
            for (var user : users) {
                switch (user) {
                    case ADMIN_USER, DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS,
                        BACKING_INDEX_DATA_ACCESS:
                        expectSearch(user, request);
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
                    case ADMIN_USER, DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS, BACKING_INDEX_DATA_ACCESS:
                        expectSearch(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearch(user, request);
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
                    case ADMIN_USER, DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS, BACKING_INDEX_DATA_ACCESS:
                        expectSearch(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectThrows(user, request, 403);
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
                    case ADMIN_USER, DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS, BACKING_INDEX_DATA_ACCESS:
                        expectSearch(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearch(user, request);
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
                    case ADMIN_USER, DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expectThrows(user, request, 404);
                        break;
                    case FAILURE_STORE_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_FAILURE_ACCESS,
                        FAILURE_INDEX_DATA_ACCESS:
                        expectThrows(user, request, 403);
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
                    case ADMIN_USER, DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS, FAILURE_STORE_ACCESS, BACKING_INDEX_DATA_ACCESS,
                        BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearch(user, request);
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
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectThrows(user, request, 403);
                        break;
                    case ADMIN_USER, FAILURE_STORE_ACCESS, BOTH_ACCESS:
                        expectSearch(user, request, failuresDocId);
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
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearch(user, request);
                        break;
                    case ADMIN_USER, FAILURE_STORE_ACCESS, BOTH_ACCESS:
                        expectSearch(user, request, failuresDocId);
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
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectThrows(user, request, 403);
                        break;
                    case ADMIN_USER, FAILURE_STORE_ACCESS, BOTH_ACCESS:
                        expectSearch(user, request, failuresDocId);
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
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearch(user, request);
                        break;
                    case ADMIN_USER, FAILURE_STORE_ACCESS, BOTH_ACCESS:
                        expectSearch(user, request, failuresDocId);
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
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearch(user, request);
                        break;
                    case ADMIN_USER, FAILURE_STORE_ACCESS, BOTH_ACCESS:
                        expectSearch(user, request, failuresDocId);
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
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearch(user, request);
                        break;
                    case ADMIN_USER, FAILURE_STORE_ACCESS, BOTH_ACCESS:
                        expectSearch(user, request, failuresDocId);
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
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearch(user, request);
                        break;
                    case ADMIN_USER, FAILURE_STORE_ACCESS, BOTH_ACCESS:
                        expectSearch(user, request, failuresDocId);
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
                    case DATA_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
                        break;
                    case ADMIN_USER, FAILURE_STORE_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearch(user, request, failuresDocId);
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
                    case DATA_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectThrows(user, request, 403);
                        break;
                    case ADMIN_USER, FAILURE_STORE_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearch(user, request, failuresDocId);
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
                    case DATA_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
                        break;
                    case ADMIN_USER, FAILURE_STORE_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearch(user, request, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search(failureIndexName + "::failures");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS,
                        FAILURE_INDEX_DATA_ACCESS:
                        expectThrows(user, request, 403);
                        break;
                    case FAILURE_STORE_ACCESS, BOTH_ACCESS, ADMIN_USER, FAILURE_INDEX_FAILURE_ACCESS:
                        expectThrows(user, request, 404);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search(failureIndexName + "::failures", "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, FAILURE_STORE_ACCESS, ADMIN_USER, STAR_READ_ONLY_ACCESS, BOTH_ACCESS, BACKING_INDEX_DATA_ACCESS,
                        BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search(dataIndexName + "::failures");
            for (var user : users) {
                switch (user) {
                    case STAR_READ_ONLY_ACCESS, BOTH_ACCESS, DATA_ACCESS, FAILURE_STORE_ACCESS, FAILURE_INDEX_DATA_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS, BACKING_INDEX_DATA_ACCESS:
                        expectThrows(user, request, 403);
                        break;
                    case ADMIN_USER, BACKING_INDEX_FAILURE_ACCESS:
                        expectThrows(user, request, 404);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search(dataIndexName + "::failures", "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, FAILURE_STORE_ACCESS, ADMIN_USER, STAR_READ_ONLY_ACCESS, BOTH_ACCESS, BACKING_INDEX_DATA_ACCESS,
                        BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search(".fs*::failures");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, FAILURE_STORE_ACCESS, ADMIN_USER, STAR_READ_ONLY_ACCESS, BOTH_ACCESS, BACKING_INDEX_DATA_ACCESS,
                        BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search(".ds*::failures");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, FAILURE_STORE_ACCESS, ADMIN_USER, STAR_READ_ONLY_ACCESS, BOTH_ACCESS, BACKING_INDEX_DATA_ACCESS,
                        BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
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
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS,
                        FAILURE_INDEX_DATA_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectThrows(user, request, 403);
                        break;
                    case ADMIN_USER, FAILURE_STORE_ACCESS, BOTH_ACCESS:
                        expectThrows(user, request, 404);
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
                    case ADMIN_USER, DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS, FAILURE_STORE_ACCESS, BACKING_INDEX_DATA_ACCESS,
                        BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
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
                    case DATA_ACCESS, FAILURE_STORE_ACCESS, STAR_READ_ONLY_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS,
                        FAILURE_INDEX_DATA_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectThrows(user, request, 403);
                        break;
                    case ADMIN_USER, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId, failuresDocId);
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
                        expectSearch(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS:
                        expectSearch(user, request, failuresDocId);
                        break;
                    case ADMIN_USER, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId, failuresDocId);
                        break;
                    case BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
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
                    case DATA_ACCESS, FAILURE_STORE_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS,
                        FAILURE_INDEX_DATA_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectThrows(user, request, 403);
                        break;
                    case ADMIN_USER, BOTH_ACCESS, STAR_READ_ONLY_ACCESS:
                        expectSearch(user, request, dataDocId, failuresDocId);
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
                        expectSearch(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearch(user, request, failuresDocId);
                        break;
                    case ADMIN_USER, BOTH_ACCESS, STAR_READ_ONLY_ACCESS:
                        expectSearch(user, request, dataDocId, failuresDocId);
                        break;
                    case BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
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
                    case DATA_ACCESS, FAILURE_STORE_ACCESS, STAR_READ_ONLY_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS,
                        FAILURE_INDEX_DATA_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectThrows(user, request, 403);
                        break;
                    case ADMIN_USER, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId, failuresDocId);
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
                    case DATA_ACCESS, BACKING_INDEX_DATA_ACCESS, STAR_READ_ONLY_ACCESS:
                        expectSearch(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS:
                        expectSearch(user, request, failuresDocId);
                        break;
                    case ADMIN_USER, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId, failuresDocId);
                        break;
                    case BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
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
                    case FAILURE_STORE_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS:
                        expectThrows(user, request, 403);
                        break;
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS:
                        expectSearch(user, request, dataDocId);
                        break;
                    case ADMIN_USER, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId, failuresDocId);
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
                        expectSearch(user, request, failuresDocId);
                        break;
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS:
                        expectSearch(user, request, dataDocId);
                        break;
                    case ADMIN_USER, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId, failuresDocId);
                        break;
                    case BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
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
                        expectSearch(user, request, failuresDocId);
                        break;
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS,
                        FAILURE_INDEX_DATA_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectThrows(user, request, 403);
                        break;
                    case ADMIN_USER, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId, failuresDocId);
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
                        expectSearch(user, request, failuresDocId);
                        break;
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS:
                        expectSearch(user, request, dataDocId);
                        break;
                    case ADMIN_USER, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId, failuresDocId);
                        break;
                    case BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
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
                        expectSearch(user, request, failuresDocId);
                        break;
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS:
                        expectSearch(user, request, dataDocId);
                        break;
                    case ADMIN_USER, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId, failuresDocId);
                        break;
                    case BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_FAILURE_ACCESS, BACKING_INDEX_DATA_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearch(user, request);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
    }

    public void testWriteOperations() throws IOException {
        setupDataStream();
        Tuple<String, String> backingIndices = getSingleDataAndFailureIndices("test1");
        String dataIndexName = backingIndices.v1();
        String failureIndexName = backingIndices.v2();

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

        // user with manage access to data stream does NOT get direct access to failure index
        expectThrows(() -> deleteIndex(MANAGE_ACCESS, failureIndexName), 403);
        expectThrows(() -> deleteIndex(MANAGE_ACCESS, dataIndexName), 400);
        // manage_failure_store user COULD delete failure index (not valid because it's a write index, but allow security-wise)
        expectThrows(() -> deleteIndex(MANAGE_FAILURE_STORE_ACCESS, dataIndexName), 403);
        expectThrows(() -> deleteIndex(MANAGE_FAILURE_STORE_ACCESS, failureIndexName), 400);
        expectThrows(() -> deleteDataStream(MANAGE_FAILURE_STORE_ACCESS, dataIndexName), 403);

        expectThrows(() -> deleteDataStream(MANAGE_FAILURE_STORE_ACCESS, "test1"), 403);
        expectThrows(() -> deleteDataStream(MANAGE_FAILURE_STORE_ACCESS, "test1::failures"), 403);

        // manage user can delete data stream
        deleteDataStream(MANAGE_ACCESS, "test1");

        // deleting data stream deletes everything, including failure index
        expectThrows(() -> adminClient().performRequest(new Request("GET", "/test1/_search")), 404);
        expectThrows(() -> adminClient().performRequest(new Request("GET", "/" + dataIndexName + "/_search")), 404);
        expectThrows(() -> adminClient().performRequest(new Request("GET", "/test1::failures/_search")), 404);
        expectThrows(() -> adminClient().performRequest(new Request("GET", "/" + failureIndexName + "/_search")), 404);
    }

    public void testFailureStoreAccessWithApiKeys() throws Exception {
        List<String> docIds = setupDataStream();
        assertThat(docIds.size(), equalTo(2));
        assertThat(docIds, hasItem("1"));
        String dataDocId = "1";
        String failuresDocId = docIds.stream().filter(id -> false == id.equals(dataDocId)).findFirst().get();

        Tuple<String, String> backingIndices = getSingleDataAndFailureIndices("test1");
        String dataIndexName = backingIndices.v1();
        String failureIndexName = backingIndices.v2();

        var user = "user";
        var role = "role";
        createUser(user, PASSWORD, role);
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
            """, role);

        String apiKey = createApiKey(user, """
            {
                "role": {
                    "cluster": ["all"],
                    "indices": [{"names": ["test1"], "privileges": ["read_failure_store"]}]
                }
            }""");

        expectSearchWithApiKey(apiKey, new Search("test1::failures"), failuresDocId);
        expectSearchWithApiKey(apiKey, new Search(failureIndexName), failuresDocId);
        expectThrowsWithApiKey(apiKey, new Search(dataIndexName), 403);
        expectThrowsWithApiKey(apiKey, new Search("test1"), 403);

        apiKey = createApiKey(user, """
            {
                "role": {
                    "cluster": ["all"],
                    "indices": [{"names": ["test1"], "privileges": ["read_failure_store", "read"]}]
                }
            }""");

        expectSearchWithApiKey(apiKey, new Search("test1::failures"), failuresDocId);
        expectSearchWithApiKey(apiKey, new Search(failureIndexName), failuresDocId);
        expectThrowsWithApiKey(apiKey, new Search(dataIndexName), 403);
        expectThrowsWithApiKey(apiKey, new Search("test1"), 403);

        apiKey = createApiKey(user, """
            {
                "role": {
                    "cluster": ["all"],
                    "indices": [{"names": ["test2"], "privileges": ["read_failure_store", "read"]}]
                }
            }""");

        expectThrowsWithApiKey(apiKey, new Search("test1::failures"), 403);
        expectThrowsWithApiKey(apiKey, new Search(failureIndexName), 403);
        expectThrowsWithApiKey(apiKey, new Search(dataIndexName), 403);
        expectThrowsWithApiKey(apiKey, new Search("test1"), 403);

        apiKey = createApiKey(user, """
            {
                "role": {
                    "cluster": ["all"],
                    "indices": [
                        {"names": ["test1"], "privileges": ["read_failure_store"]},
                        {"names": ["*"], "privileges": ["read"]}
                    ]
                }
            }""");

        expectSearchWithApiKey(apiKey, new Search("test1::failures"), failuresDocId);
        expectSearchWithApiKey(apiKey, new Search(failureIndexName), failuresDocId);
        expectThrowsWithApiKey(apiKey, new Search(dataIndexName), 403);
        expectThrowsWithApiKey(apiKey, new Search("test1"), 403);

        apiKey = createApiKey(user, """
            {
                "role": {
                    "cluster": ["all"],
                    "indices": [
                        {"names": ["*"], "privileges": ["read"]}
                    ]
                }
            }""");

        expectThrowsWithApiKey(apiKey, new Search("test1::failures"), 403);
        // funky but correct: assigned role descriptors grant direct access to failure index, limited-by to failure store
        expectSearchWithApiKey(apiKey, new Search(failureIndexName), failuresDocId);
        expectThrowsWithApiKey(apiKey, new Search(dataIndexName), 403);
        expectThrowsWithApiKey(apiKey, new Search("test1"), 403);

        upsertRole("""
            {
                "cluster": ["all"],
                "indices": [
                    {
                        "names": ["*"],
                        "privileges": ["read"]
                    }
                ]
            }
            """, role);
        apiKey = createApiKey(user, """
            {
                "role": {
                    "cluster": ["all"],
                    "indices": [
                        {"names": ["test1"], "privileges": ["read_failure_store"]}
                    ]
                }
            }""");
        expectThrowsWithApiKey(apiKey, new Search("test1::failures"), 403);
        // funky but correct: limited-by role descriptors grant direct access to failure index, assigned to failure store
        expectSearchWithApiKey(apiKey, new Search(failureIndexName), failuresDocId);
        expectThrowsWithApiKey(apiKey, new Search(dataIndexName), 403);
        expectThrowsWithApiKey(apiKey, new Search("test1"), 403);
    }

    public void testPit() throws Exception {
        List<String> docIds = setupDataStream();
        String dataDocId = "1";
        String failuresDocId = docIds.stream().filter(id -> false == id.equals(dataDocId)).findFirst().get();

        createUser("user", PASSWORD, "role");
        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["test*"],
                  "privileges": ["read"]
                }
              ]
            }""", "role");

        {
            expectThrows(
                () -> performRequest("user", new Request("POST", Strings.format("/%s/_pit?keep_alive=1m", "test1::failures"))),
                403
            );
            Response pitResponse = performRequest("user", new Request("POST", Strings.format("/%s/_pit?keep_alive=1m", "test1")));
            assertOK(pitResponse);
            String pitId = ObjectPath.createFromResponse(pitResponse).evaluate("id");
            assertThat(pitId, notNullValue());

            var searchRequest = new Request("POST", "/_search");
            searchRequest.setJsonEntity(Strings.format("""
                {
                    "pit": {
                        "id": "%s"
                    }
                }
                """, pitId));
            Response searchResponse = performRequest("user", searchRequest);
            expectSearch(searchResponse, dataDocId);
        }

        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["test*"],
                  "privileges": ["read_failure_store"]
                }
              ]
            }""", "role");

        {
            expectThrows(() -> performRequest("user", new Request("POST", Strings.format("/%s/_pit?keep_alive=1m", "test1"))), 403);
            Response pitResponse = performRequest("user", new Request("POST", Strings.format("/%s/_pit?keep_alive=1m", "test1::failures")));
            assertOK(pitResponse);
            String pitId = ObjectPath.createFromResponse(pitResponse).evaluate("id");
            assertThat(pitId, notNullValue());

            var searchRequest = new Request("POST", "/_search");
            searchRequest.setJsonEntity(Strings.format("""
                {
                    "pit": {
                        "id": "%s"
                    }
                }
                """, pitId));
            Response searchResponse = performRequest("user", searchRequest);
            expectSearch(searchResponse, failuresDocId);
        }
    }

    public void testDlsFls() throws Exception {
        setupDataStream();

        Tuple<String, String> backingIndices = getSingleDataAndFailureIndices("test1");
        String dataIndexName = backingIndices.v1();
        String failureIndexName = backingIndices.v2();

        String user = "user";
        String role = "role";
        createUser(user, PASSWORD, role);
        upsertRole("""
            {
                 "cluster": ["all"],
                 "indices": [
                     {
                        "names": ["test*"],
                        "privileges": ["read", "read_failure_store"],
                        "field_security": {
                            "grant": ["@timestamp", "age"]
                        }
                     }
                 ]
             }""", role);

        // FLS applies to regular data stream
        assertSearchResponseContainsExpectedIndicesAndFields(
            performRequest(user, new Search(randomFrom("test1", "test1::data")).toSearchRequest()),
            Map.of(dataIndexName, Set.of("@timestamp", "age"))
        );

        // FLS sort of applies to failure store
        // TODO this will change with FLS handling
        assertSearchResponseContainsExpectedIndicesAndFields(
            performRequest(user, new Search("test1::failures").toSearchRequest()),
            Map.of(failureIndexName, Set.of("@timestamp"))
        );

        upsertRole(Strings.format("""
            {
                 "cluster": ["all"],
                 "indices": [
                     {
                        "names": ["%s"],
                        "privileges": ["read"],
                        "field_security": {
                            "grant": ["@timestamp", "age"]
                        }
                     },
                     {
                        "names": ["test*"],
                        "privileges": ["read_failure_store"],
                        "field_security": {
                            "grant": ["@timestamp", "age"]
                        }
                     }
                 ]
             }""", randomFrom("test*", "test1")), role);

        // FLS applies to regular data stream
        assertSearchResponseContainsExpectedIndicesAndFields(
            performRequest(user, new Search(randomFrom("test1", "test1::data")).toSearchRequest()),
            Map.of(dataIndexName, Set.of("@timestamp", "age"))
        );

        // FLS sort of applies to failure store
        // TODO this will change with FLS handling
        assertSearchResponseContainsExpectedIndicesAndFields(
            performRequest(user, new Search("test1::failures").toSearchRequest()),
            Map.of(failureIndexName, Set.of("@timestamp"))
        );

        upsertRole("""
            {
                 "cluster": ["all"],
                 "indices": [
                     {
                        "names": ["test*"],
                        "privileges": ["read"],
                        "field_security": {
                            "grant": ["@timestamp", "age"]
                        }
                     },
                     {
                        "names": ["test*"],
                        "privileges": ["read_failure_store"]
                     }
                 ]
             }""", role);

        // since there is a section without FLS, no FLS applies
        assertSearchResponseContainsExpectedIndicesAndFields(
            performRequest(user, new Search(randomFrom("test1", "test1::data")).toSearchRequest()),
            Map.of(dataIndexName, Set.of("@timestamp", "age", "name", "email"))
        );

        assertSearchResponseContainsExpectedIndicesAndFields(
            performRequest(user, new Search("test1::failures").toSearchRequest()),
            Map.of(failureIndexName, Set.of("@timestamp", "document", "error"))
        );

        // DLS
        String dataIndexDocId = "1";
        upsertRole("""
            {
                 "cluster": ["all"],
                 "indices": [
                     {
                        "names": ["test*"],
                        "privileges": ["read", "read_failure_store"],
                        "query":{"term":{"name":{"value":"not-jack"}}}
                     }
                 ]
             }""", role);
        // DLS applies and no docs match the query
        expectSearch(user, new Search(randomFrom("test1", "test1::data")));
        expectSearch(user, new Search("test1::failures"));

        upsertRole("""
            {
                 "cluster": ["all"],
                 "indices": [
                     {
                        "names": ["test*"],
                        "privileges": ["read", "read_failure_store"],
                        "query":{"term":{"name":{"value":"jack"}}}
                     }
                 ]
             }""", role);
        // DLS applies and doc matches the query
        expectSearch(user, new Search(randomFrom("test1", "test1::data")), dataIndexDocId);
        expectSearch(user, new Search("test1::failures"));

        upsertRole("""
            {
                 "cluster": ["all"],
                 "indices": [
                     {
                        "names": ["test*"],
                        "privileges": ["read"],
                        "query":{"term":{"name":{"value":"not-jack"}}}
                     },
                     {
                        "names": ["test*"],
                        "privileges": ["read_failure_store"]
                     }
                 ]
             }""", role);
        // DLS does not apply because there is a section without DLS
        expectSearch(user, new Search(randomFrom("test1", "test1::data")), dataIndexDocId);
    }

    private static void expectThrows(ThrowingRunnable runnable, int statusCode) {
        var ex = expectThrows(ResponseException.class, runnable);
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(statusCode));
    }

    private void expectThrows(String user, Search search, int statusCode) {
        expectThrows(() -> performRequest(user, search.toSearchRequest()), statusCode);
        expectThrows(() -> performRequest(user, search.toAsyncSearchRequest()), statusCode);
    }

    private void expectSearch(String user, Search search, String... docIds) throws Exception {
        expectSearch(performRequestMaybeUsingApiKey(user, search.toSearchRequest()), docIds);
        expectAsyncSearch(performRequestMaybeUsingApiKey(user, search.toAsyncSearchRequest()), docIds);
    }

    private void expectSearchWithApiKey(String apiKey, Search search, String... docIds) throws Exception {
        expectSearch(performRequestWithApiKey(apiKey, search.toSearchRequest()), docIds);
        expectAsyncSearch(performRequestWithApiKey(apiKey, search.toAsyncSearchRequest()), docIds);
    }

    private void expectThrowsWithApiKey(String apiKey, Search search, int statusCode) {
        expectThrows(() -> performRequestWithApiKey(apiKey, search.toSearchRequest()), statusCode);
        expectThrows(() -> performRequestWithApiKey(apiKey, search.toAsyncSearchRequest()), statusCode);
    }

    @SuppressWarnings("unchecked")
    private static void expectAsyncSearch(Response response, String... docIds) throws IOException {
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

    private static void expectSearch(Response response, String... docIds) throws IOException {
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

    private List<String> setupDataStream() throws IOException {
        createTemplates();
        return randomBoolean() ? populateDataStreamWithBulkRequest() : populateDataStreamWithDocRequests();
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
        if (randomBoolean() && apiKeys.containsKey(user)) {
            return performRequestWithApiKey(apiKeys.get(user), request);
        } else {
            return performRequest(user, request);
        }
    }

    private static Response performRequestWithApiKey(String apiKey, Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "ApiKey " + apiKey).build());
        return client().performRequest(request);
    }

    private static void expectUserPrivilegesResponse(String userPrivilegesResponse) throws IOException {
        Request request = new Request("GET", "/_security/user/_privileges");
        request.setOptions(request.getOptions().toBuilder().addHeader("Authorization", basicAuthHeaderValue("user", PASSWORD)));
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

    protected String createAndStoreApiKey(String username, @Nullable String roleDescriptors) throws IOException {
        assertThat("API key already registered for user: " + username, apiKeys.containsKey(username), is(false));
        apiKeys.put(username, createApiKey(username, roleDescriptors));
        return createApiKey(username, roleDescriptors);
    }

    private String createApiKey(String username, String roleDescriptors) throws IOException {
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
        return (String) responseAsMap.get("encoded");
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

    protected void assertSearchResponseContainsExpectedIndicesAndFields(
        Response searchResponse,
        Map<String, Set<String>> expectedIndicesAndFields
    ) {
        try {
            assertOK(searchResponse);
            var response = SearchResponseUtils.responseAsSearchResponse(searchResponse);
            try {
                final var searchResult = Arrays.stream(response.getHits().getHits())
                    .collect(Collectors.toMap(SearchHit::getIndex, SearchHit::getSourceAsMap));

                assertThat(searchResult.keySet(), equalTo(expectedIndicesAndFields.keySet()));
                for (String index : expectedIndicesAndFields.keySet()) {
                    Set<String> expectedFields = expectedIndicesAndFields.get(index);
                    assertThat(searchResult.get(index).keySet(), equalTo(expectedFields));
                }
            } finally {
                response.decRef();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private Tuple<List<String>, List<String>> getDataAndFailureIndices(String dataStreamName) throws IOException {
        Request dataStream = new Request("GET", "/_data_stream/" + dataStreamName);
        Response response = adminClient().performRequest(dataStream);
        Map<String, Object> dataStreams = entityAsMap(response);
        assertEquals(Collections.singletonList("test1"), XContentMapValues.extractValue("data_streams.name", dataStreams));
        List<String> dataIndexNames = (List<String>) XContentMapValues.extractValue("data_streams.indices.index_name", dataStreams);
        List<String> failureIndexNames = (List<String>) XContentMapValues.extractValue(
            "data_streams.failure_store.indices.index_name",
            dataStreams
        );
        return new Tuple<>(dataIndexNames, failureIndexNames);
    }

    private Tuple<String, String> getSingleDataAndFailureIndices(String dataStreamName) throws IOException {
        Tuple<List<String>, List<String>> indices = getDataAndFailureIndices(dataStreamName);
        assertThat(indices.v1().size(), equalTo(1));
        assertThat(indices.v2().size(), equalTo(1));
        return new Tuple<>(indices.v1().get(0), indices.v2().get(0));
    }
}
