/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.client.Request;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class RemoteClusterSecurityWithDlsRestIT extends AbstractRemoteClusterSecurityDlsAndFlsRestIT {

    private static final String REMOTE_CLUSTER_DLS = REMOTE_CLUSTER_ALIAS + "_dls";

    private static final AtomicReference<Map<String, Object>> API_KEY_REFERENCE = new AtomicReference<>();

    private static final String API_KEY_ACCESS = """
        {
            "search": [
              {
                  "names": ["remote_index*"],
                  "query": {"term" : {"field1" : "value1"}}
              },
              {
                  "names": ["remote_index1"],
                  "query": {"term" : {"field2" : "value1"}}
              }
            ]
        }""";

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .name("fulfilling-cluster")
            .apply(commonClusterConfig)
            .setting("remote_cluster_server.enabled", "true")
            .setting("remote_cluster.port", "0")
            .setting("xpack.security.remote_cluster_server.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_server.ssl.key", "remote-cluster.key")
            .setting("xpack.security.remote_cluster_server.ssl.certificate", "remote-cluster.crt")
            .keystore("xpack.security.remote_cluster_server.ssl.secure_key_passphrase", "remote-cluster-password")
            .build();

        queryCluster = ElasticsearchCluster.local()
            .name("query-cluster")
            .apply(commonClusterConfig)
            .setting("xpack.security.remote_cluster_client.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .keystore(
                "cluster.remote." + REMOTE_CLUSTER_DLS + ".credentials",
                () -> createCrossClusterAccessApiKey(API_KEY_ACCESS, API_KEY_REFERENCE)
            )
            .build();
    }

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    public void testCrossClusterSearchWithDls() throws Exception {
        setupRemoteClusterTestCase(REMOTE_CLUSTER_DLS);

        final Request searchRequest = new Request(
            "GET",
            Strings.format(
                "/%s:%s/_search?ccs_minimize_roundtrips=%s",
                REMOTE_CLUSTER_DLS,
                randomFrom("remote_index*", "*"),
                randomBoolean()
            )
        );

        // Running a CCS request with a user without DLS/FLS should be restricted by cross cluster access API key.
        {
            assertSearchResponseContainsExpectedIndicesAndFields(
                performRequestWithUser(searchRequest, REMOTE_SEARCH_USER_NO_DLS_FLS),
                Map.ofEntries(Map.entry("remote_index1", Set.of("field1", "field2", "field3")))
            );

            // API key with owner's permissions should return the same result.
            final String apiKeyNoDlsFls = createRemoteSearchApiKeyWithUser(REMOTE_SEARCH_USER_NO_DLS_FLS, "{}").v2();
            assertSearchResponseContainsExpectedIndicesAndFields(
                performRequestWithApiKey(searchRequest, apiKeyNoDlsFls),
                Map.ofEntries(Map.entry("remote_index1", Set.of("field1", "field2", "field3")))
            );

            // API key's role restrictions should be respected.
            String apiKeyNoDlsFlsRestricted = createRemoteSearchApiKeyWithUser(REMOTE_SEARCH_USER_NO_DLS_FLS, """
                {
                    "role1": {
                      "remote_indices": [
                        {
                          "names": ["remote_index1", "remote_index2", "*4"],
                          "privileges": ["read", "read_cross_cluster"],
                          "clusters": ["*"],
                          "query": {"bool": {"must_not": {"term" : {"field1" : "value4"}}}},
                          "field_security": {"grant": ["field1"]}
                        }
                      ]
                    },
                    "role2": {
                      "remote_indices": [
                        {
                          "names": ["remote_index1", "remote_index2", "*3"],
                          "privileges": ["read", "read_cross_cluster"],
                          "clusters": ["*"],
                          "query": {"bool": {"must_not": {"term" : {"field1" : "value3"}}}},
                          "field_security": {"grant": ["field2"]}
                        }
                      ]
                    }
                }
                """).v2();
            assertSearchResponseContainsExpectedIndicesAndFields(
                performRequestWithApiKey(searchRequest, apiKeyNoDlsFlsRestricted),
                Map.ofEntries(Map.entry("remote_index1", Set.of("field1", "field2")))
            );
        }

        // Running a CCS request with a user with DLS and FLS should be intersected with cross cluster API key's DLS and FLS permissions.
        {
            assertSearchResponseContainsExpectedIndicesAndFields(
                performRequestWithUser(searchRequest, REMOTE_SEARCH_USER_DLS_FLS),
                Map.ofEntries(Map.entry("remote_index1", Set.of("field1", "field2")))
            );

            // API key with owner's permissions should return the same result.
            final String apiKeyDlsFls = createRemoteSearchApiKeyWithUser(REMOTE_SEARCH_USER_DLS_FLS, "{}").v2();
            assertSearchResponseContainsExpectedIndicesAndFields(
                performRequestWithApiKey(searchRequest, apiKeyDlsFls),
                Map.ofEntries(Map.entry("remote_index1", Set.of("field1", "field2")))
            );

            // API key's role restrictions should be respected.
            String apiKeyDlsFlsRestricted = createRemoteSearchApiKeyWithUser(REMOTE_SEARCH_USER_DLS_FLS, """
                {
                    "role1": {
                      "remote_indices": [
                        {
                          "names": ["*"],
                          "privileges": ["all"],
                          "clusters": ["*"],
                          "query": {"bool": {"must_not": {"term" : {"field1" : "value1"}}}},
                          "field_security": {"grant": ["field1"]}
                        }
                      ]
                    }
                }
                """).v2();
            assertSearchResponseContainsEmptyResult(performRequestWithApiKey(searchRequest, apiKeyDlsFlsRestricted));
        }

        // Running a CCS request with a user with DLS only.
        {
            assertSearchResponseContainsEmptyResult(performRequestWithUser(searchRequest, REMOTE_SEARCH_USER_DLS));

            // API key with owner's permissions should return the same empty search result.
            final String apiKeyDls = createRemoteSearchApiKeyWithUser(REMOTE_SEARCH_USER_DLS, "{}").v2();
            assertSearchResponseContainsEmptyResult(performRequestWithApiKey(searchRequest, apiKeyDls));

            // API key's role should not make any difference here
            String apiKeyDlsRestricted = createRemoteSearchApiKeyWithUser(REMOTE_SEARCH_USER_DLS, """
                {
                    "role1": {
                      "remote_indices": [
                        {
                          "names": ["*"],
                          "privileges": ["all"],
                          "clusters": ["*"]
                        }
                      ]
                    }
                }
                """).v2();
            assertSearchResponseContainsEmptyResult(performRequestWithApiKey(searchRequest, apiKeyDlsRestricted));
        }

        // Running a CCS request with a user with FLS only.
        {
            assertSearchResponseContainsExpectedIndicesAndFields(
                performRequestWithUser(searchRequest, REMOTE_SEARCH_USER_FLS),
                Map.ofEntries(Map.entry("remote_index1", Set.of("field1", "field3")))
            );

            // API key with owner's permissions should return the same result.
            final String apiKeyFls = createRemoteSearchApiKeyWithUser(REMOTE_SEARCH_USER_FLS, "{}").v2();
            assertSearchResponseContainsExpectedIndicesAndFields(
                performRequestWithApiKey(searchRequest, apiKeyFls),
                Map.ofEntries(Map.entry("remote_index1", Set.of("field1", "field3")))
            );

            // API key's role restrictions should be respected.
            String apiKeyFlsRestricted = createRemoteSearchApiKeyWithUser(REMOTE_SEARCH_USER_FLS, """
                {
                    "role1": {
                      "remote_indices": [
                        {
                          "names": ["*"],
                          "privileges": ["all"],
                          "clusters": ["*"],
                          "field_security": {"grant": ["field1"]}
                        }
                      ]
                    }
                }
                """).v2();
            assertSearchResponseContainsExpectedIndicesAndFields(
                performRequestWithApiKey(searchRequest, apiKeyFlsRestricted),
                Map.ofEntries(Map.entry("remote_index1", Set.of("field1")))
            );
        }
    }
}
