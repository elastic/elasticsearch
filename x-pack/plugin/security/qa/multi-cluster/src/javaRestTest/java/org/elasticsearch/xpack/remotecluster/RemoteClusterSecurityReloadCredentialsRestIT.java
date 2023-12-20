/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.MutableSettingsProvider;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;

// account for slow stored secure settings updates (involves removing and re-creating the keystore)
@TimeoutSuite(millis = 10 * TimeUnits.MINUTE)
public class RemoteClusterSecurityReloadCredentialsRestIT extends AbstractRemoteClusterSecurityTestCase {

    @BeforeClass
    public static void disableInFips() {
        assumeFalse(
            "Cannot run in FIPS mode since the keystore will be password protected and sending a password in the reload"
                + "settings api call, requires TLS to be configured for the transport layer",
            inFipsJvm()
        );
    }

    private static final MutableSettingsProvider keystoreSettings = new MutableSettingsProvider();
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
            .keystore(keystoreSettings)
            .rolesFile(Resource.fromClasspath("roles.yml"))
            .user(REMOTE_SEARCH_USER, PASS.toString(), "read_remote_shared_logs", false)
            .build();
    }

    @ClassRule
    // Use a RuleChain to ensure that fulfilling cluster is started before query cluster
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    @After
    public void cleanUp() throws IOException {
        removeRemoteCluster();
        removeRemoteClusterCredentials("my_remote_cluster", keystoreSettings);
    }

    @Before
    public void setup() throws IOException {
        indexDocumentsOnFulfillingCluster();
    }

    public void testFirstTimeSetup() throws Exception {
        configureRcs2();
        assertSharedLogsSearchSuccess();
    }

    public void testUpgradeFromRcs1() throws Exception {
        // Setup RCS 1.0 and check that it works
        configureRemoteCluster("my_remote_cluster", fulfillingCluster, true, randomBoolean(), randomBoolean());
        final Request putRoleRequest = new Request("POST", "/_security/role/read_remote_shared_logs");
        putRoleRequest.setJsonEntity("""
            {
              "indices": [
                {
                  "names": [ "shared-logs" ],
                  "privileges": [ "read", "read_cross_cluster" ]
                }
              ]
            }""");
        performRequestAgainstFulfillingCluster(putRoleRequest);
        assertSharedLogsSearchSuccess();

        // Now migrate to RCS 2.0
        // Optionally remove existing cluster definition first. In practice removing the cluster definition first is the recommended
        // approach since otherwise the reload-secure-settings call may result in WARN logs, but it's functionally possible not to
        // remove the definition
        if (randomBoolean()) {
            removeRemoteCluster();
        }
        configureRcs2();
        assertSharedLogsSearchSuccess();
    }

    public void testDowngradeToRcs1() throws Exception {
        configureRcs2();
        assertSharedLogsSearchSuccess();

        if (randomBoolean()) {
            removeRemoteCluster();
        }
        removeRemoteClusterCredentials("my_remote_cluster", keystoreSettings);
        configureRemoteCluster("my_remote_cluster", fulfillingCluster, true, randomBoolean(), randomBoolean());
        final Request putRoleRequest = new Request("POST", "/_security/role/read_remote_shared_logs");
        putRoleRequest.setJsonEntity("""
            {
              "indices": [
                {
                  "names": [ "shared-logs" ],
                  "privileges": [ "read", "read_cross_cluster" ]
                }
              ]
            }""");
        performRequestAgainstFulfillingCluster(putRoleRequest);
        assertSharedLogsSearchSuccess();
    }

    private void removeRemoteCluster() throws IOException {
        updateClusterSettings(
            Settings.builder()
                .putNull("cluster.remote.my_remote_cluster.mode")
                .putNull("cluster.remote.my_remote_cluster.skip_unavailable")
                .putNull("cluster.remote.my_remote_cluster.proxy_address")
                .putNull("cluster.remote.my_remote_cluster.seeds")
                .build()
        );
    }

    private void configureRcs2() throws Exception {
        final Map<String, Object> apiKeyMap = createCrossClusterAccessApiKey("""
            {
              "search": [
                {
                    "names": ["*"]
                }
              ]
            }""");
        final String remoteClusterCredentials = (String) apiKeyMap.get("encoded");

        final boolean isProxyMode = randomBoolean();
        final boolean configureSettingsFirst = randomBoolean();
        // it's valid to first configure remote cluster, then credentials
        if (configureSettingsFirst) {
            putRemoteClusterSettings("my_remote_cluster", fulfillingCluster, false, isProxyMode, randomBoolean());
        }

        configureRemoteClusterCredentials("my_remote_cluster", remoteClusterCredentials, keystoreSettings);

        // also valid to configure credentials, then cluster
        if (false == configureSettingsFirst) {
            configureRemoteCluster("my_remote_cluster");
        } else {
            // now that credentials are configured, we expect a successful connection
            checkRemoteConnection("my_remote_cluster", fulfillingCluster, false, isProxyMode);
        }
    }

    private void assertSharedLogsSearchSuccess() throws IOException {
        final Response response = performRequestWithRemoteSearchUser(
            new Request(
                "GET",
                String.format(Locale.ROOT, "/my_remote_cluster:shared-logs/_search?ccs_minimize_roundtrips=%s", randomBoolean())
            )
        );
        assertOK(response);
        final SearchResponse searchResponse = SearchResponse.fromXContent(responseAsParser(response));
        try {
            final List<String> actualIndices = Arrays.stream(searchResponse.getHits().getHits())
                .map(SearchHit::getIndex)
                .collect(Collectors.toList());
            assertThat(actualIndices, containsInAnyOrder("shared-logs"));
        } finally {
            searchResponse.decRef();
        }
    }

    private void indexDocumentsOnFulfillingCluster() throws IOException {
        final var indexDocRequest = new Request("POST", "/shared-logs/_doc/1?refresh=true");
        indexDocRequest.setJsonEntity("{\"field\": \"1\"}");
        assertOK(performRequestAgainstFulfillingCluster(indexDocRequest));
    }

    private Response performRequestWithRemoteSearchUser(final Request request) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerFromRandomAuthMethod(REMOTE_SEARCH_USER, PASS))
        );
        return client().performRequest(request);
    }

}
