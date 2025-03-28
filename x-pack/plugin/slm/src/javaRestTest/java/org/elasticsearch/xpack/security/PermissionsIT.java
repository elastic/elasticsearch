/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotRetentionConfiguration;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class PermissionsIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.http.ssl.enabled", "false")
        .user("test_admin", "x-pack-test-password", "superuser", false)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testSLMWithPermissions() throws Exception {
        String repo = "my_repository";
        createIndexAsAdmin("index", Settings.builder().put("index.number_of_replicas", 0).build(), "");

        // Set up two roles and users, one for reading SLM, another for managing SLM
        Request roleRequest = new Request("PUT", "/_security/role/slm-read");
        roleRequest.setJsonEntity("""
            { "cluster": ["read_slm"] }""");
        assertOK(adminClient().performRequest(roleRequest));
        roleRequest = new Request("PUT", "/_security/role/slm-manage");
        roleRequest.setJsonEntity("""
            {
              "cluster": [ "manage_slm", "cluster:admin/repository/*", "cluster:admin/snapshot/*" ],
              "indices": [
                {
                  "names": [ ".slm-history*" ],
                  "privileges": [ "all" ]
                }
              ]
            }""");
        assertOK(adminClient().performRequest(roleRequest));

        createUser("slm_admin", "slm-admin-password", "slm-manage");
        createUser("slm_user", "slm-user-password", "slm-read");

        PutRepositoryRequest repoRequest = new PutRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        Settings.Builder settingsBuilder = Settings.builder().put("location", ".");
        repoRequest.settings(settingsBuilder);
        repoRequest.name(repo);
        repoRequest.type(FsRepository.TYPE);
        assertAcknowledged(performWithOptions(RequestOptions.DEFAULT, "PUT", "/_snapshot/" + repo, Strings.toString(repoRequest)));

        Map<String, Object> config = new HashMap<>();
        config.put("indices", Collections.singletonList("index"));
        SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(
            "policy_id",
            "name",
            "1 2 3 * * ?",
            repo,
            config,
            new SnapshotRetentionConfiguration(TimeValue.ZERO, null, null)
        );
        final String policyStr = Strings.toString(policy);

        // Build two client options, each using a different user
        final RequestOptions adminOptions = RequestOptions.DEFAULT.toBuilder()
            .addHeader("Authorization", basicAuthHeaderValue("slm_admin", new SecureString("slm-admin-password".toCharArray())))
            .build();

        final RequestOptions userOptions = RequestOptions.DEFAULT.toBuilder()
            .addHeader("Authorization", basicAuthHeaderValue("slm_user", new SecureString("slm-user-password".toCharArray())))
            .build();

        expectThrows(ResponseException.class, () -> performWithOptions(userOptions, "PUT", "/_slm/policy/policy_id", policyStr));

        performWithOptions(adminOptions, "PUT", "/_slm/policy/policy_id", policyStr);

        performWithOptions(userOptions, "GET", "/_slm/policy/policy_id", null);
        performWithOptions(adminOptions, "GET", "/_slm/policy/policy_id", null);

        expectThrows(ResponseException.class, () -> performWithOptions(userOptions, "PUT", "/_slm/policy/policy_id/_execute", null));

        Response executeResponse = performWithOptions(adminOptions, "PUT", "/_slm/policy/policy_id/_execute", null);
        String body = EntityUtils.toString(executeResponse.getEntity());
        final String snapName = body.replace("{\"snapshot_name\":\"", "").replace("\"}", "");

        assertBusy(() -> {
            try {
                logger.info("--> checking for snapshot [{}] to be created", snapName);
                Request req = new Request("GET", "/_snapshot/" + repo + "/" + snapName);
                Response resp = adminClient().performRequest(req);
                String respStr = EntityUtils.toString(resp.getEntity());
                if (respStr.contains("SUCCESS") == false) {
                    fail("expected successful snapshot but got: " + respStr);
                }
            } catch (ResponseException e) {
                fail("expected snapshot to exist but it does not: " + e);
            }
        });

        expectThrows(ResponseException.class, () -> performWithOptions(userOptions, "POST", "/_slm/_execute_retention"));

        assertAcknowledged(performWithOptions(adminOptions, "POST", "/_slm/_execute_retention"));

        assertBusy(() -> {
            try {
                logger.info("--> checking for snapshot to be deleted");
                performWithOptions(adminOptions, "GET", "/_snapshot/" + repo + "/" + snapName);
                fail("expected 404 because snapshot should be deleted, but it still exists");
            } catch (ResponseException e) {
                assertThat(
                    "expected 404 for missing snapshot after it has been deleted",
                    e.getResponse().getStatusLine().getStatusCode(),
                    equalTo(404)
                );
            }
        });

        expectThrows(ResponseException.class, () -> performWithOptions(userOptions, "DELETE", "/_slm/policy/policy_id"));

        performWithOptions(adminOptions, "DELETE", "/_slm/policy/policy_id");
    }

    private Response performWithOptions(RequestOptions options, String verb, String endpoint) throws IOException {
        return performWithOptions(options, verb, endpoint, null);
    }

    private Response performWithOptions(RequestOptions options, String verb, String endpoint, @Nullable String jsonBody)
        throws IOException {
        Request req = new Request(verb, endpoint);
        if (jsonBody != null) {
            req.setJsonEntity(jsonBody);
        }
        req.setOptions(options);
        return adminClient().performRequest(req);
    }

    private void createIndexAsAdmin(String name, Settings settings, String mapping) throws IOException {
        Request request = new Request("PUT", "/" + name);
        request.setJsonEntity(Strings.format("""
            {
             "settings": %s, "mappings" : {%s}
            }""", Strings.toString(settings), mapping));
        assertOK(adminClient().performRequest(request));
    }

    private void createUser(String name, String password, String role) throws IOException {
        Request request = new Request("PUT", "/_security/user/" + name);
        request.setJsonEntity("{ \"password\": \"" + password + "\", \"roles\": [ \"" + role + "\"] }");
        assertOK(adminClient().performRequest(request));
    }

}
