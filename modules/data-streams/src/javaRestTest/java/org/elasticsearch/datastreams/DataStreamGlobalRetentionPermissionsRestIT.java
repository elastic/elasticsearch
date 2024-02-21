/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DataStreamGlobalRetentionPermissionsRestIT extends ESRestTestCase {

    private static final String PASSWORD = "secret-test-password";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .feature(FeatureFlag.FAILURE_STORE_ENABLED)
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.transport.ssl.enabled", "false")
        .setting("xpack.security.http.ssl.enabled", "false")
        .user("test_admin", PASSWORD, "superuser", false)
        .user("test_manage_global_retention", PASSWORD, "manage_data_stream_global_retention", false)
        .user("test_monitor_global_retention", PASSWORD, "monitor_data_stream_global_retention", false)
        .user("test_monitor", PASSWORD, "manage_data_stream_lifecycle", false)
        .user("test_no_privilege", PASSWORD, "no_privilege", false)
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        // If this test is running in a test framework that handles its own authorization, we don't want to overwrite it.
        if (super.restClientSettings().keySet().contains(ThreadContext.PREFIX + ".Authorization")) {
            return super.restClientSettings();
        } else {
            // Note: This user is assigned the role "manage_data_stream_lifecycle". That role is defined in roles.yml.
            String token = basicAuthHeaderValue("test_data_stream_lifecycle", new SecureString(PASSWORD.toCharArray()));
            return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
        }
    }

    @Override
    protected Settings restAdminSettings() {
        // If this test is running in a test framework that handles its own authorization, we don't want to overwrite it.
        if (super.restClientSettings().keySet().contains(ThreadContext.PREFIX + ".Authorization")) {
            return super.restClientSettings();
        } else {
            // Note: We use the admin user because the other one is too unprivileged, so it breaks the initialization of the test
            String token = basicAuthHeaderValue("test_admin", new SecureString(PASSWORD.toCharArray()));
            return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
        }
    }

    private Settings restManageGlobalRetentionClientSettings() {
        String token = basicAuthHeaderValue("test_manage_global_retention", new SecureString(PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private Settings restMonitorGlobalRetentionClientSettings() {
        String token = basicAuthHeaderValue("test_monitor_global_retention", new SecureString(PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private Settings restOnlyManageLifecycleClientSettings() {
        String token = basicAuthHeaderValue("test_monitor", new SecureString(PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private Settings restNoPrivilegeClientSettings() {
        String token = basicAuthHeaderValue("test_no_privilege", new SecureString(PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testManageGlobalRetentionPrivileges() throws Exception {
        try (var client = buildClient(restManageGlobalRetentionClientSettings(), getClusterHosts().toArray(new HttpHost[0]))) {
            Request request = new Request("PUT", "_data_stream/_global_retention");
            request.setJsonEntity("""
                {
                  "default_retention": "1d",
                  "max_retention": "7d"
                }""");
            assertAcknowledged(client.performRequest(request));
            Map<String, Object> response = entityAsMap(client.performRequest(new Request("GET", "/_data_stream/_global_retention")));
            assertThat(response.get("default_retention"), equalTo("1d"));
            assertThat(response.get("max_retention"), equalTo("7d"));
            assertAcknowledged(client.performRequest(new Request("DELETE", "/_data_stream/_global_retention")));
        }
    }

    public void testMonitorGlobalRetentionPrivileges() throws Exception {
        {
            Request request = new Request("PUT", "_data_stream/_global_retention");
            request.setJsonEntity("""
                {
                  "default_retention": "1d",
                  "max_retention": "7d"
                }""");
            assertAcknowledged(adminClient().performRequest(request));
        }
        try (var client = buildClient(restMonitorGlobalRetentionClientSettings(), getClusterHosts().toArray(new HttpHost[0]))) {
            Request request = new Request("PUT", "_data_stream/_global_retention");
            request.setJsonEntity("""
                {
                  "default_retention": "1d",
                  "max_retention": "7d"
                }""");
            ResponseException responseException = expectThrows(ResponseException.class, () -> client.performRequest(request));
            assertThat(responseException.getResponse().getStatusLine().getStatusCode(), is(403));
            assertThat(
                responseException.getMessage(),
                containsString(
                    "action [cluster:admin/data_stream/global_retention/put] is unauthorized for user [test_monitor_global_retention]"
                )
            );
            responseException = expectThrows(
                ResponseException.class,
                () -> client.performRequest(new Request("DELETE", "/_data_stream/_global_retention"))
            );
            assertThat(responseException.getResponse().getStatusLine().getStatusCode(), is(403));
            assertThat(
                responseException.getMessage(),
                containsString("action [cluster:admin/data_stream/global_retention/delete] is unauthorized for user [test_monitor_global_retention]")
            );
            Map<String, Object> response = entityAsMap(client.performRequest(new Request("GET", "/_data_stream/_global_retention")));
            assertThat(response.get("default_retention"), equalTo("1d"));
            assertThat(response.get("max_retention"), equalTo("7d"));
        }
    }

    public void testManageLifecyclePrivileges() throws Exception {
        try (var client = buildClient(restOnlyManageLifecycleClientSettings(), getClusterHosts().toArray(new HttpHost[0]))) {
            Request request = new Request("PUT", "_data_stream/_global_retention");
            request.setJsonEntity("""
                {
                  "default_retention": "1d",
                  "max_retention": "7d"
                }""");
            ResponseException responseException = expectThrows(ResponseException.class, () -> client.performRequest(request));
            assertThat(responseException.getResponse().getStatusLine().getStatusCode(), is(403));
            assertThat(
                responseException.getMessage(),
                containsString("action [cluster:admin/data_stream/global_retention/put] is unauthorized for user [test_monitor]")
            );
            // This use has the monitor privilege which includes the monitor_data_stream_global_retention
            Response response = client.performRequest(new Request("GET", "/_data_stream/_global_retention"));
            assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        }
    }

    public void testNoPrivileges() throws Exception {
        try (var client = buildClient(restNoPrivilegeClientSettings(), getClusterHosts().toArray(new HttpHost[0]))) {
            Request request = new Request("PUT", "_data_stream/_global_retention");
            request.setJsonEntity("""
                {
                  "default_retention": "1d",
                  "max_retention": "7d"
                }""");
            ResponseException responseException = expectThrows(ResponseException.class, () -> client.performRequest(request));
            assertThat(responseException.getResponse().getStatusLine().getStatusCode(), is(403));
            assertThat(
                responseException.getMessage(),
                containsString("action [cluster:admin/data_stream/global_retention/put] is unauthorized for user [test_no_privilege]")
            );
            responseException = expectThrows(
                ResponseException.class,
                () -> client.performRequest(new Request("DELETE", "/_data_stream/_global_retention"))
            );
            assertThat(responseException.getResponse().getStatusLine().getStatusCode(), is(403));
            assertThat(
                responseException.getMessage(),
                containsString("action [cluster:admin/data_stream/global_retention/delete] is unauthorized for user [test_no_privilege]")
            );
            responseException = expectThrows(
                ResponseException.class,
                () -> client.performRequest(new Request("GET", "/_data_stream/_global_retention"))
            );
            assertThat(responseException.getResponse().getStatusLine().getStatusCode(), is(403));
            assertThat(
                responseException.getMessage(),
                containsString("action [cluster:monitor/data_stream/global_retention/get] is unauthorized for user [test_no_privilege]")
            );
        }
    }
}
