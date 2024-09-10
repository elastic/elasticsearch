/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class SecurityIndexAutoCreationRestIT extends ESRestTestCase {
    private static final String ADMIN_USER = "admin_user";
    private static final SecureString ADMIN_PASSWORD = new SecureString("admin-password".toCharArray());

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(2)
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "basic")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.transport.ssl.enabled", "false")
        .systemProperty("es.xpack.security.security_main_index.auto_create", "true")
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .user(ADMIN_USER, ADMIN_PASSWORD.toString(), User.ROOT_USER_ROLE, true)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue(ADMIN_USER, ADMIN_PASSWORD);
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(ADMIN_USER, ADMIN_PASSWORD);
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testMainSecurityIndexAutoCreated() throws Exception {
        assertSecurityIndexGreen();

        // ensure it is *not* auto-created again on deletions
        deleteSecurityIndex();
        // wait a bit to ensure that index really is 404, and not that auto-creation is just slow
        Thread.sleep(3000);
        assertSecurityIndexNotFound();
    }

    private static void deleteSecurityIndex() throws IOException {
        Request request = new Request("DELETE", "/.security-7");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        assertOK(adminClient().performRequest(request));
    }

    private static void assertSecurityIndexNotFound() {
        Request request = new Request("GET", "/.security-7");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        assertThat(
            expectThrows(ResponseException.class, () -> adminClient().performRequest(request)).getResponse()
                .getStatusLine()
                .getStatusCode(),
            equalTo(404)
        );
    }

    private static void assertSecurityIndexGreen() throws IOException {
        Request request = new Request("GET", "/_cluster/health/.security-7");
        request.addParameter("wait_for_status", "green");
        request.addParameter("timeout", "30s");
        request.addParameter("level", "shards");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        assertOK(adminClient().performRequest(request));
    }
}
