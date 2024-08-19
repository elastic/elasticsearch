/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

public abstract class SecurityInBasicRestTestCase extends ESRestTestCase {

    protected static final String REST_USER = "security_test_user";
    private static final SecureString REST_PASSWORD = new SecureString("security-test-password".toCharArray());
    protected static final String TEST_USER_AUTH_HEADER = "Basic c2VjdXJpdHlfdGVzdF91c2VyOnNlY3VyaXR5LXRlc3QtcGFzc3dvcmQ=";

    protected static final String READ_SECURITY_USER = "read_security_user";
    private static final SecureString READ_SECURITY_PASSWORD = new SecureString("read-security-password".toCharArray());

    private static final String ADMIN_USER = "admin_user";
    private static final SecureString ADMIN_PASSWORD = new SecureString("admin-password".toCharArray());

    protected static final String API_KEY_USER = "api_key_user";
    private static final SecureString API_KEY_USER_PASSWORD = new SecureString("security-test-password".toCharArray());
    protected static final String API_KEY_USER_AUTH_HEADER = "Basic YXBpX2tleV91c2VyOnNlY3VyaXR5LXRlc3QtcGFzc3dvcmQ=";

    protected static final String API_KEY_ADMIN_USER = "api_key_admin";
    private static final SecureString API_KEY_ADMIN_USER_PASSWORD = new SecureString("security-test-password".toCharArray());
    protected static final String API_KEY_ADMIN_AUTH_HEADER = "Basic YXBpX2tleV9hZG1pbjpzZWN1cml0eS10ZXN0LXBhc3N3b3Jk";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(2)
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "basic")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.ssl.diagnose.trust", "true")
        .setting("xpack.security.http.ssl.enabled", "false")
        .setting("xpack.security.transport.ssl.enabled", "false")
        .setting("xpack.security.authc.token.enabled", "true")
        .setting("xpack.security.authc.api_key.enabled", "true")
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .user(ADMIN_USER, ADMIN_PASSWORD.toString(), User.ROOT_USER_ROLE, true)
        .user(REST_USER, REST_PASSWORD.toString(), "security_test_role", false)
        .user(API_KEY_USER, API_KEY_USER_PASSWORD.toString(), "api_key_user_role", false)
        .user(API_KEY_ADMIN_USER, API_KEY_ADMIN_USER_PASSWORD.toString(), "api_key_admin_role", false)
        .user(READ_SECURITY_USER, READ_SECURITY_PASSWORD.toString(), "read_security_user_role", false)
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
        String token = basicAuthHeaderValue(REST_USER, REST_PASSWORD);
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }
}
