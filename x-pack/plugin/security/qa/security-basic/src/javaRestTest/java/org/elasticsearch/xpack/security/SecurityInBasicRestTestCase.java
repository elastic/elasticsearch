/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.util.List;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;

public abstract class SecurityInBasicRestTestCase extends ESRestTestCase {
    private RestHighLevelClient highLevelAdminClient;

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("admin_user", new SecureString("admin-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("security_test_user", new SecureString("security-test-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    private RestHighLevelClient getHighLevelAdminClient() {
        if (highLevelAdminClient == null) {
            highLevelAdminClient = new RestHighLevelClient(
                adminClient(),
                ignore -> {
                },
                List.of()) {
            };
        }
        return highLevelAdminClient;
    }
}
