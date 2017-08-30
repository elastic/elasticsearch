/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.security;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.sql.RestSqlTestCase;

import static org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;

/**
 * Integration test for the rest sql action. The one that speaks json directly to a
 * user rather than to the JDBC driver or CLI.
 */
public class RestSqlIT extends RestSqlTestCase {
    /**
     * All tests run as a an administrative user but use
     * <code>es-security-runas-user</code> to become a less privileged user when needed.
     */
    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder()
                .put(ThreadContext.PREFIX + ".Authorization", token)
                .build();
    }
}
