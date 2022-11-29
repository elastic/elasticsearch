/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import static org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField.RUN_AS_USER_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;

public class SecurityUtils {

    static Settings secureClientSettings() {
        String token = basicAuthHeaderValue("test-admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    static RequestOptions runAsUserHeader(String user) {
        return RequestOptions.DEFAULT.toBuilder().addHeader(RUN_AS_USER_HEADER, user).build();
    }

    static RequestOptions userRole() {
        return runAsUserHeader("user1");
    }
}
