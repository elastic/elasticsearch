/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.xpack.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.security.authc.Realm;
import org.elasticsearch.xpack.security.authc.RealmConfig;

import java.util.Locale;

/**
 *
 */
public abstract class UsernamePasswordRealm extends Realm<UsernamePasswordToken> {

    public UsernamePasswordRealm(String type, RealmConfig config) {
        super(type, config);
    }

    @Override
    public UsernamePasswordToken token(ThreadContext threadContext) {
        return UsernamePasswordToken.extractToken(threadContext);
    }

    public boolean supports(AuthenticationToken token) {
        return token instanceof UsernamePasswordToken;
    }

    public abstract static class Factory<R extends UsernamePasswordRealm> extends Realm.Factory<R> {

        protected Factory(String type, boolean internal) {
            super(type, internal);
        }
    }

    public enum UserbaseSize {

        TINY,
        SMALL,
        MEDIUM,
        LARGE,
        XLARGE;

        public static UserbaseSize resolve(int count) {
            if (count < 10) {
                return TINY;
            }
            if (count < 100) {
                return SMALL;
            }
            if (count < 500) {
                return MEDIUM;
            }
            if (count < 1000) {
                return LARGE;
            }
            return XLARGE;
        }

        @Override
        public String toString() {
            return this == XLARGE ? "x-large" : name().toLowerCase(Locale.ROOT);
        }
    }
}
