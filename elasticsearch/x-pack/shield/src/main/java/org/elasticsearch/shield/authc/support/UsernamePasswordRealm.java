/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.shield.authc.Realm;
import org.elasticsearch.shield.authc.RealmConfig;

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

    public static abstract class Factory<R extends UsernamePasswordRealm> extends Realm.Factory<R> {

        protected Factory(String type, RestController restController, boolean internal) {
            super(type, internal);
            restController.registerRelevantHeaders(UsernamePasswordToken.BASIC_AUTH_HEADER);
        }
    }
}
