/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.action.support.user.ActionUser;
import org.elasticsearch.action.support.user.ActionUserContext;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;

import java.util.Optional;
import java.util.function.Supplier;

class SecurityUserResolver implements ActionUserContext.Resolver {
    private final Supplier<SecurityContext> securityContext;

    SecurityUserResolver(Supplier<SecurityContext> context) {
        this.securityContext = context;
    }

    @Override
    public Optional<ActionUser> resolve(ThreadContext t) {
        SecurityContext context = securityContext.get();
        if (context == null) {
            // Too early in the bootstrap process
            return Optional.empty();
        }
        final Authentication authentication = context.getAuthentication();
        return Optional.ofNullable(authentication).map(Authentication::getActionUser);
    }
}
