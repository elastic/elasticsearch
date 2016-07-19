/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.security.support.AbstractSecurityModule;
import org.elasticsearch.xpack.security.user.AnonymousUser;

/**
 *
 */
public class AuthenticationModule extends AbstractSecurityModule.Node {

    private Class<? extends AuthenticationFailureHandler> authcFailureHandler = null;

    public AuthenticationModule(Settings settings) {
        super(settings);
    }

    @Override
    protected void configureNode() {
        if (!securityEnabled) {
            bind(Realms.class).toProvider(Providers.of(null));
            return;
        }

        AnonymousUser.initialize(settings);
        if (authcFailureHandler == null) {
            bind(AuthenticationFailureHandler.class).to(DefaultAuthenticationFailureHandler.class).asEagerSingleton();
        } else {
            bind(AuthenticationFailureHandler.class).to(authcFailureHandler).asEagerSingleton();
        }
        bind(AuthenticationService.class).asEagerSingleton();
    }

    /**
     * Sets the {@link AuthenticationFailureHandler} to the specified implementation
     */
    public void setAuthenticationFailureHandler(Class<? extends AuthenticationFailureHandler> clazz) {
        this.authcFailureHandler = clazz;
    }
}
