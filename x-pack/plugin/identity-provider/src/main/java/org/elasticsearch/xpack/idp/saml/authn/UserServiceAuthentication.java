/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.authn;

import org.elasticsearch.xpack.idp.authc.AuthenticationMethod;
import org.elasticsearch.xpack.idp.authc.NetworkControl;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;

import java.util.Set;

/**
 * Lightweight representation of a user that has authenticated to the IdP in the context of a specific service provider
 */
public class UserServiceAuthentication {
    private final String principal;
    private final Set<String> roles;
    private final SamlServiceProvider serviceProvider;
    private final Set<AuthenticationMethod> authenticationMethods;
    private final Set<NetworkControl> networkControls;

    public UserServiceAuthentication(String principal, Set<String> roles, SamlServiceProvider serviceProvider,
                                     Set<AuthenticationMethod> authenticationMethods, Set<NetworkControl> networkControls) {
        this.principal = principal;
        this.roles = roles;
        this.serviceProvider = serviceProvider;
        this.authenticationMethods = authenticationMethods;
        this.networkControls = networkControls;
    }

    public UserServiceAuthentication(String principal, Set<String> roles, SamlServiceProvider serviceProvider) {
        this(principal, roles, serviceProvider, Set.of(AuthenticationMethod.PASSWORD), Set.of(NetworkControl.TLS));
    }

    public String getPrincipal() {
        return principal;
    }

    public Set<String> getRoles() {
        return roles;
    }

    public SamlServiceProvider getServiceProvider() {
        return serviceProvider;
    }

    public Set<AuthenticationMethod> getAuthenticationMethods() {
        return authenticationMethods;
    }

    public Set<NetworkControl> getNetworkControls() {
        return networkControls;
    }
}
