/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
    private final String name;
    private final String email;
    private final Set<String> roles;

    private final SamlServiceProvider serviceProvider;
    private final Set<AuthenticationMethod> authenticationMethods;
    private final Set<NetworkControl> networkControls;

    public UserServiceAuthentication(String principal, String name, String email, Set<String> roles,
                                     SamlServiceProvider serviceProvider,
                                     Set<AuthenticationMethod> authenticationMethods, Set<NetworkControl> networkControls) {
        this.principal = principal;
        this.name = name;
        this.email = email;
        this.roles = Set.copyOf(roles);
        this.serviceProvider = serviceProvider;
        this.authenticationMethods = authenticationMethods;
        this.networkControls = networkControls;
    }

    public UserServiceAuthentication(String principal, String name, String email, Set<String> roles, SamlServiceProvider serviceProvider) {
        this(principal, name, email, roles, serviceProvider, Set.of(AuthenticationMethod.PASSWORD), Set.of(NetworkControl.TLS));
    }

    public String getPrincipal() {
        return principal;
    }

    public String getName() {
        return name;
    }

    public String getEmail() {
        return email;
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

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
            "principal='" + principal + '\'' +
            ", name='" + name + '\'' +
            ", email='" + email + '\'' +
            ", roles=" + roles +
            ", serviceProvider=" + serviceProvider +
            ", authenticationMethods=" + authenticationMethods +
            ", networkControls=" + networkControls +
            '}';
    }
}
