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
public interface UserServiceAuthentication {

    String getPrincipal();
    Set<String> getGroups();
    SamlServiceProvider getServiceProvider();

    Set<AuthenticationMethod> getAuthenticationMethods();
    Set<NetworkControl> getNetworkControls();
}
