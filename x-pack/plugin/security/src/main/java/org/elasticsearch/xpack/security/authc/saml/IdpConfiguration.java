/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.saml;

import java.util.List;
import java.util.function.Supplier;

import org.opensaml.security.credential.Credential;

/**
 * A simple container class that holds all configuration related to a SAML Identity Provider (IdP).
 */
class IdpConfiguration {

    private final String entityId;
    private final Supplier<List<Credential>> signingCredentials;

    IdpConfiguration(String entityId, Supplier<List<Credential>> signingCredentials) {
        this.entityId = entityId;
        this.signingCredentials = signingCredentials;
    }

    /**
     * The SAML identifier (as a URI) for the IDP
     */
    String getEntityId() {
        return entityId;
    }

    /**
     * A list of credentials that the IDP uses for signing messages.
     * A trusted message should be signed with any one (or more) of these credentials.
     */
    List<Credential> getSigningCredentials() {
        return signingCredentials.get();
    }
}
