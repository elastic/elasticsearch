/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.opensaml.security.x509.X509Credential;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.REQUESTED_AUTHN_CONTEXT_CLASS_REF;
import static org.elasticsearch.xpack.core.security.authc.saml.SingleSpSamlRealmSettings.SP_ACS;
import static org.elasticsearch.xpack.core.security.authc.saml.SingleSpSamlRealmSettings.SP_ENTITY_ID;
import static org.elasticsearch.xpack.core.security.authc.saml.SingleSpSamlRealmSettings.SP_LOGOUT;
import static org.elasticsearch.xpack.security.authc.saml.SamlRealm.buildEncryptionCredential;
import static org.elasticsearch.xpack.security.authc.saml.SamlRealm.buildSigningConfiguration;
import static org.elasticsearch.xpack.security.authc.saml.SamlRealm.require;

public class SingleSamlSpConfiguration implements SpConfiguration {
    private final String entityId;
    private final String ascUrl;
    private final String logoutUrl;
    private final SigningConfiguration signingConfiguration;
    private final List<String> reqAuthnCtxClassRef;
    private final List<X509Credential> encryptionCredentials;

    // Visible for testing
    SingleSamlSpConfiguration(
        final String entityId,
        final String ascUrl,
        final String logoutUrl,
        final SigningConfiguration signingConfiguration,
        final List<X509Credential> encryptionCredentials,
        final List<String> reqAuthnCtxClassRef
    ) {
        this.entityId = entityId;
        this.ascUrl = ascUrl;
        this.logoutUrl = logoutUrl;
        this.signingConfiguration = signingConfiguration;
        this.reqAuthnCtxClassRef = reqAuthnCtxClassRef;
        this.encryptionCredentials = encryptionCredentials;
    }

    public static SingleSamlSpConfiguration create(RealmConfig realmConfig) throws GeneralSecurityException, IOException {
        List<X509Credential> encryptionCredential = buildEncryptionCredential(realmConfig);

        return new SingleSamlSpConfiguration(
            require(realmConfig, SP_ENTITY_ID),
            require(realmConfig, SP_ACS),
            realmConfig.getSetting(SP_LOGOUT),
            buildSigningConfiguration(realmConfig),
            encryptionCredential != null ? encryptionCredential : Collections.<X509Credential>emptyList(),
            realmConfig.getSetting(REQUESTED_AUTHN_CONTEXT_CLASS_REF)
        );
    }

    @Override
    public String getEntityId() {
        return entityId;
    }

    @Override
    public String getAscUrl() {
        return ascUrl;
    }

    @Override
    public String getLogoutUrl() {
        return logoutUrl;
    }

    @Override
    public List<X509Credential> getEncryptionCredentials() {
        return encryptionCredentials;
    }

    @Override
    public SigningConfiguration getSigningConfiguration() {
        return signingConfiguration;
    }

    @Override
    public List<String> getReqAuthnCtxClassRef() {
        return reqAuthnCtxClassRef;
    }
}
