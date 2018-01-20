/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.common.Nullable;
import org.opensaml.security.x509.X509Credential;

/**
 * A simple container class that holds all configuration related to a SAML Service Provider (SP).
 */
public class SpConfiguration {

    private final String entityId;
    private final String ascUrl;
    private final String logoutUrl;
    private final SigningConfiguration signingConfiguration;
    private final X509Credential encryptionCredential;

    public SpConfiguration(String entityId, String ascUrl, String logoutUrl,
                    SigningConfiguration signingConfiguration, @Nullable X509Credential encryptionCredential) {
        this.entityId = entityId;
        this.ascUrl = ascUrl;
        this.logoutUrl = logoutUrl;
        this.signingConfiguration = signingConfiguration;
        this.encryptionCredential = encryptionCredential;
    }

    /**
     * The SAML identifier (as a URI) for the Sp
     */
    String getEntityId() {
        return entityId;
    }

    String getAscUrl() {
        return ascUrl;
    }

    String getLogoutUrl() {
        return logoutUrl;
    }

    X509Credential getEncryptionCredential() {
        return encryptionCredential;
    }

    SigningConfiguration getSigningConfiguration() {
        return signingConfiguration;
    }
}
