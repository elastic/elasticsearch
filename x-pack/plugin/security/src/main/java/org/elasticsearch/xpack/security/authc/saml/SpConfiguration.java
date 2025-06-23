/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.saml;

import org.opensaml.security.x509.X509Credential;

import java.util.List;

/**
 * Configuration interface for a SAML Service Provider (SP).
 */
public interface SpConfiguration {

    /**
     * Returns the entity ID of the SAML Service Provider.
     *
     * @return the entity ID
     */
    String getEntityId();

    /**
     * Returns the Assertion Consumer Service (ACS) URL of the SAML Service Provider.
     *
     * @return the ACS URL
     */
    String getAscUrl();

    /**
     * Returns the URL for handling SAML logout requests.
     *
     * @return the logout URL
     */
    String getLogoutUrl();

    /**
     * Returns the list of X.509 credentials used for encryption.
     *
     * @return the encryption credentials
     */
    List<X509Credential> getEncryptionCredentials();

    /**
     * Returns the signing configuration for the SAML Service Provider.
     *
     * @return the signing configuration
     */
    SigningConfiguration getSigningConfiguration();

    /**
     * Returns the list of requested authentication context class references.
     *
     * @return the authentication context class references
     */
    List<String> getReqAuthnCtxClassRef();
}
