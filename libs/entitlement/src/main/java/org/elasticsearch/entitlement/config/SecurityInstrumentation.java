/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.config;

import org.elasticsearch.entitlement.rules.EntitlementRulesBuilder;
import org.elasticsearch.entitlement.rules.Policies;
import org.elasticsearch.entitlement.runtime.registry.InternalInstrumentationRegistry;

import java.io.File;
import java.security.KeyStore;
import java.security.Provider;
import java.security.cert.CertStore;
import java.security.cert.CertStoreParameters;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

public class SecurityInstrumentation implements InstrumentationConfig {
    @Override
    public void init(InternalInstrumentationRegistry registry) {
        EntitlementRulesBuilder builder = new EntitlementRulesBuilder(registry);

        builder.on(SSLContext.class)
            .callingVoidStatic(SSLContext::setDefault, SSLContext.class)
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled();

        builder.on(HttpsURLConnection.class)
            .callingVoidStatic(HttpsURLConnection::setDefaultSSLSocketFactory, SSLSocketFactory.class)
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled()
            .callingVoidStatic(HttpsURLConnection::setDefaultHostnameVerifier, HostnameVerifier.class)
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled()
            .callingVoid(HttpsURLConnection::setSSLSocketFactory, SSLSocketFactory.class)
            .enforce(Policies::setHttpsConnectionProperties)
            .elseThrowNotEntitled();

        builder.on(KeyStore.class)
            .callingStatic(KeyStore::getInstance, File.class, char[].class)
            .enforce((file) -> Policies.fileRead(file))
            .elseThrowNotEntitled()
            .callingStatic(KeyStore::getInstance, File.class, KeyStore.LoadStoreParameter.class)
            .enforce((file) -> Policies.fileRead(file))
            .elseThrowNotEntitled();

        builder.on(KeyStore.Builder.class)
            .callingStatic(KeyStore.Builder::newInstance, File.class, KeyStore.ProtectionParameter.class)
            .enforce((file) -> Policies.fileRead(file))
            .elseThrowNotEntitled()
            .callingStatic(KeyStore.Builder::newInstance, String.class, Provider.class, File.class, KeyStore.ProtectionParameter.class)
            .enforce((_, _, file) -> Policies.fileRead(file))
            .elseThrowNotEntitled()
            .calling(KeyStore.Builder::getKeyStore)
            .enforce(Policies::fileDescriptorRead)
            .elseThrowNotEntitled();

        builder.on(CertStore.class)
            .callingStatic(CertStore::getInstance, String.class, CertStoreParameters.class)
            .enforce((type) -> "LDAP".equals(type) ? Policies.outboundNetworkAccess() : Policies.noop())
            .elseThrowNotEntitled();
    }
}
