/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.RealmConfig.RealmIdentifier;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.pki.PkiRealmSettings;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.core.XPackSettings.HTTP_SSL_ENABLED;
import static org.elasticsearch.xpack.core.security.SecurityField.setting;

class PkiRealmBootstrapCheck implements BootstrapCheck {

    private final SSLService sslService;

    PkiRealmBootstrapCheck(SSLService sslService) {
        this.sslService = sslService;
    }

    /**
     * If a PKI realm is enabled, and does not support delegation(default), checks:
     *   1. if SSL and Client authentication are enabled on at least one network communication layer
     *   2. if the PKI realm does not have trust configuration, all network layers with client authentication enabled must also have
     *   certificate verification
     */
    @Override
    public BootstrapCheckResult check(BootstrapContext context) {
        BootstrapCheckResult clientAuthCheck = checkForClientAuthnForRealmsWithoutDelegation(context);
        if (clientAuthCheck.isFailure()) {
            return clientAuthCheck;
        }
        return checkForVerificationModeForRealmsWithoutTrustConfig(context);
    }

    /**
     * PKI realms (enabled and without delegation) only work when there is at least one network layer (HTTP or transport) that has client
     * authentication enabled. Otherwise, no network layer extracts the certificate to be verified by the realm, and the realm is
     * ineffective.
     */
    private BootstrapCheckResult checkForClientAuthnForRealmsWithoutDelegation(BootstrapContext context) {
        final Settings settings = context.settings();
        final Map<RealmIdentifier, Settings> realms = RealmSettings.getRealmSettings(settings);
        final Stream<Map.Entry<RealmIdentifier, Settings>> pkiRealmsEnabledWithoutDelegation = realms.entrySet().stream()
                .filter(e -> PkiRealmSettings.TYPE.equals(e.getKey().getType()))
                .filter(e -> e.getValue().getAsBoolean("enabled", true))
                .filter(e -> false == e.getValue().getAsBoolean("delegation.enabled", false));
        if (pkiRealmsEnabledWithoutDelegation.findAny().isPresent()) {
            for (String contextName : getSslContextNames(settings)) {
                final SSLConfiguration configuration = sslService.getSSLConfiguration(contextName);
                if (sslService.isSSLClientAuthEnabled(configuration)) {
                    return BootstrapCheckResult.success();
                }
            }
            return BootstrapCheckResult.failure(
                    "a PKI realm is enabled but cannot be used as neither HTTP or Transport have SSL and client authentication enabled");
        }
        return BootstrapCheckResult.success();
    }

    /** PKI realms (enabled and without delegation) can be configured with no trust configuration but all the network layers (HTTP and
     * transport) that extract the certificate, must verify the certificate, otherwise any valid private key and certificate pair can be
     * used to authenticate with the PKI realm.
     */
    private BootstrapCheckResult checkForVerificationModeForRealmsWithoutTrustConfig(BootstrapContext context) {
        final Settings settings = context.settings();
        final Map<RealmIdentifier, Settings> realms = RealmSettings.getRealmSettings(settings);
        final Stream<Map.Entry<RealmIdentifier, Settings>> pkiRealmsEnabledWithoutDelegation = realms.entrySet().stream()
                .filter(e -> PkiRealmSettings.TYPE.equals(e.getKey().getType()))
                .filter(e -> e.getValue().getAsBoolean("enabled", true))
                .filter(e -> false == e.getValue().getAsBoolean("delegation.enabled", false));
        // at least one realm with no trust config
        if (pkiRealmsEnabledWithoutDelegation.anyMatch(e -> (false == e.getValue().hasValue("ssl.certificate_authorities"))
                && (false == e.getValue().hasValue("ssl.truststore.path")))) {
            for (String contextName : getSslContextNames(settings)) {
                final SSLConfiguration configuration = sslService.getSSLConfiguration(contextName);
                // client auth WITH certificate verification disabled on the network layer
                if (sslService.isSSLClientAuthEnabled(configuration) &&
                        (false == configuration.verificationMode().isCertificateVerificationEnabled())) {
                    return BootstrapCheckResult.failure(
                            "a PKI realm without trust configuration is enabled but it is not secure to use it when certificate " +
                                    "verification is disabled for HTTP or Transport");
                }
            }
        }
        return BootstrapCheckResult.success();
    }

    private List<String> getSslContextNames(Settings settings) {
        final List<String> list = new ArrayList<>();
        if (HTTP_SSL_ENABLED.get(settings)) {
            list.add(setting("http.ssl"));
        }
        // TODO remove this because transport client is gone in 8 so PKI authn is not an option on the transport layer
        if (XPackSettings.TRANSPORT_SSL_ENABLED.get(settings)) {
            list.add(setting("transport.ssl"));
            list.addAll(sslService.getTransportProfileContextNames());
        }

        return list;
    }

    // FIXME this is an antipattern move this out of a bootstrap check!
    @Override
    public boolean alwaysEnforce() {
        return true;
    }
}
