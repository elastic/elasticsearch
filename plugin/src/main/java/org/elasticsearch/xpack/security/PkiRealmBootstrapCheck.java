/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.security.authc.RealmSettings;
import org.elasticsearch.xpack.security.authc.pki.PkiRealm;
import org.elasticsearch.xpack.security.transport.netty4.SecurityNetty4Transport;
import org.elasticsearch.xpack.ssl.SSLService;

import java.util.Map;

import static org.elasticsearch.xpack.XPackSettings.HTTP_SSL_ENABLED;
import static org.elasticsearch.xpack.security.SecurityField.setting;

class PkiRealmBootstrapCheck implements BootstrapCheck {

    private final SSLService sslService;

    PkiRealmBootstrapCheck(SSLService sslService) {
        this.sslService = sslService;
    }

    /**
     * If a PKI realm is enabled, checks to see if SSL and Client authentication are enabled on at
     * least one network communication layer.
     */
    @Override
    public BootstrapCheckResult check(BootstrapContext context) {
        final Settings settings = context.settings;
        final boolean pkiRealmEnabled = settings.getGroups(RealmSettings.PREFIX).values().stream()
                .filter(s -> PkiRealm.TYPE.equals(s.get("type")))
                .anyMatch(s -> s.getAsBoolean("enabled", true));
        if (pkiRealmEnabled) {
            // HTTP
            final boolean httpSsl = HTTP_SSL_ENABLED.get(settings);
            Settings httpSSLSettings = SSLService.getHttpTransportSSLSettings(settings);
            final boolean httpClientAuth = sslService.isSSLClientAuthEnabled(httpSSLSettings);
            if (httpSsl && httpClientAuth) {
                return BootstrapCheckResult.success();
            }

            // Default Transport
            final boolean transportSSLEnabled = XPackSettings.TRANSPORT_SSL_ENABLED.get(settings);
            final Settings transportSSLSettings = settings.getByPrefix(setting("transport.ssl."));
            final boolean clientAuthEnabled = sslService.isSSLClientAuthEnabled(transportSSLSettings);
            if (transportSSLEnabled && clientAuthEnabled) {
                return BootstrapCheckResult.success();
            }

            // Transport Profiles
            Map<String, Settings> groupedSettings = settings.getGroups("transport.profiles.");
            for (Map.Entry<String, Settings> entry : groupedSettings.entrySet()) {
                if (transportSSLEnabled && sslService.isSSLClientAuthEnabled(
                        SecurityNetty4Transport.profileSslSettings(entry.getValue()), transportSSLSettings)) {
                    return BootstrapCheckResult.success();
                }
            }
            return BootstrapCheckResult.failure(
                    "a PKI realm is enabled but cannot be used as neither HTTP or Transport have SSL and client authentication enabled");
        } else {
            return BootstrapCheckResult.success();
        }
    }

    @Override
    public boolean alwaysEnforce() {
        return true;
    }
}
