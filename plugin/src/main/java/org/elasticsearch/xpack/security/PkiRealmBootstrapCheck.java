/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.security.authc.RealmSettings;
import org.elasticsearch.xpack.security.authc.pki.PkiRealm;
import org.elasticsearch.xpack.security.transport.netty4.SecurityNetty4Transport;
import org.elasticsearch.xpack.ssl.SSLService;

import java.util.Map;

import static org.elasticsearch.xpack.XPackSettings.HTTP_SSL_ENABLED;
import static org.elasticsearch.xpack.security.Security.setting;

class PkiRealmBootstrapCheck implements BootstrapCheck {

    private final SSLService sslService;
    private final Settings settings;

    PkiRealmBootstrapCheck(Settings settings, SSLService sslService) {
        this.settings = settings;
        this.sslService = sslService;
    }

    /**
     * If a PKI realm is enabled, checks to see if SSL and Client authentication are enabled on at
     * least one network communication layer.
     */
    @Override
    public boolean check() {
        final boolean pkiRealmEnabled = settings.getGroups(RealmSettings.PREFIX).values().stream()
                .filter(s -> PkiRealm.TYPE.equals(s.get("type")))
                .anyMatch(s -> s.getAsBoolean("enabled", true));
        if (pkiRealmEnabled) {
            // HTTP
            final boolean httpSsl = HTTP_SSL_ENABLED.get(settings);
            Settings httpSSLSettings = SSLService.getHttpTransportSSLSettings(settings);
            final boolean httpClientAuth = sslService.isSSLClientAuthEnabled(httpSSLSettings);
            if (httpSsl && httpClientAuth) {
                return false;
            }

            // Default Transport
            final Settings transportSSLSettings = settings.getByPrefix(setting("transport.ssl."));
            final boolean clientAuthEnabled = sslService.isSSLClientAuthEnabled(transportSSLSettings);
            if (clientAuthEnabled) {
                return false;
            }

            // Transport Profiles
            Map<String, Settings> groupedSettings = settings.getGroups("transport.profiles.");
            for (Map.Entry<String, Settings> entry : groupedSettings.entrySet()) {
                if (sslService.isSSLClientAuthEnabled(SecurityNetty4Transport.profileSslSettings(entry.getValue()), transportSSLSettings)) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String errorMessage() {
        return "A PKI realm is enabled but cannot be used as neither HTTP or Transport have SSL and client authentication enabled";
    }

    @Override
    public boolean alwaysEnforce() {
        return true;
    }
}
