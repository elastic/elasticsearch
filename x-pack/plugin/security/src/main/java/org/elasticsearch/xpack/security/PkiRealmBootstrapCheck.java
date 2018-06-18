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
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.pki.PkiRealmSettings;
import org.elasticsearch.xpack.core.security.transport.netty4.SecurityNetty4Transport;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.core.XPackSettings.HTTP_SSL_ENABLED;
import static org.elasticsearch.xpack.core.security.SecurityField.setting;

class PkiRealmBootstrapCheck implements BootstrapCheck {

    private final SSLService sslService;
    private final List<SSLConfiguration> sslConfigurations;

    PkiRealmBootstrapCheck(Settings settings, SSLService sslService) {
        this.sslService = sslService;
        this.sslConfigurations = loadSslConfigurations(settings);
    }

    /**
     * {@link SSLConfiguration} may depend on {@link org.elasticsearch.common.settings.SecureSettings} that can only be read during startup.
     * We need to preload these during component configuration.
     */
    private List<SSLConfiguration> loadSslConfigurations(Settings settings) {
        final List<SSLConfiguration> list = new ArrayList<>();
        if (HTTP_SSL_ENABLED.get(settings)) {
            list.add(sslService.sslConfiguration(SSLService.getHttpTransportSSLSettings(settings), Settings.EMPTY));
        }

        if (XPackSettings.TRANSPORT_SSL_ENABLED.get(settings)) {
            final Settings transportSslSettings = settings.getByPrefix(setting("transport.ssl."));
            list.add(sslService.sslConfiguration(transportSslSettings, Settings.EMPTY));

            settings.getGroups("transport.profiles.").values().stream()
                    .map(SecurityNetty4Transport::profileSslSettings)
                    .map(s -> sslService.sslConfiguration(s, transportSslSettings))
                    .forEach(list::add);
        }

        return list;
    }

    /**
     * If a PKI realm is enabled, checks to see if SSL and Client authentication are enabled on at
     * least one network communication layer.
     */
    @Override
    public BootstrapCheckResult check(BootstrapContext context) {
        final Settings settings = context.settings;
        final boolean pkiRealmEnabled = settings.getGroups(RealmSettings.PREFIX).values().stream()
                .filter(s -> PkiRealmSettings.TYPE.equals(s.get("type")))
                .anyMatch(s -> s.getAsBoolean("enabled", true));
        if (pkiRealmEnabled) {
            for (SSLConfiguration configuration : this.sslConfigurations) {
                if (sslService.isSSLClientAuthEnabled(configuration)) {
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
