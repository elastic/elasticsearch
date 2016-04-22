/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.support.AbstractShieldModule;
import org.elasticsearch.xpack.XPackPlugin;

/**
 *
 */
public class SecurityModule extends AbstractShieldModule {

    private final SecurityLicenseState securityLicenseState;

    public SecurityModule(Settings settings, SecurityLicenseState securityLicenseState) {
        super(settings);
        this.securityLicenseState = securityLicenseState;
    }

    @Override
    protected void configure(boolean clientMode) {
        if (clientMode) {
            return;
        }

        if (securityLicenseState != null) {
            bind(SecurityLicenseState.class).toInstance(securityLicenseState);
        } else {
            bind(SecurityLicenseState.class).toProvider(Providers.<SecurityLicenseState>of(null));
        }

        XPackPlugin.bindFeatureSet(binder(), SecurityFeatureSet.class);

        if (shieldEnabled) {
            bind(SecurityContext.Secure.class).asEagerSingleton();
            bind(SecurityContext.class).to(SecurityContext.Secure.class);
            bind(ShieldLifecycleService.class).asEagerSingleton();
            bind(InternalClient.Secure.class).asEagerSingleton();
            bind(InternalClient.class).to(InternalClient.Secure.class);

        } else {
            bind(SecurityContext.class).toInstance(SecurityContext.Insecure.INSTANCE);
            bind(InternalClient.Insecure.class).asEagerSingleton();
            bind(InternalClient.class).to(InternalClient.Insecure.class);
        }
    }

}
