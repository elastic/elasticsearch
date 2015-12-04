/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.license.ShieldLicenseState;
import org.elasticsearch.shield.support.AbstractShieldModule;

public class ShieldDisabledModule extends AbstractShieldModule {

    public ShieldDisabledModule(Settings settings) {
        super(settings);
    }

    @Override
    protected void configure(boolean clientMode) {
        assert !shieldEnabled : "shield disabled module should only get loaded with shield disabled";
        if (!clientMode) {
            // required by the shield info rest action (when shield is disabled)
            bind(ShieldLicenseState.class).toProvider(Providers.<ShieldLicenseState>of(null));
        }
    }
}
