/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.PreProcessModule;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.shield.license.LicenseService;
import org.elasticsearch.shield.rest.action.RestShieldInfoAction;
import org.elasticsearch.shield.support.AbstractShieldModule;

public class ShieldDisabledModule extends AbstractShieldModule implements PreProcessModule {

    public ShieldDisabledModule(Settings settings) {
        super(settings);
    }

    @Override
    protected void configure(boolean clientMode) {
        assert !shieldEnabled : "shield disabled module should only get loaded with shield disabled";
        if (!clientMode) {
            // required by the shield info rest action (when shield is disabled)
            bind(LicenseService.class).toProvider(Providers.<LicenseService>of(null));
        }
    }

    @Override
    public void processModule(Module module) {
        if (module instanceof RestModule) {
            //we want to expose the shield rest action even when the plugin is disabled
            ((RestModule) module).addRestAction(RestShieldInfoAction.class);
        }
    }
}
