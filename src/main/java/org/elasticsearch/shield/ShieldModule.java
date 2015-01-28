/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.action.ShieldActionModule;
import org.elasticsearch.shield.audit.AuditTrailModule;
import org.elasticsearch.shield.authc.AuthenticationModule;
import org.elasticsearch.shield.authz.AuthorizationModule;
import org.elasticsearch.shield.signature.SignatureModule;
import org.elasticsearch.shield.rest.ShieldRestModule;
import org.elasticsearch.shield.license.LicenseModule;
import org.elasticsearch.shield.ssl.SSLModule;
import org.elasticsearch.shield.support.AbstractShieldModule;
import org.elasticsearch.shield.transport.ShieldTransportModule;

/**
 *
 */
public class ShieldModule extends AbstractShieldModule.Spawn {

    public ShieldModule(Settings settings) {
        super(settings);
    }

    @Override
    public Iterable<? extends Module> spawnModules(boolean clientMode) {
        assert shieldEnabled : "this module should get loaded only when shield is enabled";

        // spawn needed parts in client mode
        if (clientMode) {
            return ImmutableList.<Module>of(
                    new ShieldActionModule(settings),
                    new ShieldTransportModule(settings),
                    new SSLModule(settings));
        }

        return ImmutableList.<Module>of(
                new LicenseModule(settings),
                new AuthenticationModule(settings),
                new AuthorizationModule(settings),
                new AuditTrailModule(settings),
                new ShieldRestModule(settings),
                new ShieldActionModule(settings),
                new ShieldTransportModule(settings),
                new SignatureModule(settings),
                new SSLModule(settings));
    }

    @Override
    protected void configure(boolean clientMode) {
    }
}
