/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.signature;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.support.AbstractShieldModule;

/**
 *
 */
public class SignatureModule extends AbstractShieldModule.Node {

    public SignatureModule(Settings settings) {
        super(settings);
    }

    @Override
    protected void configureNode() {
        bind(InternalSignatureService.class).asEagerSingleton();
        bind(SignatureService.class).to(InternalSignatureService.class).asEagerSingleton();
    }
}
