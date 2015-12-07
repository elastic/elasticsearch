/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.rest;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.support.AbstractShieldModule;

/**
 *
 */
public class ShieldRestModule extends AbstractShieldModule.Node {

    public ShieldRestModule(Settings settings) {
        super(settings);
    }

    @Override
    protected void configureNode() {
        bind(ShieldRestFilter.class).asEagerSingleton();
    }
}
