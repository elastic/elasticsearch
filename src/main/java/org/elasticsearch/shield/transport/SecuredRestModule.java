/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.SecurityFilter;
import org.elasticsearch.shield.support.AbstractShieldModule;

/**
 *
 */
public class SecuredRestModule extends AbstractShieldModule.Node {

    public SecuredRestModule(Settings settings) {
        super(settings);
    }

    @Override
    protected void configureNode() {
        bind(SecurityFilter.Rest.class).asEagerSingleton();
    }
}
