/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.admin;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.support.AbstractShieldModule;

/**
 * TODO: document
 */
public class ShieldAdminModule extends AbstractShieldModule.Node {

    private ShieldInternalUserHolder userHolder;

    public ShieldAdminModule(Settings settings) {
        super(settings);
    }

    @Override
    protected void configureNode() {
        userHolder = new ShieldInternalUserHolder();
        bind(ShieldInternalUserHolder.class).toInstance(userHolder);
        bind(ShieldTemplateService.class).asEagerSingleton();
    }
}
