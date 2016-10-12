/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.security.support.AbstractSecurityModule;

public class SecurityRestModule extends AbstractSecurityModule.Node {

    public SecurityRestModule(Settings settings) {
        super(settings);
    }

    @Override
    protected void configureNode() {
        bind(SecurityRestFilter.class).asEagerSingleton();
    }
}
