/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.n2n;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;

/**
 *
 */
public class N2NAuthModule extends AbstractModule {

    private final Settings settings;

    public N2NAuthModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {

    }
}
