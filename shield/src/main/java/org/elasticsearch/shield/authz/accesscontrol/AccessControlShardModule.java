/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.accesscontrol;

import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.IndexSearcherWrapper;
import org.elasticsearch.shield.support.AbstractShieldModule;

public class AccessControlShardModule extends AbstractShieldModule.Node {

    public AccessControlShardModule(Settings settings) {
        super(settings);
    }

    @Override
    protected void configureNode() {
        Multibinder<IndexSearcherWrapper> multibinder
                = Multibinder.newSetBinder(binder(), IndexSearcherWrapper.class);
        multibinder.addBinding().to(ShieldIndexSearcherWrapper.class);
    }
}
