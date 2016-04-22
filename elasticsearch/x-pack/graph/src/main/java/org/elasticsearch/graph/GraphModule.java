/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.graph;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.xpack.XPackPlugin;

/**
 *
 */
public class GraphModule extends AbstractModule {

    private final boolean enabled;
    private final boolean transportClientNode;

    public GraphModule(boolean enabled, boolean transportClientNode) {
        this.enabled = enabled;
        this.transportClientNode = transportClientNode;
    }

    @Override
    protected void configure() {
        XPackPlugin.bindFeatureSet(binder(), GraphFeatureSet.class);
        if (enabled && transportClientNode == false) {
            bind(GraphLicensee.class).asEagerSingleton();
        } else {
            bind(GraphLicensee.class).toProvider(Providers.of(null));
        }
    }

}
