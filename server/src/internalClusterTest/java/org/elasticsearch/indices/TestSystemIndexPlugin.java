/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;

import java.util.Collection;
import java.util.List;

/**
 * Just a test plugin to allow the test descriptor to be installed in the cluster.
 */
public class TestSystemIndexPlugin extends Plugin implements SystemIndexPlugin {
    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return List.of(new TestSystemIndexDescriptor(), new TestSystemIndexDescriptorAllowsTemplates());
    }

    @Override
    public String getFeatureName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public String getFeatureDescription() {
        return this.getClass().getCanonicalName();
    }
}
