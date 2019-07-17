/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.util.Collection;
import java.util.Collections;

public class SpatialPlugin extends Plugin {

    public SpatialPlugin(Settings settings) {
    }

    public Collection<Module> createGuiceModules() {
        return Collections.singletonList(b -> {
            XPackPlugin.bindFeatureSet(b, SpatialFeatureSet.class);
        });
    }
}
