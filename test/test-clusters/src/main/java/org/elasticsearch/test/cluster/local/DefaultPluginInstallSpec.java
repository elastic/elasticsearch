/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.util.resource.Resource;

import java.util.function.Function;

public class DefaultPluginInstallSpec implements PluginInstallSpec {
    Function<? super String, ? extends Resource> propertiesOverride;
    Function<? super String, ? extends Resource> entitlementsOverride;

    @Override
    public PluginInstallSpec withPropertiesOverride(Function<? super String, ? extends Resource> override) {
        this.propertiesOverride = override;
        return this;
    }

    @Override
    public PluginInstallSpec withEntitlementsOverride(Function<? super String, ? extends Resource> override) {
        this.entitlementsOverride = override;
        return this;
    }
}
