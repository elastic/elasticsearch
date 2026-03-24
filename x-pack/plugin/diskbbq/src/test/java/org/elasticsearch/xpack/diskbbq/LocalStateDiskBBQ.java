/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.diskbbq;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.vectors.VectorsFormatProvider;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.nio.file.Path;

public final class LocalStateDiskBBQ extends LocalStateCompositeXPackPlugin {
    private final DiskBBQPlugin plugin;

    public LocalStateDiskBBQ(final Settings settings, final Path configPath) {
        super(settings, configPath);
        LocalStateDiskBBQ thisVar = this;
        plugin = new DiskBBQPlugin(settings) {
            @Override
            protected XPackLicenseState getLicenseState() {
                return thisVar.getLicenseState();
            }
        };
        plugins.add(plugin);
    }

    @Override
    public VectorsFormatProvider getVectorsFormatProvider() {
        return plugin.getVectorsFormatProvider();
    }
}
