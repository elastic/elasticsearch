/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.nio.file.Path;
import java.util.Collection;

public class LocalStateSearchableSnapshots extends LocalStateCompositeXPackPlugin implements SystemIndexPlugin {

    private final SearchableSnapshots plugin;

    public LocalStateSearchableSnapshots(final Settings settings, final Path configPath) {
        super(settings, configPath);
        this.plugin = new SearchableSnapshots(settings) {

            @Override
            protected XPackLicenseState getLicenseState() {
                return LocalStateSearchableSnapshots.this.getLicenseState();
            }

        };
        plugins.add(plugin);
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return plugin.getSystemIndexDescriptors(settings);
    }

    @Override
    public String getFeatureName() {
        return plugin.getFeatureName();
    }

    @Override
    public String getFeatureDescription() {
        return plugin.getFeatureDescription();
    }
}
