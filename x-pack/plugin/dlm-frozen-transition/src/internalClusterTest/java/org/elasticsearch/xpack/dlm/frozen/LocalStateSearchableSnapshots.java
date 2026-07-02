/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;

import java.nio.file.Path;
import java.util.Collection;
import java.util.function.Supplier;

/**
 * Local-state wrapper for the searchable snapshots plugin, used in integration tests.
 * This mirrors the pattern from the searchable-snapshots module's own integ tests.
 */
public class LocalStateSearchableSnapshots extends LocalStateCompositeXPackPlugin implements SystemIndexPlugin {

    private final SearchableSnapshots plugin;
    private final DLMFrozenTransitionPlugin dlmFrozenTransitionPlugin;

    public LocalStateSearchableSnapshots(final Settings settings, final Path configPath) {
        super(settings, configPath);
        this.plugin = new SearchableSnapshots(settings) {

            @Override
            protected XPackLicenseState getLicenseState() {
                return LocalStateSearchableSnapshots.this.getLicenseState();
            }

        };
        plugins.add(plugin);

        this.dlmFrozenTransitionPlugin = new DLMFrozenTransitionPlugin() {
            @Override
            protected Supplier<XPackLicenseState> getLicenseStateSupplier() {
                return LocalStateSearchableSnapshots.this::getLicenseState;
            }
        };
        plugins.add(dlmFrozenTransitionPlugin);
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
