/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.simulatedlatencyrepo;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;

import java.nio.file.Path;
import java.util.Collection;

public class LocalStateSearchableSnapshots extends LocalStateCompositeXPackPlugin implements SystemIndexPlugin {

    private final SearchableSnapshots plugin;

    public LocalStateSearchableSnapshots(final Settings settings, final Path configPath) {
        super(settings, configPath);
        this.plugin = new SearchableSnapshots(settings) {

            @Override
            protected XPackLicenseState getLicenseState() {
                return new XPackLicenseState(
                    () -> getEpochMillisSupplier().getAsLong(),
                    new XPackLicenseStatus(License.OperationMode.TRIAL, true, null)
                );
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
