/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.nio.file.Path;

public class LocalStateSearchableSnapshots extends LocalStateCompositeXPackPlugin {

    public LocalStateSearchableSnapshots(final Settings settings, final Path configPath) {
        super(settings, configPath);
        LocalStateSearchableSnapshots thisVar = this;

        plugins.add(new SearchableSnapshots(settings) {

            @Override
            protected XPackLicenseState getLicenseState() {
                return thisVar.getLicenseState();
            }

        });
    }
}
