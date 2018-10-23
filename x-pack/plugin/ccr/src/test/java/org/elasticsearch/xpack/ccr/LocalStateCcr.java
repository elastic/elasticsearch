/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.nio.file.Path;

public class LocalStateCcr extends LocalStateCompositeXPackPlugin {

    public LocalStateCcr(final Settings settings, final Path configPath) throws Exception {
        super(settings, configPath);

        plugins.add(new Ccr(settings, new CcrLicenseChecker(() -> true, () -> false)) {

            @Override
            protected XPackLicenseState getLicenseState() {
                return LocalStateCcr.this.getLicenseState();
            }

        });
    }

}

