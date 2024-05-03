/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.nio.file.Path;

public class NonCompliantLicenseLocalStateCcr extends LocalStateCompositeXPackPlugin {

    public NonCompliantLicenseLocalStateCcr(final Settings settings, final Path configPath) throws Exception {
        super(settings, configPath);

        plugins.add(new Ccr(settings, new CcrLicenseChecker(() -> false, () -> false)) {

        });
    }

}
