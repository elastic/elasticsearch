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

        // not sure if this was so to ensure license is enabled always or if it was to circumvent the old license state overriding.
//        plugins.add(new Ccr(settings, new CcrLicenseChecker(() -> true, () -> false)));
        plugins.add(new Ccr(settings));
    }

}

