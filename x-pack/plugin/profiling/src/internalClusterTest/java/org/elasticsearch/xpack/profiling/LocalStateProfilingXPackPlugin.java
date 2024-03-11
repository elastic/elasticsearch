/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.nio.file.Path;

public class LocalStateProfilingXPackPlugin extends LocalStateCompositeXPackPlugin {
    public LocalStateProfilingXPackPlugin(final Settings settings, final Path configPath) {
        super(settings, configPath);
        plugins.add(new ProfilingPlugin(settings) {
            @Override
            protected ProfilingLicenseChecker createLicenseChecker() {
                return new ProfilingLicenseChecker(LocalStateProfilingXPackPlugin.this::getLicenseState);
            }
        });
    }
}
