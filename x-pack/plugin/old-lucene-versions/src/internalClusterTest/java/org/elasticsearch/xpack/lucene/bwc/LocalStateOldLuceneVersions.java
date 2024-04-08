/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.nio.file.Path;

public class LocalStateOldLuceneVersions extends LocalStateCompositeXPackPlugin {

    private final OldLuceneVersions plugin;

    public LocalStateOldLuceneVersions(final Settings settings, final Path configPath) {
        super(settings, configPath);
        this.plugin = new OldLuceneVersions() {

            @Override
            protected XPackLicenseState getLicenseState() {
                return LocalStateOldLuceneVersions.this.getLicenseState();
            }

        };
        plugins.add(plugin);
    }
}
