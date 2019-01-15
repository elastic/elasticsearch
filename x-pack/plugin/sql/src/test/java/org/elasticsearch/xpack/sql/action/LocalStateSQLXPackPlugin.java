/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.sql.plugin.SqlPlugin;

import java.nio.file.Path;

public class LocalStateSQLXPackPlugin extends LocalStateCompositeXPackPlugin {

    public LocalStateSQLXPackPlugin(final Settings settings, final Path configPath) throws Exception {
        super(settings, configPath);
        LocalStateSQLXPackPlugin thisVar = this;
        plugins.add(new SqlPlugin(settings) {
            @Override
            protected XPackLicenseState getLicenseState() {
                return thisVar.getLicenseState();
            }
        });
    }
}
