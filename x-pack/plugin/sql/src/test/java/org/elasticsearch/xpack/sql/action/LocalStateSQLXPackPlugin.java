/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.sql.plugin.SqlLicenseChecker;
import org.elasticsearch.xpack.sql.plugin.SqlPlugin;

import java.nio.file.Path;

public class LocalStateSQLXPackPlugin extends LocalStateCompositeXPackPlugin {

    public LocalStateSQLXPackPlugin(Settings settings, Path configPath) throws Exception {
        super(settings, configPath);
        plugins.add(new SqlPlugin(true, new SqlLicenseChecker((mode) -> {})));
    }
}
