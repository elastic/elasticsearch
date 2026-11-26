/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.nio.file.Path;
import java.util.Map;

/**
 * Hosts {@link LogsDBPlugin} inside a {@link LocalStateCompositeXPackPlugin}, for tests that need logsdb together with x-pack.
 * <p>
 * Do not list the plain {@code XPackPlugin} in such tests. It keeps the SSL service in a static {@code SetOnce}, so a second node
 * started in the same JVM fails with {@code AlreadySetException}. The local state plugin keeps these services in instance fields.
 * {@link LogsDBPlugin} reads the license state and service from statics by default, which the local state plugin never fills in,
 * so this class redirects both to the hosting plugin.
 */
public class LogsDBLocalStateXPackPlugin extends LocalStateCompositeXPackPlugin {

    private final LogsDBPlugin logsDBPlugin;

    public LogsDBLocalStateXPackPlugin(final Settings settings, final Path configPath) {
        super(settings, configPath);
        logsDBPlugin = new LogsDBPlugin(settings) {
            @Override
            protected XPackLicenseState getLicenseState() {
                return LogsDBLocalStateXPackPlugin.this.getLicenseState();
            }

            @Override
            protected LicenseService getLicenseService() {
                return LogsDBLocalStateXPackPlugin.this.getLicenseService();
            }
        };
        plugins.add(logsDBPlugin);
    }

    /** The local state plugin does not forward mappers of hosted plugins, but the pattern_text tests need the logsdb mappers. */
    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return logsDBPlugin.getMappers();
    }
}
