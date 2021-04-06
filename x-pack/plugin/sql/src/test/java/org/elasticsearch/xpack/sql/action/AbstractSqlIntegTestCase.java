/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;

@ESIntegTestCase.ClusterScope(scope = SUITE, numDataNodes = 0, numClientNodes = 0, maxNumDataNodes = 0)
public abstract class AbstractSqlIntegTestCase extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        settings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        settings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        settings.put(XPackSettings.GRAPH_ENABLED.getKey(), false);
        settings.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        settings.put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial");
        return settings.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(LocalStateSQLXPackPlugin.class);
    }
}

