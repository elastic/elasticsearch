/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;

@ESIntegTestCase.ClusterScope(scope = SUITE, numDataNodes = 0, numClientNodes = 0, maxNumDataNodes = 0)
public class SettingsTests extends ESIntegTestCase {
    /**
     * Configure the node to use all settings the ESQL plugin permits.
     */
    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));

        Setting<Integer> truncationDefaultSize = EsqlPlugin.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE;
        Setting<Integer> truncationMaxSize = EsqlPlugin.QUERY_RESULT_TRUNCATION_MAX_SIZE;

        settings.put(truncationDefaultSize.getKey(), truncationDefaultSize.getDefault(null));
        settings.put(truncationMaxSize.getKey(), truncationMaxSize.getDefault(null));

        return settings.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(EsqlPlugin.class);
    }

    public void testSettingsAreValid() {
        // Noop; we only need to assure that a node can start with the given settings.
    }
}
