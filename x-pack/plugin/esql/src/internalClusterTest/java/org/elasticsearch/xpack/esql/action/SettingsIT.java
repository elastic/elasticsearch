/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.client.internal.ClusterAdminClient;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.util.Collection;
import java.util.Collections;

public class SettingsIT extends AbstractEsqlIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(EsqlPlugin.class);
    }

    public void testSettingsAreDynamic() {
        Setting<Integer> truncationDefaultSize = EsqlPlugin.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE;
        Setting<Integer> truncationMaxSize = EsqlPlugin.QUERY_RESULT_TRUNCATION_MAX_SIZE;

        ClusterAdminClient client = admin().cluster();

        Settings.Builder settings = Settings.builder();
        settings.put(truncationDefaultSize.getKey(), truncationDefaultSize.getDefault(null) - 1);
        settings.put(truncationMaxSize.getKey(), truncationMaxSize.getDefault(null) - 1);

        ClusterUpdateSettingsRequest settingsRequest = new ClusterUpdateSettingsRequest().persistentSettings(settings.build());
        // This fails unless settings are declared dynamic.
        client.updateSettings(settingsRequest).actionGet();

        clearPersistentSettings(truncationDefaultSize, truncationMaxSize);
    }

    private void clearPersistentSettings(Setting<?>... settings) {
        Settings.Builder clearedSettings = Settings.builder();

        for (Setting<?> s : settings) {
            clearedSettings.putNull(s.getKey());
        }

        var claerSettingsRequest = new ClusterUpdateSettingsRequest().persistentSettings(clearedSettings.build());
        admin().cluster().updateSettings(claerSettingsRequest).actionGet();
    }
}
