/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.session.Cursors;

import java.util.Collections;

import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;

public class SqlPluginTests extends ESTestCase {

    public void testSqlDisabledIsNoOp() {
        Settings settings = Settings.builder().put("xpack.sql.enabled", false).build();
        SqlPlugin plugin = new SqlPlugin(settings);
        assertThat(plugin.createComponents(mock(Client.class), "cluster", new NamedWriteableRegistry(Cursors.getNamedWriteables())),
            hasSize(3));
        assertThat(plugin.getActions(), hasSize(8));
        assertThat(
            plugin.getRestHandlers(Settings.EMPTY, mock(RestController.class),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS, new SettingsFilter(Collections.emptyList()),
                mock(IndexNameExpressionResolver.class), () -> mock(DiscoveryNodes.class)),
            hasSize(7));
    }
}
