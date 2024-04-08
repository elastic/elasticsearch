/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.esql.heap_attack;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class HeapAttackPlugin extends Plugin implements ActionPlugin {
    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        NamedWriteableRegistry namedWriteableRegistry,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        return List.of(new RestTriggerOutOfMemoryAction());
    }

    // Deliberately unregistered, only used in unit tests. Copied to AbstractSimpleTransportTestCase#IGNORE_DESERIALIZATION_ERRORS_SETTING
    // so that tests in other packages can see it too.
    static final Setting<Boolean> IGNORE_DESERIALIZATION_ERRORS_SETTING = Setting.boolSetting(
        "transport.ignore_deserialization_errors",
        true,
        Setting.Property.NodeScope
    );

    @Override
    public List<Setting<?>> getSettings() {
        return CollectionUtils.appendToCopy(super.getSettings(), IGNORE_DESERIALIZATION_ERRORS_SETTING);
    }

    @Override
    public Settings additionalSettings() {
        return Settings.builder().put(super.additionalSettings()).put(IGNORE_DESERIALIZATION_ERRORS_SETTING.getKey(), true).build();
    }
}
