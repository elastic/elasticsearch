/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.Build;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class ShutdownPlugin extends Plugin implements ActionPlugin {

    public static final String SHUTDOWN_FEATURE_ENABLED_FLAG = "es.shutdown_feature_flag_enabled";
    public static final Setting<Boolean> SHUTDOWN_FEATURE_ENABLED_FLAG_SETTING = Setting.boolSetting(
        SHUTDOWN_FEATURE_ENABLED_FLAG,
        (settings) -> {
            final String enabled = settings.get(SHUTDOWN_FEATURE_ENABLED_FLAG);
            // Enabled by default on snapshot builds, disabled on release builds
            if (Build.CURRENT.isSnapshot()) {
                if (enabled != null && enabled.equalsIgnoreCase("false")) {
                    return "false";
                } else {
                    return "true";
                }
            } else {
                if (enabled != null && enabled.equalsIgnoreCase("true")) {
                    throw new IllegalArgumentException("shutdown plugin may not be enabled on a non-snapshot build");
                } else {
                    return "false";
                }
            }
        },
        Setting.Property.NodeScope
    );

    public boolean isEnabled(Settings settings) {
        return SHUTDOWN_FEATURE_ENABLED_FLAG_SETTING.get(settings);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        ActionHandler<PutShutdownNodeAction.Request, AcknowledgedResponse> putShutdown = new ActionHandler<>(
            PutShutdownNodeAction.INSTANCE,
            TransportPutShutdownNodeAction.class
        );
        ActionHandler<DeleteShutdownNodeAction.Request, AcknowledgedResponse> deleteShutdown = new ActionHandler<>(
            DeleteShutdownNodeAction.INSTANCE,
            TransportDeleteShutdownNodeAction.class
        );
        ActionHandler<GetShutdownStatusAction.Request, GetShutdownStatusAction.Response> getStatus = new ActionHandler<>(
            GetShutdownStatusAction.INSTANCE,
            TransportGetShutdownStatusAction.class
        );
        return Arrays.asList(putShutdown, deleteShutdown, getStatus);
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        if (isEnabled(settings) == false) {
            return Collections.emptyList();
        }
        return Arrays.asList(new RestPutShutdownNodeAction(), new RestDeleteShutdownNodeAction(), new RestGetShutdownStatusAction());
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Collections.singletonList(SHUTDOWN_FEATURE_ENABLED_FLAG_SETTING);
    }
}
