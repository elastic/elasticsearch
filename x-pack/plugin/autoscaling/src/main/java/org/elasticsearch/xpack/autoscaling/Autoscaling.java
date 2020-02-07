/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.Build;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
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
import org.elasticsearch.xpack.autoscaling.action.GetAutoscalingDecisionAction;
import org.elasticsearch.xpack.autoscaling.action.TransportGetAutoscalingDecisionAction;
import org.elasticsearch.xpack.autoscaling.rest.RestGetAutoscalingDecisionHandler;

import java.util.List;
import java.util.function.Supplier;

/**
 * Container class for autoscaling functionality.
 */
public class Autoscaling extends Plugin implements ActionPlugin {

    public static final Setting<Boolean> AUTOSCALING_ENABLED_SETTING = Setting.boolSetting(
        "xpack.autoscaling.enabled",
        false,
        Setting.Property.NodeScope
    );

    private final boolean enabled;

    public Autoscaling(final Settings settings) {
        this.enabled = AUTOSCALING_ENABLED_SETTING.get(settings);
    }

    /**
     * The settings defined by autoscaling.
     *
     * @return the settings
     */
    @Override
    public List<Setting<?>> getSettings() {
        if (isSnapshot()) {
            return List.of(AUTOSCALING_ENABLED_SETTING);
        } else {
            return List.of();
        }
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (enabled) {
            return List.of(new ActionHandler<>(GetAutoscalingDecisionAction.INSTANCE, TransportGetAutoscalingDecisionAction.class));
        } else {
            return List.of();
        }
    }

    @Override
    public List<RestHandler> getRestHandlers(
        final Settings settings,
        final RestController controller,
        final ClusterSettings clusterSettings,
        final IndexScopedSettings indexScopedSettings,
        final SettingsFilter settingsFilter,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final Supplier<DiscoveryNodes> nodesInCluster
    ) {
        if (enabled) {
            return List.of(new RestGetAutoscalingDecisionHandler(controller));
        } else {
            return List.of();
        }
    }

    boolean isSnapshot() {
        return Build.CURRENT.isSnapshot();
    }

}
