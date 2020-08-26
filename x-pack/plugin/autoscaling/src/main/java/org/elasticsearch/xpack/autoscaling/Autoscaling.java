/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Build;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.autoscaling.action.DeleteAutoscalingPolicyAction;
import org.elasticsearch.xpack.autoscaling.action.GetAutoscalingDecisionAction;
import org.elasticsearch.xpack.autoscaling.action.GetAutoscalingPolicyAction;
import org.elasticsearch.xpack.autoscaling.action.PutAutoscalingPolicyAction;
import org.elasticsearch.xpack.autoscaling.action.TransportDeleteAutoscalingPolicyAction;
import org.elasticsearch.xpack.autoscaling.action.TransportGetAutoscalingDecisionAction;
import org.elasticsearch.xpack.autoscaling.action.TransportGetAutoscalingPolicyAction;
import org.elasticsearch.xpack.autoscaling.action.TransportPutAutoscalingPolicyAction;
import org.elasticsearch.xpack.autoscaling.decision.AlwaysAutoscalingDeciderConfiguration;
import org.elasticsearch.xpack.autoscaling.decision.AlwaysAutoscalingDeciderService;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderConfiguration;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderService;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecisionService;
import org.elasticsearch.xpack.autoscaling.rest.RestDeleteAutoscalingPolicyHandler;
import org.elasticsearch.xpack.autoscaling.rest.RestGetAutoscalingDecisionHandler;
import org.elasticsearch.xpack.autoscaling.rest.RestGetAutoscalingPolicyHandler;
import org.elasticsearch.xpack.autoscaling.rest.RestPutAutoscalingPolicyHandler;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Container class for autoscaling functionality.
 */
public class Autoscaling extends Plugin implements ActionPlugin, ExtensiblePlugin, AutoscalingExtension {
    private static final Logger logger = LogManager.getLogger(AutoscalingExtension.class);
    private static final Boolean AUTOSCALING_FEATURE_FLAG_REGISTERED;

    static {
        final String property = System.getProperty("es.autoscaling_feature_flag_registered");
        if (Build.CURRENT.isSnapshot() && property != null) {
            throw new IllegalArgumentException("es.autoscaling_feature_flag_registered is only supported in non-snapshot builds");
        }
        if ("true".equals(property)) {
            AUTOSCALING_FEATURE_FLAG_REGISTERED = true;
        } else if ("false".equals(property)) {
            AUTOSCALING_FEATURE_FLAG_REGISTERED = false;
        } else if (property == null) {
            AUTOSCALING_FEATURE_FLAG_REGISTERED = null;
        } else {
            throw new IllegalArgumentException(
                "expected es.autoscaling_feature_flag_registered to be unset or [true|false] but was [" + property + "]"
            );
        }
    }

    public static final Setting<Boolean> AUTOSCALING_ENABLED_SETTING = Setting.boolSetting(
        "xpack.autoscaling.enabled",
        false,
        Setting.Property.NodeScope
    );

    private final boolean enabled;

    private final List<AutoscalingExtension> autoscalingExtensions;

    public Autoscaling(final Settings settings) {
        this.enabled = AUTOSCALING_ENABLED_SETTING.get(settings);
        this.autoscalingExtensions = new ArrayList<>(List.of(this));
    }

    /**
     * The settings defined by autoscaling.
     *
     * @return the settings
     */
    @Override
    public List<Setting<?>> getSettings() {
        if (isSnapshot() || (AUTOSCALING_FEATURE_FLAG_REGISTERED != null && AUTOSCALING_FEATURE_FLAG_REGISTERED)) {
            return List.of(AUTOSCALING_ENABLED_SETTING);
        } else {
            return List.of();
        }
    }

    boolean isSnapshot() {
        return Build.CURRENT.isSnapshot();
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        return List.of(new AutoscalingDecisionService.Holder(this));
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (enabled) {
            return List.of(
                new ActionHandler<>(GetAutoscalingDecisionAction.INSTANCE, TransportGetAutoscalingDecisionAction.class),
                new ActionHandler<>(DeleteAutoscalingPolicyAction.INSTANCE, TransportDeleteAutoscalingPolicyAction.class),
                new ActionHandler<>(GetAutoscalingPolicyAction.INSTANCE, TransportGetAutoscalingPolicyAction.class),
                new ActionHandler<>(PutAutoscalingPolicyAction.INSTANCE, TransportPutAutoscalingPolicyAction.class)
            );
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
            return List.of(
                new RestGetAutoscalingDecisionHandler(),
                new RestDeleteAutoscalingPolicyHandler(),
                new RestGetAutoscalingPolicyHandler(),
                new RestPutAutoscalingPolicyHandler()
            );
        } else {
            return List.of();
        }
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(Metadata.Custom.class, AutoscalingMetadata.NAME, AutoscalingMetadata::new),
            new NamedWriteableRegistry.Entry(NamedDiff.class, AutoscalingMetadata.NAME, AutoscalingMetadata.AutoscalingMetadataDiff::new),
            new NamedWriteableRegistry.Entry(
                AutoscalingDeciderConfiguration.class,
                AlwaysAutoscalingDeciderConfiguration.NAME,
                AlwaysAutoscalingDeciderConfiguration::new
            )
        );
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return List.of(
            new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(AutoscalingMetadata.NAME), AutoscalingMetadata::parse),
            new NamedXContentRegistry.Entry(
                AutoscalingDeciderConfiguration.class,
                new ParseField(AlwaysAutoscalingDeciderConfiguration.NAME),
                AlwaysAutoscalingDeciderConfiguration::parse
            )
        );
    }

    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        loader.loadExtensions(AutoscalingExtension.class).forEach(autoscalingExtensions::add);
    }

    @Override
    public Collection<AutoscalingDeciderService<? extends AutoscalingDeciderConfiguration>> deciders() {
        return List.of(new AlwaysAutoscalingDeciderService());
    }

    public Set<AutoscalingDeciderService<? extends AutoscalingDeciderConfiguration>> createDeciderServices() {
        return autoscalingExtensions.stream().flatMap(p -> p.deciders().stream()).collect(Collectors.toSet());
    }
}
