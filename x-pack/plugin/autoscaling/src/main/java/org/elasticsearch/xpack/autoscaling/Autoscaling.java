/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
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
import org.elasticsearch.xpack.autoscaling.action.GetAutoscalingCapacityAction;
import org.elasticsearch.xpack.autoscaling.action.GetAutoscalingPolicyAction;
import org.elasticsearch.xpack.autoscaling.action.PutAutoscalingPolicyAction;
import org.elasticsearch.xpack.autoscaling.action.TransportDeleteAutoscalingPolicyAction;
import org.elasticsearch.xpack.autoscaling.action.TransportGetAutoscalingCapacityAction;
import org.elasticsearch.xpack.autoscaling.action.TransportGetAutoscalingPolicyAction;
import org.elasticsearch.xpack.autoscaling.action.TransportPutAutoscalingPolicyAction;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCalculateCapacityService;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderService;
import org.elasticsearch.xpack.autoscaling.capacity.FixedAutoscalingDeciderService;
import org.elasticsearch.xpack.autoscaling.capacity.memory.AutoscalingMemoryInfoService;
import org.elasticsearch.xpack.autoscaling.existence.FrozenExistenceDeciderService;
import org.elasticsearch.xpack.autoscaling.rest.RestDeleteAutoscalingPolicyHandler;
import org.elasticsearch.xpack.autoscaling.rest.RestGetAutoscalingCapacityHandler;
import org.elasticsearch.xpack.autoscaling.rest.RestGetAutoscalingPolicyHandler;
import org.elasticsearch.xpack.autoscaling.rest.RestPutAutoscalingPolicyHandler;
import org.elasticsearch.xpack.autoscaling.shards.FrozenShardsDeciderService;
import org.elasticsearch.xpack.autoscaling.storage.FrozenStorageDeciderService;
import org.elasticsearch.xpack.autoscaling.storage.ProactiveStorageDeciderService;
import org.elasticsearch.xpack.autoscaling.storage.ReactiveStorageDeciderService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Container class for autoscaling functionality.
 */
public class Autoscaling extends Plugin implements ActionPlugin, ExtensiblePlugin, AutoscalingExtension {

    static {
        final String property = System.getProperty("es.autoscaling_feature_flag_registered");
        if (property != null) {
            throw new IllegalArgumentException("es.autoscaling_feature_flag_registered is no longer supported");
        }
    }

    private final List<AutoscalingExtension> autoscalingExtensions;
    private final SetOnce<ClusterService> clusterService = new SetOnce<>();
    private final SetOnce<AllocationDeciders> allocationDeciders = new SetOnce<>();
    private final AutoscalingLicenseChecker autoscalingLicenseChecker;

    public Autoscaling() {
        this(new AutoscalingLicenseChecker());
    }

    Autoscaling(final AutoscalingLicenseChecker autoscalingLicenseChecker) {
        this.autoscalingExtensions = new ArrayList<>(List.of(this));
        this.autoscalingLicenseChecker = Objects.requireNonNull(autoscalingLicenseChecker);
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
        this.clusterService.set(clusterService);
        return List.of(
            new AutoscalingCalculateCapacityService.Holder(this),
            autoscalingLicenseChecker,
            new AutoscalingMemoryInfoService(clusterService, client)
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(AutoscalingMemoryInfoService.FETCH_TIMEOUT);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(GetAutoscalingCapacityAction.INSTANCE, TransportGetAutoscalingCapacityAction.class),
            new ActionHandler<>(DeleteAutoscalingPolicyAction.INSTANCE, TransportDeleteAutoscalingPolicyAction.class),
            new ActionHandler<>(GetAutoscalingPolicyAction.INSTANCE, TransportGetAutoscalingPolicyAction.class),
            new ActionHandler<>(PutAutoscalingPolicyAction.INSTANCE, TransportPutAutoscalingPolicyAction.class)
        );
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
        return List.of(
            new RestGetAutoscalingCapacityHandler(),
            new RestDeleteAutoscalingPolicyHandler(),
            new RestGetAutoscalingPolicyHandler(),
            new RestPutAutoscalingPolicyHandler()
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(Metadata.Custom.class, AutoscalingMetadata.NAME, AutoscalingMetadata::new),
            new NamedWriteableRegistry.Entry(NamedDiff.class, AutoscalingMetadata.NAME, AutoscalingMetadata.AutoscalingMetadataDiff::new),
            new NamedWriteableRegistry.Entry(
                AutoscalingDeciderResult.Reason.class,
                FixedAutoscalingDeciderService.NAME,
                FixedAutoscalingDeciderService.FixedReason::new
            ),
            new NamedWriteableRegistry.Entry(
                AutoscalingDeciderResult.Reason.class,
                ReactiveStorageDeciderService.NAME,
                ReactiveStorageDeciderService.ReactiveReason::new
            ),
            new NamedWriteableRegistry.Entry(
                AutoscalingDeciderResult.Reason.class,
                ProactiveStorageDeciderService.NAME,
                ProactiveStorageDeciderService.ProactiveReason::new
            ),
            new NamedWriteableRegistry.Entry(
                AutoscalingDeciderResult.Reason.class,
                FrozenShardsDeciderService.NAME,
                FrozenShardsDeciderService.FrozenShardsReason::new
            ),
            new NamedWriteableRegistry.Entry(
                AutoscalingDeciderResult.Reason.class,
                FrozenStorageDeciderService.NAME,
                FrozenStorageDeciderService.FrozenReason::new
            ),
            new NamedWriteableRegistry.Entry(
                AutoscalingDeciderResult.Reason.class,
                FrozenExistenceDeciderService.NAME,
                FrozenExistenceDeciderService.FrozenExistenceReason::new
            )
        );
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return List.of(
            new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(AutoscalingMetadata.NAME), AutoscalingMetadata::parse)
        );
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        loader.loadExtensions(AutoscalingExtension.class).forEach(autoscalingExtensions::add);
    }

    @Override
    public Collection<AutoscalingDeciderService> deciders() {
        assert allocationDeciders.get() != null;
        return List.of(
            new FixedAutoscalingDeciderService(),
            new ReactiveStorageDeciderService(
                clusterService.get().getSettings(),
                clusterService.get().getClusterSettings(),
                allocationDeciders.get()
            ),
            new ProactiveStorageDeciderService(
                clusterService.get().getSettings(),
                clusterService.get().getClusterSettings(),
                allocationDeciders.get()
            ),
            new FrozenShardsDeciderService(),
            new FrozenStorageDeciderService(),
            new FrozenExistenceDeciderService()
        );
    }

    public Set<AutoscalingDeciderService> createDeciderServices(AllocationDeciders allocationDeciders) {
        this.allocationDeciders.set(allocationDeciders);
        return autoscalingExtensions.stream().flatMap(p -> p.deciders().stream()).collect(Collectors.toSet());
    }

}
