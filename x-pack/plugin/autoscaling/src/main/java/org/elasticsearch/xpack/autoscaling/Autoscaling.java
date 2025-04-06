/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.autoscaling.action.DeleteAutoscalingPolicyAction;
import org.elasticsearch.xpack.autoscaling.action.GetAutoscalingCapacityAction;
import org.elasticsearch.xpack.autoscaling.action.GetAutoscalingPolicyAction;
import org.elasticsearch.xpack.autoscaling.action.PutAutoscalingPolicyAction;
import org.elasticsearch.xpack.autoscaling.action.ReservedAutoscalingPolicyAction;
import org.elasticsearch.xpack.autoscaling.action.TransportDeleteAutoscalingPolicyAction;
import org.elasticsearch.xpack.autoscaling.action.TransportGetAutoscalingCapacityAction;
import org.elasticsearch.xpack.autoscaling.action.TransportGetAutoscalingPolicyAction;
import org.elasticsearch.xpack.autoscaling.action.TransportPutAutoscalingPolicyAction;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCalculateCapacityService;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderService;
import org.elasticsearch.xpack.autoscaling.capacity.FixedAutoscalingDeciderService;
import org.elasticsearch.xpack.autoscaling.capacity.nodeinfo.AutoscalingNodeInfoService;
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
import java.util.function.Predicate;
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

    static final LicensedFeature.Momentary AUTOSCALING_FEATURE = LicensedFeature.momentary(
        null,
        "autoscaling",
        License.OperationMode.ENTERPRISE
    );

    private final List<AutoscalingExtension> autoscalingExtensions;
    private final SetOnce<ClusterService> clusterServiceHolder = new SetOnce<>();
    private final SetOnce<AllocationService> allocationServiceHolder = new SetOnce<>();
    private final AutoscalingLicenseChecker autoscalingLicenseChecker;
    private final SetOnce<ReservedAutoscalingPolicyAction> reservedAutoscalingPolicyAction = new SetOnce<>();

    @SuppressWarnings("this-escape")
    public Autoscaling() {
        this(new AutoscalingLicenseChecker());
    }

    @SuppressWarnings("this-escape")
    Autoscaling(final AutoscalingLicenseChecker autoscalingLicenseChecker) {
        this.autoscalingExtensions = new ArrayList<>(List.of(this));
        this.autoscalingLicenseChecker = Objects.requireNonNull(autoscalingLicenseChecker);
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        this.clusterServiceHolder.set(services.clusterService());
        this.allocationServiceHolder.set(services.allocationService());
        var capacityServiceHolder = new AutoscalingCalculateCapacityService.Holder(this);
        this.reservedAutoscalingPolicyAction.set(new ReservedAutoscalingPolicyAction(capacityServiceHolder));
        return List.of(
            capacityServiceHolder,
            autoscalingLicenseChecker,
            new AutoscalingNodeInfoService(services.clusterService(), services.client())
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(AutoscalingNodeInfoService.FETCH_TIMEOUT);
    }

    @Override
    public List<ActionHandler> getActions() {
        return List.of(
            new ActionHandler(GetAutoscalingCapacityAction.INSTANCE, TransportGetAutoscalingCapacityAction.class),
            new ActionHandler(DeleteAutoscalingPolicyAction.INSTANCE, TransportDeleteAutoscalingPolicyAction.class),
            new ActionHandler(GetAutoscalingPolicyAction.INSTANCE, TransportGetAutoscalingPolicyAction.class),
            new ActionHandler(PutAutoscalingPolicyAction.INSTANCE, TransportPutAutoscalingPolicyAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        final Settings settings,
        NamedWriteableRegistry namedWriteableRegistry,
        final RestController controller,
        final ClusterSettings clusterSettings,
        final IndexScopedSettings indexScopedSettings,
        final SettingsFilter settingsFilter,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
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
            new NamedWriteableRegistry.Entry(Metadata.ClusterCustom.class, AutoscalingMetadata.NAME, AutoscalingMetadata::new),
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
            new NamedXContentRegistry.Entry(
                Metadata.ClusterCustom.class,
                new ParseField(AutoscalingMetadata.NAME),
                AutoscalingMetadata::parse
            )
        );
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        loader.loadExtensions(AutoscalingExtension.class).forEach(autoscalingExtensions::add);
    }

    @Override
    public Collection<AutoscalingDeciderService> deciders() {
        final var allocationService = allocationServiceHolder.get();
        assert allocationService != null;
        final ClusterService clusterService = clusterServiceHolder.get();
        return List.of(
            new FixedAutoscalingDeciderService(),
            new ReactiveStorageDeciderService(
                clusterService.getSettings(),
                clusterService.getClusterSettings(),
                allocationService.getAllocationDeciders(),
                allocationService.getShardRoutingRoleStrategy()
            ),
            new ProactiveStorageDeciderService(
                clusterService.getSettings(),
                clusterService.getClusterSettings(),
                allocationService.getAllocationDeciders(),
                allocationService.getShardRoutingRoleStrategy()
            ),
            new FrozenShardsDeciderService(),
            new FrozenStorageDeciderService(),
            new FrozenExistenceDeciderService()
        );
    }

    public Set<AutoscalingDeciderService> createDeciderServices() {
        return autoscalingExtensions.stream().flatMap(p -> p.deciders().stream()).collect(Collectors.toSet());
    }

    public Collection<ReservedClusterStateHandler<ClusterState, ?>> reservedClusterStateHandlers() {
        return Set.of(reservedAutoscalingPolicyAction.get());
    }
}
