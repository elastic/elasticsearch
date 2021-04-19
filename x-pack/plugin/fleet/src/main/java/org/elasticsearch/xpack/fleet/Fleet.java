/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor.Type;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.core.template.TemplateUtils;
import org.elasticsearch.xpack.fleet.action.GetGlobalCheckpointsAction;
import org.elasticsearch.xpack.fleet.action.GetGlobalCheckpointsShardAction;
import org.elasticsearch.xpack.fleet.rest.RestGetGlobalCheckpointsAction;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ClientHelper.FLEET_ORIGIN;

/**
 * A plugin to manage and provide access to the system indices used by Fleet.
 *
 * Currently only exposes general-purpose APIs on {@code _fleet}-prefixed routes, to be more specialized as Fleet's requirements stabilize.
 */
public class Fleet extends Plugin implements SystemIndexPlugin {

    private static final int CURRENT_INDEX_VERSION = 7;
    private static final String VERSION_KEY = "version";
    private static final String MAPPING_VERSION_VARIABLE = "fleet.version";
    private static final List<String> ALLOWED_PRODUCTS = List.of("kibana", "fleet");

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return List.of(
            fleetActionsSystemIndexDescriptor(),
            fleetAgentsSystemIndexDescriptor(),
            fleetEnrollmentApiKeysSystemIndexDescriptor(),
            fleetPoliciesSystemIndexDescriptor(),
            fleetPoliciesLeaderSystemIndexDescriptor(),
            fleetServersSystemIndexDescriptors(),
            fleetArtifactsSystemIndexDescriptors()
        );
    }

    @Override
    public String getFeatureName() {
        return "fleet";
    }

    @Override
    public String getFeatureDescription() {
        return "Manages configuration for Fleet";
    }

    private SystemIndexDescriptor fleetActionsSystemIndexDescriptor() {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        request.source(loadTemplateSource("/fleet-actions.json"), XContentType.JSON);

        return SystemIndexDescriptor.builder()
            .setType(Type.EXTERNAL_MANAGED)
            .setAllowedElasticProductOrigins(ALLOWED_PRODUCTS)
            .setOrigin(FLEET_ORIGIN)
            .setVersionMetaKey(VERSION_KEY)
            .setMappings(request.mappings())
            .setSettings(request.settings())
            .setPrimaryIndex(".fleet-actions-" + CURRENT_INDEX_VERSION)
            .setIndexPattern(".fleet-actions*")
            .setAliasName(".fleet-actions")
            .setDescription("Fleet agents")
            .build();
    }

    private SystemIndexDescriptor fleetAgentsSystemIndexDescriptor() {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        request.source(loadTemplateSource("/fleet-agents.json"), XContentType.JSON);

        return SystemIndexDescriptor.builder()
            .setType(Type.EXTERNAL_MANAGED)
            .setAllowedElasticProductOrigins(ALLOWED_PRODUCTS)
            .setOrigin(FLEET_ORIGIN)
            .setVersionMetaKey(VERSION_KEY)
            .setMappings(request.mappings())
            .setSettings(request.settings())
            .setPrimaryIndex(".fleet-agents-" + CURRENT_INDEX_VERSION)
            .setIndexPattern(".fleet-agents*")
            .setAliasName(".fleet-agents")
            .setDescription("Configuration of fleet servers")
            .build();
    }

    private SystemIndexDescriptor fleetEnrollmentApiKeysSystemIndexDescriptor() {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        request.source(loadTemplateSource("/fleet-enrollment-api-keys.json"), XContentType.JSON);

        return SystemIndexDescriptor.builder()
            .setType(Type.EXTERNAL_MANAGED)
            .setAllowedElasticProductOrigins(ALLOWED_PRODUCTS)
            .setOrigin(FLEET_ORIGIN)
            .setVersionMetaKey(VERSION_KEY)
            .setMappings(request.mappings())
            .setSettings(request.settings())
            .setPrimaryIndex(".fleet-enrollment-api-keys-" + CURRENT_INDEX_VERSION)
            .setIndexPattern(".fleet-enrollment-api-keys*")
            .setAliasName(".fleet-enrollment-api-keys")
            .setDescription("Fleet API Keys for enrollment")
            .build();
    }

    private SystemIndexDescriptor fleetPoliciesSystemIndexDescriptor() {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        request.source(loadTemplateSource("/fleet-policies.json"), XContentType.JSON);

        return SystemIndexDescriptor.builder()
            .setType(Type.EXTERNAL_MANAGED)
            .setAllowedElasticProductOrigins(ALLOWED_PRODUCTS)
            .setOrigin(FLEET_ORIGIN)
            .setVersionMetaKey(VERSION_KEY)
            .setMappings(request.mappings())
            .setSettings(request.settings())
            .setPrimaryIndex(".fleet-policies-" + CURRENT_INDEX_VERSION)
            .setIndexPattern(".fleet-policies-[0-9]+*")
            .setAliasName(".fleet-policies")
            .setDescription("Fleet Policies")
            .build();
    }

    private SystemIndexDescriptor fleetPoliciesLeaderSystemIndexDescriptor() {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        request.source(loadTemplateSource("/fleet-policies-leader.json"), XContentType.JSON);

        return SystemIndexDescriptor.builder()
            .setType(Type.EXTERNAL_MANAGED)
            .setAllowedElasticProductOrigins(ALLOWED_PRODUCTS)
            .setOrigin(FLEET_ORIGIN)
            .setVersionMetaKey(VERSION_KEY)
            .setMappings(request.mappings())
            .setSettings(request.settings())
            .setPrimaryIndex(".fleet-policies-leader-" + CURRENT_INDEX_VERSION)
            .setIndexPattern(".fleet-policies-leader*")
            .setAliasName(".fleet-policies-leader")
            .setDescription("Fleet Policies leader")
            .build();
    }

    private SystemIndexDescriptor fleetServersSystemIndexDescriptors() {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        request.source(loadTemplateSource("/fleet-servers.json"), XContentType.JSON);

        return SystemIndexDescriptor.builder()
            .setType(Type.EXTERNAL_MANAGED)
            .setAllowedElasticProductOrigins(ALLOWED_PRODUCTS)
            .setOrigin(FLEET_ORIGIN)
            .setVersionMetaKey(VERSION_KEY)
            .setMappings(request.mappings())
            .setSettings(request.settings())
            .setPrimaryIndex(".fleet-servers-" + CURRENT_INDEX_VERSION)
            .setIndexPattern(".fleet-servers*")
            .setAliasName(".fleet-servers")
            .setDescription("Fleet servers")
            .build();
    }

    private SystemIndexDescriptor fleetArtifactsSystemIndexDescriptors() {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        request.source(loadTemplateSource("/fleet-artifacts.json"), XContentType.JSON);

        return SystemIndexDescriptor.builder()
            .setType(Type.EXTERNAL_MANAGED)
            .setAllowedElasticProductOrigins(ALLOWED_PRODUCTS)
            .setOrigin(FLEET_ORIGIN)
            .setVersionMetaKey(VERSION_KEY)
            .setMappings(request.mappings())
            .setSettings(request.settings())
            .setPrimaryIndex(".fleet-artifacts-" + CURRENT_INDEX_VERSION)
            .setIndexPattern(".fleet-artifacts*")
            .setAliasName(".fleet-artifacts")
            .setDescription("Fleet artifacts")
            .build();
    }

    private String loadTemplateSource(String resource) {
        return TemplateUtils.loadTemplate(resource, Version.CURRENT.toString(), MAPPING_VERSION_VARIABLE);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
            new ActionHandler<>(GetGlobalCheckpointsAction.INSTANCE, GetGlobalCheckpointsAction.TransportAction.class),
            new ActionHandler<>(GetGlobalCheckpointsShardAction.INSTANCE, GetGlobalCheckpointsShardAction.TransportAction.class)
        );
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
        return Arrays.asList(new RestGetGlobalCheckpointsAction());
    }
}
