/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateResponse.ResetFeatureStateStatus;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction.Request;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor.Type;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.template.TemplateUtils;
import org.elasticsearch.xpack.fleet.action.DeleteSecretAction;
import org.elasticsearch.xpack.fleet.action.GetGlobalCheckpointsAction;
import org.elasticsearch.xpack.fleet.action.GetGlobalCheckpointsShardAction;
import org.elasticsearch.xpack.fleet.action.GetSecretAction;
import org.elasticsearch.xpack.fleet.action.PostSecretAction;
import org.elasticsearch.xpack.fleet.action.TransportDeleteSecretAction;
import org.elasticsearch.xpack.fleet.action.TransportGetSecretAction;
import org.elasticsearch.xpack.fleet.action.TransportPostSecretAction;
import org.elasticsearch.xpack.fleet.rest.RestDeleteSecretsAction;
import org.elasticsearch.xpack.fleet.rest.RestFleetMultiSearchAction;
import org.elasticsearch.xpack.fleet.rest.RestFleetSearchAction;
import org.elasticsearch.xpack.fleet.rest.RestGetGlobalCheckpointsAction;
import org.elasticsearch.xpack.fleet.rest.RestGetSecretsAction;
import org.elasticsearch.xpack.fleet.rest.RestPostSecretsAction;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ClientHelper.FLEET_ORIGIN;

/**
 * A plugin to manage and provide access to the system indices used by Fleet.
 *
 * Currently only exposes general-purpose APIs on {@code _fleet}-prefixed routes, to be more specialized as Fleet's requirements stabilize.
 */
public class Fleet extends Plugin implements SystemIndexPlugin {

    public static final String FLEET_SECRETS_INDEX_NAME = ".fleet-secrets";

    private static final int CURRENT_INDEX_VERSION = 7;
    private static final String MAPPING_VERSION_VARIABLE = "fleet.version";
    private static final List<String> ALLOWED_PRODUCTS = List.of("kibana", "fleet");
    private static final int FLEET_ACTIONS_MAPPINGS_VERSION = 2;
    private static final int FLEET_AGENTS_MAPPINGS_VERSION = 2;
    private static final int FLEET_ENROLLMENT_API_KEYS_MAPPINGS_VERSION = 3;
    private static final int FLEET_SECRETS_MAPPINGS_VERSION = 1;
    private static final int FLEET_POLICIES_MAPPINGS_VERSION = 2;
    private static final int FLEET_POLICIES_LEADER_MAPPINGS_VERSION = 1;
    private static final int FLEET_SERVERS_MAPPINGS_VERSION = 1;
    private static final int FLEET_ARTIFACTS_MAPPINGS_VERSION = 1;
    private static final int FLEET_ACTIONS_RESULTS_MAPPINGS_VERSION = 1;

    @Override
    public Collection<?> createComponents(PluginServices services) {
        FleetTemplateRegistry registry = new FleetTemplateRegistry(
            services.environment().settings(),
            services.clusterService(),
            services.threadPool(),
            services.client(),
            services.xContentRegistry(),
            services.projectResolver()
        );
        registry.initialize();
        return List.of();
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return List.of(
            fleetActionsSystemIndexDescriptor(),
            fleetAgentsSystemIndexDescriptor(),
            fleetEnrollmentApiKeysSystemIndexDescriptor(),
            fleetSecretsSystemIndexDescriptor(),
            fleetPoliciesSystemIndexDescriptor(),
            fleetPoliciesLeaderSystemIndexDescriptor(),
            fleetServersSystemIndexDescriptors(),
            fleetArtifactsSystemIndexDescriptors()
        );
    }

    @Override
    public Collection<SystemDataStreamDescriptor> getSystemDataStreamDescriptors() {
        return List.of(fleetActionsResultsDescriptor());
    }

    @Override
    public String getFeatureName() {
        return "fleet";
    }

    @Override
    public String getFeatureDescription() {
        return "Manages configuration for Fleet";
    }

    private static SystemIndexDescriptor fleetActionsSystemIndexDescriptor() {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        request.source(loadTemplateSource("/fleet-actions.json", FLEET_ACTIONS_MAPPINGS_VERSION), XContentType.JSON);

        return SystemIndexDescriptor.builder()
            .setType(Type.EXTERNAL_MANAGED)
            .setAllowedElasticProductOrigins(ALLOWED_PRODUCTS)
            .setOrigin(FLEET_ORIGIN)
            .setMappings(request.mappings())
            .setSettings(request.settings())
            .setPrimaryIndex(".fleet-actions-" + CURRENT_INDEX_VERSION)
            .setIndexPattern(".fleet-actions~(-results*)")
            .setAliasName(".fleet-actions")
            .setDescription("Fleet agents")
            .build();
    }

    private static SystemIndexDescriptor fleetAgentsSystemIndexDescriptor() {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        request.source(loadTemplateSource("/fleet-agents.json", FLEET_AGENTS_MAPPINGS_VERSION), XContentType.JSON);

        return SystemIndexDescriptor.builder()
            .setType(Type.EXTERNAL_MANAGED)
            .setAllowedElasticProductOrigins(ALLOWED_PRODUCTS)
            .setOrigin(FLEET_ORIGIN)
            .setMappings(request.mappings())
            .setSettings(request.settings())
            .setPrimaryIndex(".fleet-agents-" + CURRENT_INDEX_VERSION)
            .setIndexPattern(".fleet-agents*")
            .setAliasName(".fleet-agents")
            .setDescription("Configuration of fleet servers")
            .build();
    }

    private static SystemIndexDescriptor fleetEnrollmentApiKeysSystemIndexDescriptor() {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        request.source(
            loadTemplateSource("/fleet-enrollment-api-keys.json", FLEET_ENROLLMENT_API_KEYS_MAPPINGS_VERSION),
            XContentType.JSON
        );

        return SystemIndexDescriptor.builder()
            .setType(Type.EXTERNAL_MANAGED)
            .setAllowedElasticProductOrigins(ALLOWED_PRODUCTS)
            .setOrigin(FLEET_ORIGIN)
            .setMappings(request.mappings())
            .setSettings(request.settings())
            .setPrimaryIndex(".fleet-enrollment-api-keys-" + CURRENT_INDEX_VERSION)
            .setIndexPattern(".fleet-enrollment-api-keys*")
            .setAliasName(".fleet-enrollment-api-keys")
            .setDescription("Fleet API Keys for enrollment")
            .build();
    }

    private static SystemIndexDescriptor fleetSecretsSystemIndexDescriptor() {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        request.source(loadTemplateSource("/fleet-secrets.json", FLEET_SECRETS_MAPPINGS_VERSION), XContentType.JSON);
        return SystemIndexDescriptor.builder()
            .setType(Type.INTERNAL_MANAGED)
            .setOrigin(FLEET_ORIGIN)
            .setMappings(request.mappings())
            .setSettings(request.settings())
            .setPrimaryIndex(FLEET_SECRETS_INDEX_NAME + "-" + CURRENT_INDEX_VERSION)
            .setIndexPattern(FLEET_SECRETS_INDEX_NAME + "*")
            .setAliasName(FLEET_SECRETS_INDEX_NAME)
            .setDescription("Secret values managed by Fleet")
            .build();
    }

    private static SystemIndexDescriptor fleetPoliciesSystemIndexDescriptor() {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        request.source(loadTemplateSource("/fleet-policies.json", FLEET_POLICIES_MAPPINGS_VERSION), XContentType.JSON);

        return SystemIndexDescriptor.builder()
            .setType(Type.EXTERNAL_MANAGED)
            .setAllowedElasticProductOrigins(ALLOWED_PRODUCTS)
            .setOrigin(FLEET_ORIGIN)
            .setMappings(request.mappings())
            .setSettings(request.settings())
            .setPrimaryIndex(".fleet-policies-" + CURRENT_INDEX_VERSION)
            .setIndexPattern(".fleet-policies-[0-9]+*")
            .setAliasName(".fleet-policies")
            .setDescription("Fleet Policies")
            .build();
    }

    private static SystemIndexDescriptor fleetPoliciesLeaderSystemIndexDescriptor() {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        request.source(loadTemplateSource("/fleet-policies-leader.json", FLEET_POLICIES_LEADER_MAPPINGS_VERSION), XContentType.JSON);

        return SystemIndexDescriptor.builder()
            .setType(Type.EXTERNAL_MANAGED)
            .setAllowedElasticProductOrigins(ALLOWED_PRODUCTS)
            .setOrigin(FLEET_ORIGIN)
            .setMappings(request.mappings())
            .setSettings(request.settings())
            .setPrimaryIndex(".fleet-policies-leader-" + CURRENT_INDEX_VERSION)
            .setIndexPattern(".fleet-policies-leader*")
            .setAliasName(".fleet-policies-leader")
            .setDescription("Fleet Policies leader")
            .build();
    }

    private static SystemIndexDescriptor fleetServersSystemIndexDescriptors() {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        request.source(loadTemplateSource("/fleet-servers.json", FLEET_SERVERS_MAPPINGS_VERSION), XContentType.JSON);

        return SystemIndexDescriptor.builder()
            .setType(Type.EXTERNAL_MANAGED)
            .setAllowedElasticProductOrigins(ALLOWED_PRODUCTS)
            .setOrigin(FLEET_ORIGIN)
            .setMappings(request.mappings())
            .setSettings(request.settings())
            .setPrimaryIndex(".fleet-servers-" + CURRENT_INDEX_VERSION)
            .setIndexPattern(".fleet-servers*")
            .setAliasName(".fleet-servers")
            .setDescription("Fleet servers")
            .build();
    }

    private static SystemIndexDescriptor fleetArtifactsSystemIndexDescriptors() {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        request.source(loadTemplateSource("/fleet-artifacts.json", FLEET_ARTIFACTS_MAPPINGS_VERSION), XContentType.JSON);

        return SystemIndexDescriptor.builder()
            .setType(Type.EXTERNAL_MANAGED)
            .setAllowedElasticProductOrigins(ALLOWED_PRODUCTS)
            .setOrigin(FLEET_ORIGIN)
            .setMappings(request.mappings())
            .setSettings(request.settings())
            .setPrimaryIndex(".fleet-artifacts-" + CURRENT_INDEX_VERSION)
            .setIndexPattern(".fleet-artifacts*")
            .setAliasName(".fleet-artifacts")
            .setDescription("Fleet artifacts")
            .build();
    }

    private static SystemDataStreamDescriptor fleetActionsResultsDescriptor() {
        final String source = loadTemplateSource("/fleet-actions-results.json", FLEET_ACTIONS_RESULTS_MAPPINGS_VERSION);
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, source)) {
            ComposableIndexTemplate composableIndexTemplate = ComposableIndexTemplate.parse(parser);
            return new SystemDataStreamDescriptor(
                ".fleet-actions-results",
                "Result history of fleet actions",
                SystemDataStreamDescriptor.Type.EXTERNAL,
                composableIndexTemplate,
                Map.of(),
                ALLOWED_PRODUCTS,
                FLEET_ORIGIN,
                ExecutorNames.DEFAULT_SYSTEM_DATA_STREAM_THREAD_POOLS
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void cleanUpFeature(
        ClusterService clusterService,
        ProjectResolver projectResolver,
        Client client,
        ActionListener<ResetFeatureStateStatus> listener
    ) {
        Collection<SystemDataStreamDescriptor> dataStreamDescriptors = getSystemDataStreamDescriptors();
        if (dataStreamDescriptors.isEmpty() == false) {
            try {
                Request request = new Request(
                    TimeValue.THIRTY_SECONDS /* TODO should we wait longer? */,
                    dataStreamDescriptors.stream().map(SystemDataStreamDescriptor::getDataStreamName).toArray(String[]::new)
                );
                request.indicesOptions(
                    IndicesOptions.builder(request.indicesOptions())
                        .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
                        .build()
                );

                client.execute(
                    DeleteDataStreamAction.INSTANCE,
                    request,
                    ActionListener.wrap(
                        response -> SystemIndexPlugin.super.cleanUpFeature(clusterService, projectResolver, client, listener),
                        e -> {
                            Throwable unwrapped = ExceptionsHelper.unwrapCause(e);
                            if (unwrapped instanceof ResourceNotFoundException) {
                                SystemIndexPlugin.super.cleanUpFeature(clusterService, projectResolver, client, listener);
                            } else {
                                listener.onFailure(e);
                            }
                        }
                    )
                );
            } catch (Exception e) {
                Throwable unwrapped = ExceptionsHelper.unwrapCause(e);
                if (unwrapped instanceof ResourceNotFoundException) {
                    SystemIndexPlugin.super.cleanUpFeature(clusterService, projectResolver, client, listener);
                } else {
                    listener.onFailure(e);
                }
            }
        } else {
            SystemIndexPlugin.super.cleanUpFeature(clusterService, projectResolver, client, listener);
        }
    }

    private static String loadTemplateSource(String resource, int mappingsVersion) {
        return TemplateUtils.loadTemplate(
            resource,
            Version.CURRENT.toString(),
            MAPPING_VERSION_VARIABLE,
            Map.of("fleet.managed.index.version", Integer.toString(mappingsVersion))
        );
    }

    @Override
    public List<ActionHandler> getActions() {
        return List.of(
            new ActionHandler(GetGlobalCheckpointsAction.INSTANCE, GetGlobalCheckpointsAction.LocalAction.class),
            new ActionHandler(GetGlobalCheckpointsShardAction.INSTANCE, GetGlobalCheckpointsShardAction.TransportAction.class),
            new ActionHandler(GetSecretAction.INSTANCE, TransportGetSecretAction.class),
            new ActionHandler(PostSecretAction.INSTANCE, TransportPostSecretAction.class),
            new ActionHandler(DeleteSecretAction.INSTANCE, TransportDeleteSecretAction.class)
        );
    }

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
        return List.of(
            new RestGetGlobalCheckpointsAction(),
            new RestFleetSearchAction(restController.getSearchUsageHolder(), clusterSupportsFeature),
            new RestFleetMultiSearchAction(settings, restController.getSearchUsageHolder(), clusterSupportsFeature),
            new RestGetSecretsAction(),
            new RestPostSecretsAction(),
            new RestDeleteSecretsAction()
        );
    }
}
