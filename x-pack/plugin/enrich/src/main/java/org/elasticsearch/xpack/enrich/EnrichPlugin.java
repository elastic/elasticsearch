/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
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
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.enrich.action.DeleteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.ListEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.action.TransportDeleteEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.action.TransportListEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.action.TransportPutEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.rest.RestDeleteEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.rest.RestListEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.rest.RestPutEnrichPolicyAction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.core.XPackSettings.ENRICH_ENABLED_SETTING;

public class EnrichPlugin extends Plugin implements ActionPlugin, IngestPlugin {

    static final Setting<Integer> ENRICH_FETCH_SIZE_SETTING =
        Setting.intSetting("index.xpack.enrich.fetch_size", 10000, 1, 1000000, Setting.Property.NodeScope);

    private final Settings settings;
    private final Boolean enabled;

    public EnrichPlugin(final Settings settings) {
        this.settings = settings;
        this.enabled = ENRICH_ENABLED_SETTING.get(settings);
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        final ClusterService clusterService = parameters.ingestService.getClusterService();
        // Pipelines are created from cluster state update thead and calling ClusterService#state() from that thead is illegal
        // (because the current cluster state update is in progress)
        // So with the below atomic reference we keep track of the latest updated cluster state:
        AtomicReference<ClusterState> reference = new AtomicReference<>();
        clusterService.addStateApplier(event -> reference.set(event.state()));

        return Collections.singletonMap(EnrichProcessorFactory.TYPE,
                new EnrichProcessorFactory(reference::get, parameters.localShardSearcher));
    }

    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (enabled == false) {
            return emptyList();
        }

        return Arrays.asList(
            new ActionHandler<>(DeleteEnrichPolicyAction.INSTANCE, TransportDeleteEnrichPolicyAction.class),
            new ActionHandler<>(ListEnrichPolicyAction.INSTANCE, TransportListEnrichPolicyAction.class),
            new ActionHandler<>(PutEnrichPolicyAction.INSTANCE, TransportPutEnrichPolicyAction.class)
        );
    }

    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        if (enabled == false) {
            return emptyList();
        }

        return Arrays.asList(
            new RestDeleteEnrichPolicyAction(settings, restController),
            new RestListEnrichPolicyAction(settings, restController),
            new RestPutEnrichPolicyAction(settings, restController)
        );
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
        EnrichPolicyExecutor enrichPolicyExecutor = new EnrichPolicyExecutor(settings, clusterService, client, threadPool,
            new IndexNameExpressionResolver(), System::currentTimeMillis);
        return Collections.singleton(enrichPolicyExecutor);
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Collections.singletonList(new NamedWriteableRegistry.Entry(MetaData.Custom.class, EnrichMetadata.TYPE,
            EnrichMetadata::new));
    }

    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return Collections.singletonList(new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField(EnrichMetadata.TYPE),
            EnrichMetadata::fromXContent));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Collections.singletonList(ENRICH_FETCH_SIZE_SETTING);
    }
}
