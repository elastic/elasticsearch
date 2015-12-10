/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter.local;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.marvel.agent.exporter.ExportBulk;
import org.elasticsearch.marvel.agent.exporter.Exporter;
import org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils;
import org.elasticsearch.marvel.agent.renderer.RendererRegistry;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.shield.SecuredClient;

import static org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils.installedTemplateVersionIsSufficient;
import static org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils.installedTemplateVersionMandatesAnUpdate;

/**
 *
 */
public class LocalExporter extends Exporter implements ClusterStateListener {

    public static final String TYPE = "local";

    private final Client client;
    private final ClusterService clusterService;
    private final RendererRegistry renderers;

    private volatile LocalBulk bulk;
    private volatile boolean active = true;

    /** Version of the built-in template **/
    private final Version templateVersion;

    public LocalExporter(Exporter.Config config, Client client, ClusterService clusterService, RendererRegistry renderers) {
        super(TYPE, config);
        this.client = client;
        this.clusterService = clusterService;
        this.renderers = renderers;

        // Checks that the built-in template is versioned
        templateVersion = MarvelTemplateUtils.loadDefaultTemplateVersion();
        if (templateVersion == null) {
            throw new IllegalStateException("unable to find built-in template version");
        }

        bulk = resolveBulk(clusterService.state(), bulk);
        clusterService.add(this);
    }

    LocalBulk getBulk() {
        return bulk;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        LocalBulk currentBulk = bulk;
        LocalBulk newBulk = resolveBulk(event.state(), currentBulk);

        // yes, this method will always be called by the cluster event loop thread
        // but we need to sync with the {@code #close()} mechanism
        synchronized (this) {
            if (active) {
                bulk = newBulk;
            } else if (newBulk != null) {
                newBulk.terminate();
            }
            if (currentBulk == null && bulk != null) {
                logger.debug("local exporter [{}] - started!", name());
            }
            if (bulk != currentBulk && currentBulk != null) {
                logger.debug("local exporter [{}] - stopped!", name());
                currentBulk.terminate();
            }
        }
    }

    @Override
    public ExportBulk openBulk() {
        return bulk;
    }

    // requires synchronization due to cluster state update events (see above)
    @Override
    public synchronized void close() {
        active = false;
        clusterService.remove(this);
        if (bulk != null) {
            try {
                bulk.terminate();
                bulk = null;
            } catch (Exception e) {
                logger.error("local exporter [{}] - failed to cleanly close bulk", e, name());
            }
        }
    }

    LocalBulk resolveBulk(ClusterState clusterState, LocalBulk currentBulk) {
        if (clusterService.localNode() == null || clusterState == null) {
            return currentBulk;
        }

        if (clusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we think may not have .marvel-es-
            // indices but they may not have been restored from the cluster state on disk
            logger.debug("local exporter [{}] - waiting until gateway has recovered from disk", name());
            return null;
        }

        IndexTemplateMetaData installedTemplate = MarvelTemplateUtils.findMarvelTemplate(clusterState);

        // if this is not the master, we'll just look to see if the marvel template is already
        // installed and if so, if it has a compatible version. If it is (installed and compatible)
        // we'll be able to start this exporter. Otherwise, we'll just wait for a new cluster state.
        if (!clusterService.localNode().masterNode()) {
            if (installedTemplate == null) {
                // the marvel template is not yet installed in the given cluster state, we'll wait.
                logger.debug("local exporter [{}] - marvel index template [{}] does not exist, so service cannot start", name(), MarvelTemplateUtils.INDEX_TEMPLATE_NAME);
                return null;
            }
            Version installedTemplateVersion = MarvelTemplateUtils.templateVersion(installedTemplate);
            if (!installedTemplateVersionIsSufficient(installedTemplateVersion)) {
                logger.debug("local exporter [{}] - cannot start. the currently installed marvel template (version [{}]) is incompatible with the " +
                        "current elasticsearch version [{}]. waiting until the template is updated", name(), installedTemplateVersion, Version.CURRENT);
                return null;
            }

            // ok.. we have a compatible template... we can start
            logger.debug("local exporter [{}] - started!", name());
            return currentBulk != null ? currentBulk : new LocalBulk(name(), logger, client, indexNameResolver, renderers);
        }

        // we are on master
        //
        // if we cannot find a template or a compatible template, we'll install one in / update it.
        if (installedTemplate == null) {
            logger.debug("local exporter [{}] - could not find existing marvel template, installing a new one", name());
            putTemplate();
            // we'll get that template on the next cluster state update
            return null;
        }
        Version installedTemplateVersion = MarvelTemplateUtils.templateVersion(installedTemplate);
        if (installedTemplateVersionMandatesAnUpdate(templateVersion, installedTemplateVersion, logger, name())) {
            logger.debug("local exporter [{}] - installing new marvel template [{}], replacing [{}]", name(), templateVersion, installedTemplateVersion);
            putTemplate();
            // we'll get that template on the next cluster state update
            return null;
        } else if (!installedTemplateVersionIsSufficient(installedTemplateVersion)) {
            logger.error("local exporter [{}] - marvel template version [{}] is below the minimum compatible version [{}]. "
                            + "please manually update the marvel template to a more recent version"
                            + "and delete the current active marvel index (don't forget to back up it first if needed)",
                    name(), installedTemplateVersion, MarvelTemplateUtils.MIN_SUPPORTED_TEMPLATE_VERSION);
            // we're not going to do anything with the template.. it's too old, and the schema might
            // be too different than what this version of marvel/es can work with. For this reason we're
            // not going to export any data, to avoid mapping conflicts.
            return null;
        }

        // ok.. we have a compatible template... we can start
        return currentBulk != null ? currentBulk : new LocalBulk(name(), logger, client, indexNameResolver, renderers);
    }

    void putTemplate() {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest(MarvelTemplateUtils.INDEX_TEMPLATE_NAME).source(MarvelTemplateUtils.loadDefaultTemplate());
        assert !Thread.currentThread().isInterrupted() : "current thread has been interrupted before putting index template!!!";

        // async call, so we won't block cluster event thread
        client.admin().indices().putTemplate(request, new ActionListener<PutIndexTemplateResponse>() {
            @Override
            public void onResponse(PutIndexTemplateResponse response) {
                if (response.isAcknowledged()) {
                    logger.trace("local exporter [{}] - successfully installed marvel template", name());

                    if (config.settings().getAsBoolean("update_mappings", true)) {
                        updateMappings(MarvelSettings.MARVEL_DATA_INDEX_NAME);
                        updateMappings(indexNameResolver().resolve(System.currentTimeMillis()));
                    }
                } else {
                    logger.error("local exporter [{}] - failed to update marvel index template", name());
                }
            }

            @Override
            public void onFailure(Throwable throwable) {
                logger.error("local exporter [{}] - failed to update marvel index template", throwable, name());
            }
        });
    }

    // TODO: Remove this method once marvel indices are versioned (v 2.2.0)
    void updateMappings(String index) {
        logger.trace("local exporter [{}] - updating mappings for index [{}]", name(), index);

        // Parse the default template to get its mappings
        PutIndexTemplateRequest template = new PutIndexTemplateRequest().source(MarvelTemplateUtils.loadDefaultTemplate());
        if ((template == null) || (template.mappings() == null) || (template.mappings().isEmpty())) {
            return;
        }

        // async call, so we won't block cluster event thread
        client.admin().indices().getMappings(new GetMappingsRequest().indices(index), new ActionListener<GetMappingsResponse>() {
            @Override
            public void onResponse(GetMappingsResponse response) {
                ImmutableOpenMap<String, MappingMetaData> indexMappings = response.getMappings().get(index);
                if (indexMappings != null) {

                    // Iterates over document types defined in the default template
                    for (String type : template.mappings().keySet()) {
                        if (indexMappings.get(type) != null) {
                            logger.trace("local exporter [{}] - type [{} already exists in mapping of index [{}]", name(), type, index);
                            continue;
                        }

                        logger.trace("local exporter [{}] - adding type [{}] to index [{}] mappings", name(), type, index);
                        updateMappingForType(index, type, template.mappings().get(type));
                    }
                }
            }

            @Override
            public void onFailure(Throwable e) {
                if (e instanceof IndexNotFoundException) {
                    logger.trace("local exporter [{}] - index [{}] not found, unable to update mappings", name(), index);
                } else {
                    logger.error("local exporter [{}] - failed to get mappings for index [{}]", name(), index);
                }
            }
        });
    }

    void updateMappingForType(String index, String type, String mappingSource) {
        logger.trace("local exporter [{}] - updating index [{}] mappings for type [{}]", name(), index, type);
        client.admin().indices().putMapping(new PutMappingRequest(index).type(type).source(mappingSource), new ActionListener<PutMappingResponse>() {
            @Override
            public void onResponse(PutMappingResponse response) {
                if (response.isAcknowledged()) {
                    logger.trace("local exporter [{}] - mapping of index [{}] updated for type [{}]", name(), index, type);
                } else {
                    logger.trace("local exporter [{}] - mapping of index [{}] failed to be updated for type [{}]", name(), index, type);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                logger.error("local exporter [{}] - failed to update mapping of index [{}] for type [{}]", name(), index, type);
            }
        });
    }

    public static class Factory extends Exporter.Factory<LocalExporter> {

        private final SecuredClient client;
        private final RendererRegistry registry;
        private final ClusterService clusterService;

        @Inject
        public Factory(SecuredClient client, ClusterService clusterService, RendererRegistry registry) {
            super(TYPE, true);
            this.client = client;
            this.clusterService = clusterService;
            this.registry = registry;
        }

        @Override
        public LocalExporter create(Config config) {
            return new LocalExporter(config, client, clusterService, registry);
        }
    }
}
