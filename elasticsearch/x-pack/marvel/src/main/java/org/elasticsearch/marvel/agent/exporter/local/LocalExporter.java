/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter.local;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.marvel.agent.exporter.ExportBulk;
import org.elasticsearch.marvel.agent.exporter.Exporter;
import org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils;
import org.elasticsearch.marvel.agent.renderer.RendererRegistry;
import org.elasticsearch.marvel.shield.SecuredClient;

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

    /** Version number of built-in templates **/
    private final Integer templateVersion;

    public LocalExporter(Exporter.Config config, Client client, ClusterService clusterService, RendererRegistry renderers) {
        super(TYPE, config);
        this.client = client;
        this.clusterService = clusterService;
        this.renderers = renderers;

        // Loads the current version number of built-in templates
        templateVersion = MarvelTemplateUtils.TEMPLATE_VERSION;
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

        String templateName = MarvelTemplateUtils.indexTemplateName(templateVersion);
        boolean templateInstalled = hasTemplate(templateName, clusterState);

        // if this is not the master, we'll just look to see if the marvel timestamped template is already
        // installed and if so, if it has a compatible version. If it is (installed and compatible)
        // we'll be able to start this exporter. Otherwise, we'll just wait for a new cluster state.
        if (!clusterService.localNode().masterNode()) {
            // We only need to check the index template for timestamped indices
            if (!templateInstalled) {
                // the template for timestamped indices is not yet installed in the given cluster state, we'll wait.
                logger.debug("local exporter [{}] - marvel index template does not exist, so service cannot start", name());
                return null;
            }

            // ok.. we have a compatible template... we can start
            logger.debug("local exporter [{}] - started!", name());
            return currentBulk != null ? currentBulk : new LocalBulk(name(), logger, client, indexNameResolver, renderers);
        }

        // we are on master
        //
        // Check that there is nothing that could block metadata updates
        if (clusterState.blocks().hasGlobalBlock(ClusterBlockLevel.METADATA_WRITE)) {
            logger.debug("local exporter [{}] - waiting until metadata writes are unblocked", name());
            return null;
        }

        // Install the index template for timestamped indices first, so that other nodes can ship data
        if (!templateInstalled) {
            logger.debug("local exporter [{}] - could not find existing marvel template for timestamped indices, installing a new one", name());
            putTemplate(templateName, MarvelTemplateUtils.loadTimestampedIndexTemplate());
            // we'll get that template on the next cluster state update
            return null;
        }

        // Install the index template for data index
        templateName = MarvelTemplateUtils.dataTemplateName(templateVersion);
        if (!hasTemplate(templateName, clusterState)) {
            logger.debug("local exporter [{}] - could not find existing marvel template for data index, installing a new one", name());
            putTemplate(templateName, MarvelTemplateUtils.loadDataIndexTemplate());
            // we'll get that template on the next cluster state update
            return null;
        }

        // ok.. we have a compatible templates... we can start
        return currentBulk != null ? currentBulk : new LocalBulk(name(), logger, client, indexNameResolver, renderers);
    }

    /**
     * List templates that exists in cluster state metadata and that match a given template name pattern.
     */
    private ImmutableOpenMap<String, Integer> findTemplates(String templatePattern, ClusterState state) {
        if (state == null || state.getMetaData() == null || state.getMetaData().getTemplates().isEmpty()) {
            return ImmutableOpenMap.of();
        }

        ImmutableOpenMap.Builder<String, Integer> templates = ImmutableOpenMap.builder();
        for (ObjectCursor<String> template : state.metaData().templates().keys()) {
            if (Regex.simpleMatch(templatePattern, template.value)) {
                try {
                    Integer version = Integer.parseInt(template.value.substring(templatePattern.length() - 1));
                    templates.put(template.value, version);
                    logger.debug("found index template [{}] in version [{}]", template, version);
                } catch (NumberFormatException e) {
                    logger.warn("cannot extract version number for template [{}]", template.value);
                }
            }
        }
        return templates.build();
    }

    private boolean hasTemplate(String templateName, ClusterState state) {
        ImmutableOpenMap<String, Integer> templates = findTemplates(templateName, state);
        return templates.size() > 0;
    }

    void putTemplate(String template, byte[] source) {
        logger.debug("local exporter [{}] - installing template [{}]", name(), template);

        PutIndexTemplateRequest request = new PutIndexTemplateRequest(template).source(source);
        assert !Thread.currentThread().isInterrupted() : "current thread has been interrupted before putting index template!!!";

        // async call, so we won't block cluster event thread
        client.admin().indices().putTemplate(request, new ActionListener<PutIndexTemplateResponse>() {
            @Override
            public void onResponse(PutIndexTemplateResponse response) {
                if (response.isAcknowledged()) {
                    logger.trace("local exporter [{}] - successfully installed marvel template [{}]", name(), template);
                } else {
                    logger.error("local exporter [{}] - failed to update marvel index template [{}]", name(), template);
                }
            }

            @Override
            public void onFailure(Throwable throwable) {
                logger.error("local exporter [{}] - failed to update marvel index template [{}]", throwable, name(), template);
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
