/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter.local;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.marvel.agent.exporter.ExportBulk;
import org.elasticsearch.marvel.agent.exporter.Exporter;
import org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils;
import org.elasticsearch.marvel.agent.renderer.RendererRegistry;
import org.elasticsearch.marvel.shield.SecuredClient;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;

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

    public LocalExporter(Exporter.Config config, Client client, ClusterService clusterService, RendererRegistry renderers) {
        super(TYPE, config);
        this.client = client;
        this.clusterService = clusterService;
        this.renderers = renderers;
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
            if (!installedTemplateVersionIsSufficient(Version.CURRENT, installedTemplateVersion)) {
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
            putTemplate(config.settings().getAsSettings("template.settings"));
            // we'll get that template on the next cluster state update
            return null;
        }
        Version installedTemplateVersion = MarvelTemplateUtils.templateVersion(installedTemplate);
        if (installedTemplateVersionMandatesAnUpdate(Version.CURRENT, installedTemplateVersion)) {
            logger.debug("local exporter [{}] - installing new marvel template [{}], replacing [{}]", name(), Version.CURRENT, installedTemplateVersion);
            putTemplate(config.settings().getAsSettings("template.settings"));
            // we'll get that template on the next cluster state update
            return null;
        } else if (!installedTemplateVersionIsSufficient(Version.CURRENT, installedTemplateVersion)) {
            logger.error("local exporter [{}] - marvel template version [{}] is below the minimum compatible version [{}]. "
                            + "please manually update the marvel template to a more recent version"
                            + "and delete the current active marvel index (don't forget to back up it first if needed)",
                    name(), installedTemplateVersion, MIN_SUPPORTED_TEMPLATE_VERSION);
            // we're not going to do anything with the template.. it's too old, and the schema might
            // be too different than what this version of marvel/es can work with. For this reason we're
            // not going to export any data, to avoid mapping conflicts.
            return null;
        }

        // ok.. we have a compatible template... we can start
        return currentBulk != null ? currentBulk : new LocalBulk(name(), logger, client, indexNameResolver, renderers);
    }

    boolean installedTemplateVersionIsSufficient(Version current, Version installed) {
        // null indicates couldn't parse the version from the installed template, this means it is probably too old or invalid...
        if (installed == null) {
            return false;
        }
        // ensure the template is not too old
        if (installed.before(MIN_SUPPORTED_TEMPLATE_VERSION)) {
            return false;
        }

        // We do not enforce that versions are equivalent to the current version as we may be in a rolling upgrade scenario
        // and until a master is elected with the new version, data nodes that have been upgraded will not be able to ship
        // data. This means that there is an implication that the new shippers will ship data correctly even with an old template.
        // There is also no upper bound and we rely on elasticsearch nodes not being able to connect to each other across major
        // versions
        return true;
    }

    boolean installedTemplateVersionMandatesAnUpdate(Version current, Version installed) {
        if (installed == null) {
            logger.debug("local exporter [{}] - currently installed marvel template is missing a version - installing a new one [{}]", name(), current);
            return true;
        }
        // Never update a very old template
        if (installed.before(MIN_SUPPORTED_TEMPLATE_VERSION)) {
            return false;
        }
        // Always update a template to the last up-to-date version
        if (current.after(installed)) {
            logger.debug("local exporter [{}] - currently installed marvel template version [{}] will be updated to a newer version [{}]", name(), installed, current);
            return true;
            // When the template is up-to-date, force an update for snapshot versions only
        } else if (current.equals(installed)) {
            logger.debug("local exporter [{}] - currently installed marvel template version [{}] is up-to-date", name(), installed);
            return installed.snapshot() && !current.snapshot();
            // Never update a template that is newer than the expected one
        } else {
            logger.debug("local exporter [{}] - currently installed marvel template version [{}] is newer than the one required [{}]... keeping it.", name(), installed, current);
            return false;
        }
    }

    void putTemplate(Settings customSettings) {
        try (InputStream is = getClass().getResourceAsStream("/marvel_index_template.json")) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Streams.copy(is, out);
            final byte[] template = out.toByteArray();
            PutIndexTemplateRequest request = new PutIndexTemplateRequest(MarvelTemplateUtils.INDEX_TEMPLATE_NAME).source(template);
            if (customSettings != null && customSettings.names().size() > 0) {
                Settings updatedSettings = Settings.builder()
                        .put(request.settings())
                        .put(customSettings)
                        // making sure we override any other template that may apply
                        .put("order", Integer.MAX_VALUE)
                        .build();
                request.settings(updatedSettings);
            }

            assert !Thread.currentThread().isInterrupted() : "current thread has been interrupted before putting index template!!!";

            // async call, so we won't block cluster event thread
            client.admin().indices().putTemplate(request, new ActionListener<PutIndexTemplateResponse>() {
                @Override
                public void onResponse(PutIndexTemplateResponse response) {
                    if (response.isAcknowledged()) {
                        logger.trace("local exporter [{}] - successfully installed marvel template", name());
                    } else {
                        logger.error("local exporter [{}] - failed to update marvel index template", name());
                    }
                }

                @Override
                public void onFailure(Throwable throwable) {
                    logger.error("local exporter [{}] - failed to update marvel index template", throwable, name());
                }
            });

        } catch (Exception e) {
            throw new IllegalStateException("failed to update marvel index template", e);
        }
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
