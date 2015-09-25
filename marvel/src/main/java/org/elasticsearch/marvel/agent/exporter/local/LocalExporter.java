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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.marvel.agent.exporter.ExportBulk;
import org.elasticsearch.marvel.agent.exporter.Exporter;
import org.elasticsearch.marvel.agent.renderer.RendererRegistry;
import org.elasticsearch.marvel.shield.SecuredClient;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;

import static org.elasticsearch.marvel.agent.exporter.http.HttpExporter.MIN_SUPPORTED_TEMPLATE_VERSION;
import static org.elasticsearch.marvel.agent.exporter.http.HttpExporterUtils.MARVEL_VERSION_FIELD;

/**
 *
 */
public class LocalExporter extends Exporter {

    public static final String TYPE = "local";

    private final Client client;
    private final ClusterService clusterService;
    private final RendererRegistry renderers;

    private volatile LocalBulk bulk;

    public LocalExporter(Exporter.Config config, SecuredClient client, ClusterService clusterService, RendererRegistry renderers) {
        super(TYPE, config);
        this.client = client;
        this.clusterService = clusterService;
        this.renderers = renderers;

        clusterService.add(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                bulk = start(event.state());
            }
        });
    }

    @Override
    public ExportBulk openBulk() {
        return bulk;
    }

    @Override
    public void close() {
        if (bulk != null) {
            try {
                bulk.terminate();
            } catch (Exception e) {
                logger.error("failed to cleanly close open bulk for [{}] exporter", e, name());
            }
        }
    }

    LocalBulk start(ClusterState clusterState) {
        if (bulk != null) {
            return bulk;
        }

        if (clusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we think may not have .marvel-es-
            // indices but they may not have been restored from the cluster state on disk
            logger.debug("exporter [{}] waiting until gateway has recovered from disk", name());
            return null;
        }

        IndexTemplateMetaData installedTemplate = clusterState.getMetaData().getTemplates().get(INDEX_TEMPLATE_NAME);

        // if this is not the master, we'll just look to see if the marvel template is already
        // installed and if so, if it has a compatible version. If it is (installed and compatible)
        // we'll be able to start this exporter. Otherwise, we'll just wait for a new cluster state.
        if (!clusterService.localNode().masterNode()) {
            if (installedTemplate == null) {
                // the marvel template is not yet installed in the given cluster state, we'll wait.
                logger.debug("marvel index template [{}] does not exist, so service cannot start", INDEX_TEMPLATE_NAME);
                return null;
            }
            Version installedTemplateVersion = templateVersion(installedTemplate);
            if (!installedTemplateVersionIsSufficient(Version.CURRENT, installedTemplateVersion)) {
                logger.debug("exporter cannot start. the currently installed marvel template (version [{}]) is incompatible with the " +
                        "current elasticsearch version [{}]. waiting until the template is updated", installedTemplateVersion, Version.CURRENT);
            }

            // ok.. we have a compatible template... we can start
            logger.debug("marvel [{}] exporter started!", name());
            return new LocalBulk(name(), logger, client, indexNameResolver, renderers);
        }

        // we are on master
        //
        // if we cannot find a template or a compatible template, we'll install one in / update it.
        if (installedTemplate == null) {
            putTemplate(config.settings().getAsSettings("template.settings"));
            // we'll get that template on the next cluster state update
            return null;
        }
        Version installedTemplateVersion = templateVersion(installedTemplate);
        if (installedTemplateVersionMandatesAnUpdate(Version.CURRENT, installedTemplateVersion)) {
            logger.debug("installing new marvel template [{}], replacing [{}]", Version.CURRENT, installedTemplateVersion);
            putTemplate(config.settings().getAsSettings("template.settings"));
            // we'll get that template on the next cluster state update
            return null;
        }

        // ok.. we have a compatible template... we can start
        logger.debug("marvel [{}] exporter started!", name());
        return new LocalBulk(name(), logger, client, indexNameResolver, renderers);
    }

    static Version templateVersion(IndexTemplateMetaData templateMetaData) {
        String version = templateMetaData.settings().get("index." + MARVEL_VERSION_FIELD);
        if (Strings.hasLength(version)) {
            return Version.fromString(version);
        }
        return null;
    }

    boolean installedTemplateVersionIsSufficient(Version current, Version installed) {
        if (installed == null) {
            return false;
        }
        if (installed.before(MIN_SUPPORTED_TEMPLATE_VERSION)) {
            return false;
        }
        if (current.after(installed)) {
            return true;
        }
        if (current.equals(installed)) {
            return current.snapshot();
        }
        return false;
    }

    boolean installedTemplateVersionMandatesAnUpdate(Version current, Version installed) {
        if (installed == null) {
            return true;
        }
        // Never update a very old template
        if (installed.before(MIN_SUPPORTED_TEMPLATE_VERSION)) {
            logger.error("marvel template version [{}] is below the minimum compatible version [{}]. "
                            + "please manually update the marvel template to a more recent version"
                            + "and delete the current active marvel index (don't forget to back up it first if needed)",
                    installed, MIN_SUPPORTED_TEMPLATE_VERSION);
            return false;
        }
        // Always update a template to the last up-to-date version
        if (current.after(installed)) {
            logger.debug("the installed marvel template version [{}] will be updated to a newer version [{}]", installed, current);
            return true;
            // When the template is up-to-date, force an update for snapshot versions only
        } else if (current.equals(installed)) {
            logger.debug("the installed marvel template version [{}] is up-to-date", installed);
            return installed.snapshot() && !current.snapshot();
            // Never update a template that is newer than the expected one
        } else {
            logger.debug("the installed marvel template version [{}] is newer than the one required [{}]... keeping it.", installed, current);
            return false;
        }
    }

    void putTemplate(Settings customSettings) {
        try (InputStream is = getClass().getResourceAsStream("/marvel_index_template.json")) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Streams.copy(is, out);
            final byte[] template = out.toByteArray();
            PutIndexTemplateRequest request = new PutIndexTemplateRequest(INDEX_TEMPLATE_NAME).source(template);
            if (customSettings != null && customSettings.names().size() > 0) {
                Settings updatedSettings = Settings.builder()
                        .put(request.settings())
                        .put(customSettings)
                        .build();
                request.settings(updatedSettings);
            }

            assert !Thread.currentThread().isInterrupted() : "current thread has been interrupted before putting index template!!!";

            // async call, so we won't block cluster event thread
            client.admin().indices().putTemplate(request, new ActionListener<PutIndexTemplateResponse>() {
                @Override
                public void onResponse(PutIndexTemplateResponse response) {
                    if (!response.isAcknowledged()) {
                        logger.error("failed to update marvel index template");
                    }
                }

                @Override
                public void onFailure(Throwable throwable) {
                    logger.error("failed to update marvel index template", throwable);
                }
            });

        } catch (Exception e) {
            throw new IllegalStateException("failed to update marvel index template", e);
        }
    }

    public enum State {
        STARTING,
        STARTED,
        STOPPING,
        STOPPED,
        FAILED
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
