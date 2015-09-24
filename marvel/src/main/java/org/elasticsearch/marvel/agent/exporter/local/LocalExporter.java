/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter.local;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.marvel.agent.exporter.ExportBulk;
import org.elasticsearch.marvel.agent.exporter.Exporter;
import org.elasticsearch.marvel.agent.exporter.http.HttpExporterUtils;
import org.elasticsearch.marvel.agent.renderer.RendererRegistry;
import org.elasticsearch.marvel.shield.SecuredClient;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.marvel.agent.exporter.http.HttpExporter.MIN_SUPPORTED_CLUSTER_VERSION;
import static org.elasticsearch.marvel.agent.exporter.http.HttpExporter.MIN_SUPPORTED_TEMPLATE_VERSION;
import static org.elasticsearch.marvel.agent.exporter.http.HttpExporterUtils.MARVEL_VERSION_FIELD;

/**
 *
 */
public class LocalExporter extends Exporter {

    public static final String TYPE = "local";

    public static final String INDEX_TEMPLATE_NAME = "marvel";

    public static final String BULK_TIMEOUT_SETTING = "bulk.timeout";

    private final Client client;
    private final ClusterService clusterService;
    private final RendererRegistry renderers;

    private final LocalBulk bulk;

    final @Nullable TimeValue bulkTimeout;

    private final AtomicReference<State> state = new AtomicReference<>();

    /**
     * Version of the built-in template
     **/
    private final Version builtInTemplateVersion;

    public LocalExporter(Exporter.Config config, SecuredClient client, ClusterService clusterService, RendererRegistry renderers) {
        super(TYPE, config);
        this.client = client;
        this.clusterService = clusterService;
        this.renderers = renderers;

        // Checks that the built-in template is versioned
        builtInTemplateVersion = HttpExporterUtils.parseTemplateVersion(HttpExporterUtils.loadDefaultTemplate());
        if (builtInTemplateVersion == null) {
            throw new IllegalStateException("unable to find built-in template version");
        }

        bulkTimeout = config.settings().getAsTime(BULK_TIMEOUT_SETTING, null);

        state.set(State.STARTING);
        bulk = new LocalBulk(name(), logger, client, indexNameResolver, renderers);
    }

    @Override
    public ExportBulk openBulk() {
        if (!canExport()) {
            return null;
        }
        return bulk;
    }

    @Override
    public void close() {
        if (state.compareAndSet(State.STARTING, State.STOPPING) || state.compareAndSet(State.STARTED, State.STOPPING)) {
            try {
                bulk.terminate();
            } catch (Exception e) {
                logger.error("failed to cleanly close open bulk for [{}] exporter", e, name());
            }
            state.set(State.STOPPED);
        }
    }

    ClusterState clusterState() {
        return client.admin().cluster().prepareState().get().getState();
    }

    Version clusterVersion() {
        return Version.CURRENT;
    }

    Version templateVersion() {
        for (IndexTemplateMetaData template : client.admin().indices().prepareGetTemplates(INDEX_TEMPLATE_NAME).get().getIndexTemplates()) {
            if (template.getName().equals(INDEX_TEMPLATE_NAME)) {
                String version = template.settings().get("index." + MARVEL_VERSION_FIELD);
                if (Strings.hasLength(version)) {
                    return Version.fromString(version);
                }
            }
        }
        return null;
    }

    boolean shouldUpdateTemplate(Version current, Version expected) {
        // Always update a template even if its version is not found
        if (current == null) {
            return true;
        }
        // Never update a template in an unknown version
        if (expected == null) {
            return false;
        }
        // Never update a very old template
        if (current.before(MIN_SUPPORTED_TEMPLATE_VERSION)) {
            logger.error("marvel template version [{}] is below the minimum compatible version [{}]. "
                            + "please manually update the marvel template to a more recent version"
                            + "and delete the current active marvel index (don't forget to back up it first if needed)",
                    current, MIN_SUPPORTED_TEMPLATE_VERSION);
            return false;
        }
        // Always update a template to the last up-to-date version
        if (expected.after(current)) {
            logger.info("marvel template version will be updated to a newer version [current:{}, expected:{}]", current, expected);
            return true;
            // When the template is up-to-date, force an update for snapshot versions only
        } else if (expected.equals(current)) {
            logger.debug("marvel template version is up-to-date [current:{}, expected:{}]", current, expected);
            return expected.snapshot();
            // Never update a template that is newer than the expected one
        } else {
            logger.debug("marvel template version is newer than the one required by the marvel agent [current:{}, expected:{}]", current, expected);
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

            PutIndexTemplateResponse response = client.admin().indices().putTemplate(request).actionGet();
            if (!response.isAcknowledged()) {
                throw new IllegalStateException("failed to put marvel index template");
            }
        } catch (Exception e) {
            throw new IllegalStateException("failed to update marvel index template", e);
        }
    }

    boolean canExport() {
        if (state.get() == State.STARTED) {
            return true;
        }

        if (state.get() != State.STARTING) {
            return false;
        }

        ClusterState clusterState = clusterState();
        if (clusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we think may not have .marvel-es-
            // indices but they may not have been restored from the cluster state on disk
            logger.debug("exporter [{}] waiting until gateway has recovered from disk", name());
            return false;
        }

        Version clusterVersion = clusterVersion();
        if ((clusterVersion == null) || clusterVersion.before(MIN_SUPPORTED_CLUSTER_VERSION)) {
            logger.error("cluster version [" + clusterVersion + "] is not supported, please use a cluster with minimum version [" + MIN_SUPPORTED_CLUSTER_VERSION + "]");
            state.set(State.FAILED);
            return false;
        }

        Version templateVersion = templateVersion();
        if (!clusterService.state().nodes().localNodeMaster()) {
            if (templateVersion == null) {
                logger.debug("marvel index template [{}] does not exist, so service cannot start", INDEX_TEMPLATE_NAME);
                return false;
            }

            // TODO why do we need this check? the marvel indices are anyway auto-created
//            String indexName = indexNameResolver.resolve(System.currentTimeMillis());
//            if (!clusterState.routingTable().index(indexName).allPrimaryShardsActive()) {
//                logger.debug("marvel index [{}] has some primary shards not yet started, so service cannot start", indexName);
//                return false;
//            }
        }
        //TODO  this is erroneous
        //      the check may figure out that the existing version is too old and therefore
        //      it can't and won't update the template (prompting the user to delete the template).
        //      In this case, we shouldn't export data. But we do.. the "shouldUpdate" method
        //      needs to be "boolean ensureCompatibleTemplate". The boolean returned indicates whether
        //      the template is valid (either was valid or was updated to a valid one) or not. If
        //      not, the state of this exporter should not be set to STARTED.
        if (shouldUpdateTemplate(templateVersion, builtInTemplateVersion)) {
            putTemplate(config.settings().getAsSettings("template.settings"));
        }

        logger.debug("exporter [{}] can now export marvel data", name());
        state.set(State.STARTED);
        return true;
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
