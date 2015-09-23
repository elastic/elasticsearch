/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter.local;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.marvel.agent.exporter.Exporter;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.agent.exporter.http.HttpExporterUtils;
import org.elasticsearch.marvel.agent.renderer.Renderer;
import org.elasticsearch.marvel.agent.renderer.RendererRegistry;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.shield.SecuredClient;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
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

    public static final String QUEUE_SIZE_SETTING = "queue_max_size";
    public static final String BULK_SIZE_SETTING = "bulk_size";
    public static final String BULK_FLUSH_INTERVAL_SETTING = "bulk_flush_interval";
    public static final String INDEX_NAME_TIME_FORMAT_SETTING = "index.name.time_format";

    public static final int DEFAULT_MAX_QUEUE_SIZE = 1000;
    public static final int DEFAULT_BULK_SIZE = 1000;
    public static final int MAX_BULK_SIZE = 10000;
    public static final TimeValue DEFAULT_BULK_FLUSH_INTERVAL = TimeValue.timeValueSeconds(1);
    public static final String DEFAULT_INDEX_NAME_TIME_FORMAT = "YYYY.MM.dd";

    private final Client client;
    private final ClusterService clusterService;
    private final RendererRegistry registry;
    private final QueueConsumer queueConsumer;
    private final DateTimeFormatter indexTimeFormatter;

    private final AtomicReference<State> state = new AtomicReference<>(State.INITIALIZED);
    private final LinkedBlockingQueue<IndexRequest> queue;

    /**
     * Version of the built-in template
     **/
    private final Version builtInTemplateVersion;

    public LocalExporter(Exporter.Config config, SecuredClient client, ClusterService clusterService, RendererRegistry registry) {
        super(TYPE, config);
        this.client = client;
        this.clusterService = clusterService;
        this.registry = registry;
        this.queueConsumer = new QueueConsumer(EsExecutors.threadName(config.settings(), "marvel-queue-consumer-" + config.name()));

        int maxQueueSize = config.settings().getAsInt(QUEUE_SIZE_SETTING, DEFAULT_MAX_QUEUE_SIZE);
        if (maxQueueSize <= 0) {
            logger.warn("invalid value [{}] for setting [{}]. using default value [{}]", maxQueueSize, QUEUE_SIZE_SETTING, DEFAULT_MAX_QUEUE_SIZE);
            maxQueueSize = DEFAULT_MAX_QUEUE_SIZE;
        }
        this.queue = new LinkedBlockingQueue<>(maxQueueSize);

        String indexTimeFormat = config.settings().get(INDEX_NAME_TIME_FORMAT_SETTING, DEFAULT_INDEX_NAME_TIME_FORMAT);
        try {
            indexTimeFormatter = DateTimeFormat.forPattern(indexTimeFormat).withZoneUTC();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid marvel index name time format [" + indexTimeFormat + "] set for [" + settingFQN(INDEX_NAME_TIME_FORMAT_SETTING) + "]", e);
        }

        // Checks that the built-in template is versioned
        builtInTemplateVersion = HttpExporterUtils.parseTemplateVersion(HttpExporterUtils.loadDefaultTemplate());
        if (builtInTemplateVersion == null) {
            throw new IllegalStateException("unable to find built-in template version");
        }
        state.set(State.STARTING);
    }

    public void stop() {
        if (state.compareAndSet(State.STARTED, State.STOPPING) || state.compareAndSet(State.STARTING, State.STOPPING)) {
            try {
                queueConsumer.interrupt();
            } finally {
                state.set(State.STOPPED);
            }
        }
    }

    @Override
    public void close() {
        if (state.get() != State.STOPPED) {
            stop();
        }
    }

    private boolean canExport() {
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
        if (clusterService.state().nodes().localNodeMaster() == false) {
            if (templateVersion == null) {
                logger.debug("marvel index template [{}] does not exist, so service cannot start", INDEX_TEMPLATE_NAME);
                return false;
            }
            if (clusterState.routingTable().index(indexName()).allPrimaryShardsActive() == false) {
                logger.debug("marvel index [{}] has some primary shards not yet started, so service cannot start", indexName());
                return false;
            }
        } else if (shouldUpdateTemplate(templateVersion, builtInTemplateVersion)) {
            putTemplate(config.settings().getAsSettings("template.settings"));
        }

        logger.debug("exporter [{}] can now export marvel data", name());
        queueConsumer.start();
        state.set(State.STARTED);
        return true;
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
            logger.error("marvel template version [{}] is below the minimum compatible version [{}]: "
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

    @Override
    public void export(Collection<MarvelDoc> marvelDocs) {
        if (marvelDocs == null) {
            logger.debug("no marvel documents to export");
            return;
        }

        if (canExport() == false) {
            logger.debug("exporter [{}] can not export data", name());
            return;
        }

        BytesStreamOutput buffer = null;
        for (MarvelDoc marvelDoc : marvelDocs) {
            try {
                IndexRequestBuilder request = client.prepareIndex();
                if (marvelDoc.index() != null) {
                    request.setIndex(marvelDoc.index());
                } else {
                    request.setIndex(indexName());
                }
                if (marvelDoc.type() != null) {
                    request.setType(marvelDoc.type());
                }
                if (marvelDoc.id() != null) {
                    request.setId(marvelDoc.id());
                }

                // Get the appropriate renderer in order to render the MarvelDoc
                Renderer renderer = registry.renderer(marvelDoc.type());
                if (renderer == null) {
                    logger.warn("unable to render marvel document of type [{}]. no renderer found in registry", marvelDoc.type());
                    return;
                }

                if (buffer == null) {
                    buffer = new BytesStreamOutput();
                }

                renderer.render(marvelDoc, XContentType.SMILE, buffer);
                request.setSource(buffer.bytes().toBytes());

                queue.add(request.request());
            } catch (IOException e) {
                logger.error("failed to export marvel data", e);
            } finally {
                if (buffer != null) {
                    buffer.reset();
                }
            }
        }
    }

    String indexName() {
        return MarvelSettings.MARVEL_INDICES_PREFIX + indexTimeFormatter.print(System.currentTimeMillis());
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

    class QueueConsumer extends Thread {

        private volatile boolean running = true;

        QueueConsumer(String name) {
            super(name);
            setDaemon(true);
        }

        @Override
        public void run() {
            try (BulkProcessor bulkProcessor = createBulkProcessor(config)) {
                while (running) {
                    try {
                        IndexRequest request = queue.take();
                        if (request != null) {
                            bulkProcessor.add(request);
                        }
                    } catch (InterruptedException e) {
                        logger.debug("marvel queue consumer interrupted, flushing bulk processor", e);
                        bulkProcessor.flush();
                        running = false;
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        // log the exception and keep going
                        logger.warn("failed to index marvel documents from queue", e);
                    }
                }
            }
        }

        private BulkProcessor createBulkProcessor(Config config) {
            int bulkSize = Math.min(config.settings().getAsInt(BULK_SIZE_SETTING, DEFAULT_BULK_SIZE), MAX_BULK_SIZE);
            bulkSize = (bulkSize < 1) ? DEFAULT_BULK_SIZE : bulkSize;

            TimeValue interval = config.settings().getAsTime(BULK_FLUSH_INTERVAL_SETTING, DEFAULT_BULK_FLUSH_INTERVAL);
            interval = (interval.millis() < 1) ? DEFAULT_BULK_FLUSH_INTERVAL : interval;

            return BulkProcessor.builder(client, new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request) {
                    logger.debug("executing [{}] bulk index requests", request.numberOfActions());
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                    if (response.hasFailures()) {
                        logger.info("failed to bulk index marvel documents: [{}]", response.buildFailureMessage());
                    }
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                    logger.error("failed to bulk index marvel documents: [{}]", failure, failure.getMessage());
                }
            }).setName("marvel-bulk-processor-" + config.name())
                    .setBulkActions(bulkSize)
                    .setFlushInterval(interval)
                    .setConcurrentRequests(1)
                    .build();
        }
    }

    public enum State {
        INITIALIZED,
        STARTING,
        STARTED,
        STOPPING,
        STOPPED,
        FAILED
    }
}
