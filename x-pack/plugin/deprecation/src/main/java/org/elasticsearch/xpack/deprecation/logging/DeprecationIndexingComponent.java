/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation.logging;

import co.elastic.logging.log4j2.EcsLayout;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor2;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.logging.ECSJsonLayout;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.logging.RateLimitingFilter;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.deprecation.Deprecation.WRITE_DEPRECATION_LOGS_TO_INDEX;

/**
 * This component manages the construction and lifecycle of the {@link DeprecationIndexingAppender}.
 * It also starts and stops the appender
 */
public class DeprecationIndexingComponent extends AbstractLifecycleComponent implements ClusterStateListener {

    public static final Setting<TimeValue> DEPRECATION_INDEXING_FLUSH_INTERVAL = Setting.timeSetting(
        "cluster.deprecation_indexing.flush_interval",
        TimeValue.timeValueSeconds(5),
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(DeprecationIndexingComponent.class);

    private final DeprecationIndexingAppender appender;
    private final BulkProcessor2 processor;
    /*
     * We do not want to index deprecation logs on server startup before the index template and ILM policy are available. So we have
     * processor put them in the following buffer until the server is ready. Once the template and ILM policy are available, this buffer is
     * drained and its contents are sent to the processor. The queue is unbounded because we are first going through processor::add, which
     * starts rejecting documents if the total number of bytes in flight gets too large.
     */
    private final ConcurrentLinkedQueue<Runnable> pendingRequestsBuffer = new ConcurrentLinkedQueue<>();
    private final RateLimitingFilter rateLimitingFilterForIndexing;
    private final ClusterService clusterService;

    /*
     * False until the deprecation index template and ILM policy exist (indicating that we should not flush deprecation logs to
     * Elasticsearch yet).
     */
    private final AtomicBoolean flushEnabled = new AtomicBoolean(false);

    private DeprecationIndexingComponent(
        Client client,
        Settings settings,
        RateLimitingFilter rateLimitingFilterForIndexing,
        boolean enableDeprecationLogIndexingDefault,
        ClusterService clusterService
    ) {
        this.rateLimitingFilterForIndexing = rateLimitingFilterForIndexing;
        this.clusterService = clusterService;

        this.processor = getBulkProcessor(new OriginSettingClient(client, ClientHelper.DEPRECATION_ORIGIN), settings);
        final Consumer<IndexRequest> consumer = processor::add;

        final LoggerContext context = (LoggerContext) LogManager.getContext(false);
        final Configuration configuration = context.getConfiguration();

        final EcsLayout ecsLayout = ECSJsonLayout.newBuilder()
            .setDataset("deprecation.elasticsearch")
            .setConfiguration(configuration)
            .build();

        this.appender = new DeprecationIndexingAppender(
            "deprecation_indexing_appender",
            rateLimitingFilterForIndexing,
            ecsLayout,
            consumer
        );
        enableDeprecationLogIndexing(enableDeprecationLogIndexingDefault);

    }

    public static DeprecationIndexingComponent createDeprecationIndexingComponent(
        Client client,
        Settings settings,
        RateLimitingFilter rateLimitingFilterForIndexing,
        boolean enableDeprecationLogIndexingDefault,
        ClusterService clusterService
    ) {
        final DeprecationIndexingComponent deprecationIndexingComponent = new DeprecationIndexingComponent(
            client,
            settings,
            rateLimitingFilterForIndexing,
            enableDeprecationLogIndexingDefault,
            clusterService
        );

        clusterService.addListener(deprecationIndexingComponent);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(WRITE_DEPRECATION_LOGS_TO_INDEX, deprecationIndexingComponent::enableDeprecationLogIndexing);

        return deprecationIndexingComponent;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (flushEnabled.get()) {
            return;
        }
        if (event.metadataChanged() == false) {
            return;
        }
        final IndexLifecycleMetadata indexLifecycleMetadata = event.state().metadata().custom(IndexLifecycleMetadata.TYPE);

        if (event.state().getMetadata().templatesV2().containsKey(".deprecation-indexing-template")
            && indexLifecycleMetadata != null
            && indexLifecycleMetadata.getPolicies().containsKey(".deprecation-indexing-ilm-policy")) {
            flushEnabled.set(true);
            flushBuffer();
            logger.debug("Deprecation log indexing started, because both template and ilm policy are loaded");
            clusterService.removeListener(this);
        }
    }

    /**
     * This method removes everything that is currently in the pendingRequestsBuffer and sends it to the client. This method is
     * threadsafe. Anything added to the pendingRequestsBuffer while this method is executing might be removed and sent to the client,
     * but there is no blocking so there is no guarantee of it.
     */
    private void flushBuffer() {
        for (Runnable pendingRequest = pendingRequestsBuffer.poll(); pendingRequest != null; pendingRequest = pendingRequestsBuffer
            .poll()) {
            pendingRequest.run();
        }
    }

    @Override
    protected void doStart() {
        logger.info("deprecation component started");
        this.appender.start();
        Loggers.addAppender(LogManager.getLogger("org.elasticsearch.deprecation"), this.appender);
    }

    @Override
    protected void doStop() {
        Loggers.removeAppender(LogManager.getLogger("org.elasticsearch.deprecation"), this.appender);
        flushEnabled.set(false);
        this.appender.stop();
    }

    @Override
    protected void doClose() {
        this.processor.close();
    }

    public void enableDeprecationLogIndexing(boolean newEnabled) {
        if (appender.isEnabled() != newEnabled) {
            appender.setEnabled(newEnabled);

            // We've flipped from disabled to enabled. Make sure we start with a clean cache of
            // previously-seen keys, otherwise we won't index anything.
            if (newEnabled) {
                this.rateLimitingFilterForIndexing.reset();
            }
        }
    }

    /**
     * Constructs a bulk processor for writing documents
     *
     * @param client   the client to use
     * @param settings the settings to use
     * @return an initialised bulk processor
     */
    private BulkProcessor2 getBulkProcessor(Client client, Settings settings) {
        TimeValue flushInterval = DEPRECATION_INDEXING_FLUSH_INTERVAL.get(settings);
        BulkProcessor2.Listener listener = new DeprecationBulkListener();
        return BulkProcessor2.builder((bulkRequest, actionListener) -> {
            /*
             * If flush is enabled already, we just call client::bulk. But if it is not ready then we store the request and listener in a
             * queue. We do this here because at this point the bulk processor will have already rejected the request if the
             * in-flight-bytes limit has been exceeded. This means that we don't have to worry about bounding pendingRequestsBuffer.
             */
            if (flushEnabled.get()) {
                logger.trace("Flush is enabled, sending a bulk request");
                client.bulk(bulkRequest, actionListener);
                flushBuffer(); // just in case something was missed after the first flush
            } else {
                logger.trace("Flush is disabled, scheduling a bulk request");

                // this is an unbounded queue, so the entry will always be accepted
                pendingRequestsBuffer.offer(() -> client.bulk(bulkRequest, actionListener));
            }
        }, listener, client.threadPool()).setMaxNumberOfRetries(3).setFlushInterval(flushInterval).build();
    }

    private static class DeprecationBulkListener implements BulkProcessor2.Listener {
        @Override
        public void beforeBulk(long executionId, BulkRequest request) {}

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            long numberOfActions = request.numberOfActions();
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "indexed [{}] deprecation documents into [{}]",
                    numberOfActions,
                    Arrays.stream(response.getItems()).map(BulkItemResponse::getIndex).distinct().collect(Collectors.joining(","))
                );
            }

            if (response.hasFailures()) {
                List<String> failures = Arrays.stream(response.getItems())
                    .filter(BulkItemResponse::isFailed)
                    .map(r -> r.getId() + " " + r.getFailureMessage())
                    .collect(Collectors.toList());
                logger.error("Bulk write of deprecation logs encountered some failures: [{}]", failures);
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Exception failure) {
            logger.error("Bulk write of " + request.numberOfActions() + " deprecation logs failed: " + failure.getMessage(), failure);
        }
    }
}
