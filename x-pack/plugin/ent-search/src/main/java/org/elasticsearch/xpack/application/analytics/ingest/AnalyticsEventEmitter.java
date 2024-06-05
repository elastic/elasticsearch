/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.ingest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkProcessor2;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.application.analytics.AnalyticsCollection;
import org.elasticsearch.xpack.application.analytics.AnalyticsCollectionResolver;
import org.elasticsearch.xpack.application.analytics.action.PostAnalyticsEventAction;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEventFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;

public class AnalyticsEventEmitter extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(AnalyticsEventEmitter.class);

    private final Client client;

    private final BulkProcessor2 bulkProcessor;

    private final AnalyticsEventFactory eventFactory;

    private final AnalyticsCollectionResolver collectionResolver;

    private final AtomicBoolean dropEvent = new AtomicBoolean(false);

    @Inject
    public AnalyticsEventEmitter(Client client, BulkProcessorFactory bulkProcessorFactory, AnalyticsCollectionResolver collectionResolver) {
        this(client, bulkProcessorFactory.create(), collectionResolver, AnalyticsEventFactory.INSTANCE);
    }

    AnalyticsEventEmitter(
        Client client,
        BulkProcessor2 bulkProcessor,
        AnalyticsCollectionResolver collectionResolver,
        AnalyticsEventFactory eventFactory
    ) {
        this.client = new OriginSettingClient(client, ENT_SEARCH_ORIGIN);
        this.bulkProcessor = bulkProcessor;
        this.eventFactory = eventFactory;
        this.collectionResolver = collectionResolver;
    }

    /**
     * Emits an analytics event to be persisted by bulk processor.
     *
     * @param request the request containing the analytics event data
     * @param listener the listener to call once the event has been emitted
     */
    public void emitEvent(
        final PostAnalyticsEventAction.Request request,
        final ActionListener<PostAnalyticsEventAction.Response> listener
    ) {
        try {
            AnalyticsEvent event = eventFactory.fromRequest(request);
            IndexRequest eventIndexRequest = createIndexRequest(event);

            bulkProcessor.add(eventIndexRequest);

            if (dropEvent.compareAndSet(true, false)) {
                logger.warn("Bulk processor has been flushed. Accepting new events again.");
            }

            if (request.isDebug()) {
                listener.onResponse(new PostAnalyticsEventAction.DebugResponse(true, event));
            } else {
                listener.onResponse(PostAnalyticsEventAction.Response.ACCEPTED);
            }
        } catch (IOException e) {
            listener.onFailure(new ElasticsearchException("Unable to parse the event.", e));
        } catch (EsRejectedExecutionException e) {
            listener.onFailure(
                new ElasticsearchStatusException("Unable to add the event: too many requests.", RestStatus.TOO_MANY_REQUESTS)
            );

            if (dropEvent.compareAndSet(false, true)) {
                logger.warn("Bulk processor is full. Start dropping events.");
            }
        }
    }

    private IndexRequest createIndexRequest(AnalyticsEvent event) throws IOException {
        AnalyticsCollection collection = this.collectionResolver.collection(event.eventCollectionName());

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            return client.prepareIndex(collection.getEventDataStream())
                .setCreate(true)
                .setSource(event.toXContent(builder, ToXContent.EMPTY_PARAMS))
                .request();
        }
    }

    @Override
    protected void doStart() {
        // Nothing to do to start the event emitter.
    }

    @Override
    protected void doStop() {
        // Nothing to do to stop the event emitter.
    }

    @Override
    protected void doClose() {
        // Ensure the bulk processor is closed, so pending requests are flushed.
        bulkProcessor.close();
    }
}
