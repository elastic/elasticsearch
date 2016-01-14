/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter.local;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.marvel.agent.exporter.ExportBulk;
import org.elasticsearch.marvel.agent.exporter.MonitoringIndexNameResolver;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.agent.renderer.Renderer;
import org.elasticsearch.marvel.agent.renderer.RendererRegistry;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class LocalBulk extends ExportBulk {

    private final ESLogger logger;
    private final Client client;
    private final MonitoringIndexNameResolver indexNameResolver;
    private final RendererRegistry renderers;

    private BytesStreamOutput buffer = null;
    BulkRequestBuilder requestBuilder;

    AtomicReference<State> state = new AtomicReference<>();

    public LocalBulk(String name, ESLogger logger, Client client, MonitoringIndexNameResolver indexNameResolver, RendererRegistry renderers) {
        super(name);
        this.logger = logger;
        this.client = client;
        this.indexNameResolver = indexNameResolver;
        this.renderers = renderers;
        state.set(State.ACTIVE);
    }

    @Override
    public synchronized ExportBulk add(Collection<MarvelDoc> docs) throws Exception {
        for (MarvelDoc marvelDoc : docs) {
            if (state.get() != State.ACTIVE) {
                return this;
            }
            if (requestBuilder == null) {
                requestBuilder = client.prepareBulk();
            }

            // Get the appropriate renderer in order to render the MarvelDoc
            Renderer renderer = renderers.getRenderer(marvelDoc);
            assert renderer != null : "unable to render monitoring document of type [" + marvelDoc.getType() + "]. no renderer registered";

            if (renderer == null) {
                logger.warn("local exporter [{}] - unable to render monitoring document of type [{}]: no renderer found in registry",
                        name, marvelDoc.getType());
                continue;
            }

            IndexRequestBuilder request = client.prepareIndex();

            // we need the index to be based on the document timestamp and/or template version
            request.setIndex(indexNameResolver.resolve(marvelDoc));

            if (marvelDoc.getType() != null) {
                request.setType(marvelDoc.getType());
            }
            if (marvelDoc.getId() != null) {
                request.setId(marvelDoc.getId());
            }

            if (buffer == null) {
                buffer = new BytesStreamOutput();
            } else {
                buffer.reset();
            }

            renderer.render(marvelDoc, XContentType.SMILE, buffer);
            request.setSource(buffer.bytes().toBytes());

            requestBuilder.add(request);
        }
        return this;
    }

    @Override
    public void flush() throws IOException {
        if (state.get() != State.ACTIVE || requestBuilder == null) {
            return;
        }
        try {
            logger.trace("exporter [{}] - exporting {} documents", name, requestBuilder.numberOfActions());
            BulkResponse bulkResponse = requestBuilder.get();
            if (bulkResponse.hasFailures()) {
                throw new ElasticsearchException(buildFailureMessage(bulkResponse));
            }
        } finally {
            requestBuilder = null;
            if (buffer != null) {
                buffer.reset();
            }
        }
    }

    void terminate() {
        state.set(State.TERMINATING);
        synchronized (this) {
            requestBuilder = null;
            buffer = null;
            state.compareAndSet(State.TERMINATING, State.TERMINATED);
        }
    }

    /**
     * In case of something goes wrong and there's a lot of shards/indices,
     * we limit the number of failures displayed in log.
     */
    private String buildFailureMessage(BulkResponse bulkResponse) {
        BulkItemResponse[] items = bulkResponse.getItems();

        if (logger.isDebugEnabled() || (items.length < 100)) {
            return bulkResponse.buildFailureMessage();
        }

        StringBuilder sb = new StringBuilder();
        sb.append("failure in bulk execution, only the first 100 failures are printed:");
        for (int i = 0; i < items.length && i < 100; i++) {
            BulkItemResponse item = items[i];
            if (item.isFailed()) {
                sb.append("\n[").append(i)
                        .append("]: index [").append(item.getIndex()).append("], type [").append(item.getType())
                        .append("], id [").append(item.getId()).append("], message [").append(item.getFailureMessage())
                        .append("]");
            }
        }
        return sb.toString();
    }

    enum State {
        ACTIVE,
        TERMINATING,
        TERMINATED
    }
}
