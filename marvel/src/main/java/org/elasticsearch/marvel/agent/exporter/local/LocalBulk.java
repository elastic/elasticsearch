/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter.local;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.marvel.agent.exporter.ExportBulk;
import org.elasticsearch.marvel.agent.exporter.IndexNameResolver;
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
    private final IndexNameResolver indexNameResolver;
    private final RendererRegistry renderers;

    private BytesStreamOutput buffer = null;
    private BulkRequestBuilder requestBuilder;

    AtomicReference<State> state = new AtomicReference<>();

    public LocalBulk(String name, ESLogger logger, Client client, IndexNameResolver indexNameResolver, RendererRegistry renderers) {
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

            IndexRequestBuilder request = client.prepareIndex();
            if (marvelDoc.index() != null) {
                request.setIndex(marvelDoc.index());
            } else {
                request.setIndex(indexNameResolver.resolve(marvelDoc));
            }
            if (marvelDoc.type() != null) {
                request.setType(marvelDoc.type());
            }
            if (marvelDoc.id() != null) {
                request.setId(marvelDoc.id());
            }

            // Get the appropriate renderer in order to render the MarvelDoc
            Renderer renderer = renderers.renderer(marvelDoc.type());
            assert renderer != null : "unable to render marvel document of type [" + marvelDoc.type() + "]. no renderer found in registry";

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
        logger.trace("exporter [{}] - exporting data...", name);
//        long start = System.nanoTime(); TODO remove
        BulkResponse bulkResponse = requestBuilder.get();
//        TimeValue time = TimeValue.timeValueNanos(System.nanoTime() - start);
//        logger.trace("exporter [{}] - data exported, took [{}] seconds", name, time.format(PeriodType.seconds()));
        if (bulkResponse.hasFailures()) {
            throw new ElasticsearchException(bulkResponse.buildFailureMessage());
        }
        requestBuilder = null;
    }

    void terminate() {
        state.set(State.TERMINATING);
        synchronized (this) {
            requestBuilder = null;
            state.compareAndSet(State.TERMINATING, State.TERMINATED);
        }
    }

    enum State {
        ACTIVE,
        TERMINATING,
        TERMINATED
    }
}
