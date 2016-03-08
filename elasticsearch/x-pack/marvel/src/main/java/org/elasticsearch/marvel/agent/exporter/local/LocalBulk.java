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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.marvel.agent.exporter.ExportBulk;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;
import org.elasticsearch.marvel.agent.resolver.MonitoringIndexNameResolver;
import org.elasticsearch.marvel.agent.resolver.ResolversRegistry;
import org.elasticsearch.marvel.support.init.proxy.MonitoringClientProxy;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class LocalBulk extends ExportBulk {

    private final ESLogger logger;
    private final MonitoringClientProxy client;
    private final ResolversRegistry resolvers;

    BulkRequestBuilder requestBuilder;
    AtomicReference<State> state = new AtomicReference<>();

    public LocalBulk(String name, ESLogger logger, MonitoringClientProxy client, ResolversRegistry resolvers) {
        super(name);
        this.logger = logger;
        this.client = client;
        this.resolvers = resolvers;
        state.set(State.ACTIVE);
    }

    @Override
    public synchronized ExportBulk add(Collection<MonitoringDoc> docs) throws Exception {
        for (MonitoringDoc doc : docs) {
            if (state.get() != State.ACTIVE) {
                return this;
            }
            if (requestBuilder == null) {
                requestBuilder = client.prepareBulk();
            }

            try {
                MonitoringIndexNameResolver<MonitoringDoc> resolver = resolvers.getResolver(doc);
                if (resolver != null) {
                    IndexRequest request = new IndexRequest(resolver.index(doc), resolver.type(doc), resolver.id(doc));
                    request.source(resolver.source(doc, XContentType.SMILE));
                    requestBuilder.add(request);

                    if (logger.isTraceEnabled()) {
                        logger.trace("local exporter [{}] - added index request [index={}, type={}, id={}]",
                                name, request.index(), request.type(), request.id());
                    }
                } else {
                    logger.warn("local exporter [{}] - unable to render monitoring document of type [{}]: no renderer found in registry",
                            name, doc);
                }
            } catch (Exception e) {
                logger.warn("local exporter [{}] - failed to add document [{}], skipping it", e, name, doc);
            }
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
        }
    }

    void terminate() {
        state.set(State.TERMINATING);
        synchronized (this) {
            requestBuilder = null;
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
