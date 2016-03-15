/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter.local;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.marvel.agent.exporter.ExportBulk;
import org.elasticsearch.marvel.agent.exporter.ExportException;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;
import org.elasticsearch.marvel.agent.resolver.MonitoringIndexNameResolver;
import org.elasticsearch.marvel.agent.resolver.ResolversRegistry;
import org.elasticsearch.marvel.support.init.proxy.MonitoringClientProxy;

import java.util.Arrays;
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
    public synchronized ExportBulk add(Collection<MonitoringDoc> docs) throws ExportException {
        ExportException exception = null;

        for (MonitoringDoc doc : docs) {
            if (state.get() != State.ACTIVE) {
                return this;
            }
            if (requestBuilder == null) {
                requestBuilder = client.prepareBulk();
            }

            try {
                MonitoringIndexNameResolver<MonitoringDoc> resolver = resolvers.getResolver(doc);
                IndexRequest request = new IndexRequest(resolver.index(doc), resolver.type(doc), resolver.id(doc));
                request.source(resolver.source(doc, XContentType.SMILE));
                requestBuilder.add(request);

                if (logger.isTraceEnabled()) {
                    logger.trace("local exporter [{}] - added index request [index={}, type={}, id={}]",
                            name, request.index(), request.type(), request.id());
                }
            } catch (Exception e) {
                if (exception == null) {
                    exception = new ExportException("failed to add documents to export bulk [{}]", name);
                }
                exception.addExportException(new ExportException("failed to add document [{}]", e,  doc, name));
            }
        }

        if (exception != null) {
            throw exception;
        }

        return this;
    }

    @Override
    public void flush() throws ExportException {
        if (state.get() != State.ACTIVE || requestBuilder == null || requestBuilder.numberOfActions() == 0) {
            return;
        }
        try {
            logger.trace("exporter [{}] - exporting {} documents", name, requestBuilder.numberOfActions());
            BulkResponse bulkResponse = requestBuilder.get();

            if (bulkResponse.hasFailures()) {
                throwExportException(bulkResponse.getItems());
            }
        } catch (Exception e) {
            throw new ExportException("failed to flush export bulk [{}]", e, name);
        } finally {
            requestBuilder = null;
        }
    }

    void throwExportException(BulkItemResponse[] bulkItemResponses) {
        ExportException exception = new ExportException("bulk [{}] reports failures when exporting documents", name);

        Arrays.stream(bulkItemResponses)
                .filter(BulkItemResponse::isFailed)
                .map(item -> new ExportException(item.getFailure().getCause()))
                .forEach(exception::addExportException);

        if (exception.hasExportExceptions()) {
            throw exception;
        }
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
