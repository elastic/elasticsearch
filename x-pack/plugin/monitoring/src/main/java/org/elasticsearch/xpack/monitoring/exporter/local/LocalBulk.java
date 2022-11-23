/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter.local;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.exporter.ExportBulk;
import org.elasticsearch.xpack.monitoring.exporter.ExportException;

import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.xpack.core.ClientHelper.MONITORING_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * LocalBulk exports monitoring data in the local cluster using bulk requests. Its usage is not thread safe since the
 * {@link LocalBulk#add(Collection)} and {@link LocalBulk#flush(org.elasticsearch.action.ActionListener)}
 * methods are not synchronized.
 */
public class LocalBulk extends ExportBulk {

    private final Logger logger;
    private final Client client;
    private final DateFormatter formatter;

    private BulkRequestBuilder requestBuilder;

    LocalBulk(String name, Logger logger, Client client, DateFormatter dateTimeFormatter) {
        super(name, client.threadPool().getThreadContext());
        this.logger = logger;
        this.client = client;
        this.formatter = dateTimeFormatter;
    }

    @Override
    protected void doAdd(Collection<MonitoringDoc> docs) throws ExportException {
        ExportException exception = null;

        for (MonitoringDoc doc : docs) {
            if (requestBuilder == null) {
                requestBuilder = client.prepareBulk();
            }

            try {
                final String index = MonitoringTemplateUtils.indexName(formatter, doc.getSystem(), doc.getTimestamp());

                final IndexRequest request = new IndexRequest(index);
                if (Strings.hasText(doc.getId())) {
                    request.id(doc.getId());
                }

                final BytesReference source = XContentHelper.toXContent(doc, XContentType.SMILE, false);
                request.source(source, XContentType.SMILE);

                requestBuilder.add(request);

                if (logger.isTraceEnabled()) {
                    logger.trace(
                        "local exporter [{}] - added index request [index={}, id={}, pipeline={}, monitoring data type={}]",
                        name,
                        request.index(),
                        request.id(),
                        request.getPipeline(),
                        doc.getType()
                    );
                }
            } catch (Exception e) {
                if (exception == null) {
                    exception = new ExportException("failed to add documents to export bulk [{}]", name);
                }
                exception.addExportException(new ExportException("failed to add document [{}]", e, doc, name));
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    @Override
    protected void doFlush(ActionListener<Void> listener) {
        if (requestBuilder == null || requestBuilder.numberOfActions() == 0) {
            listener.onResponse(null);
        } else {
            try {
                logger.trace("exporter [{}] - exporting {} documents", name, requestBuilder.numberOfActions());
                executeAsyncWithOrigin(
                    client.threadPool().getThreadContext(),
                    MONITORING_ORIGIN,
                    requestBuilder.request(),
                    ActionListener.<BulkResponse>wrap(bulkResponse -> {
                        if (bulkResponse.hasFailures()) {
                            throwExportException(bulkResponse.getItems(), listener);
                        } else {
                            listener.onResponse(null);
                        }
                    }, e -> listener.onFailure(new ExportException("failed to flush export bulk [{}]", e, name))),
                    client::bulk
                );
            } finally {
                requestBuilder = null;
            }
        }
    }

    void throwExportException(BulkItemResponse[] bulkItemResponses, ActionListener<Void> listener) {
        ExportException exception = new ExportException("bulk [{}] reports failures when exporting documents", name);

        Arrays.stream(bulkItemResponses)
            .filter(BulkItemResponse::isFailed)
            .map(item -> new ExportException(item.getFailure().getCause()))
            .forEach(exception::addExportException);

        if (exception.hasExportExceptions()) {
            for (ExportException e : exception) {
                logger.warn("unexpected error while indexing monitoring document", e);
            }
            listener.onFailure(exception);
        } else {
            listener.onResponse(null);
        }
    }

}
