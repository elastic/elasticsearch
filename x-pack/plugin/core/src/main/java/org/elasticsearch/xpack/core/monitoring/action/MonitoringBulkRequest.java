/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.monitoring.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.bulk.BulkRequestParser;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A monitoring bulk request holds one or more {@link MonitoringBulkDoc}s.
 * <p>
 * Every monitoring document added to the request is associated to a {@link MonitoredSystem}. The monitored system is used
 * to resolve the index name in which the document will be indexed into.
 */
public class MonitoringBulkRequest extends ActionRequest {

    private final List<MonitoringBulkDoc> docs = new ArrayList<>();

    public MonitoringBulkRequest() {}

    public MonitoringBulkRequest(StreamInput in) throws IOException {
        super(in);
        docs.addAll(in.readList(MonitoringBulkDoc::new));
    }

    /**
     * @return the list of {@link MonitoringBulkDoc} to be indexed
     */
    public Collection<MonitoringBulkDoc> getDocs() {
        return Collections.unmodifiableCollection(new ArrayList<>(this.docs));
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (docs.isEmpty()) {
            validationException = addValidationError("no monitoring documents added", validationException);
        }
        for (int i = 0; i < docs.size(); i++) {
            MonitoringBulkDoc doc = docs.get(i);
            if (doc.getSource() == null || doc.getSource().length() == 0) {
                validationException = addValidationError("source is missing for monitoring document [" + i + "]", validationException);
            }
        }
        return validationException;
    }

    /**
     * Adds a monitoring document to the list of documents to be indexed.
     */
    public MonitoringBulkRequest add(MonitoringBulkDoc doc) {
        docs.add(doc);
        return this;
    }

    /**
     * Parses a monitoring bulk request and builds the list of documents to be indexed.
     */
    public MonitoringBulkRequest add(
        final MonitoredSystem system,
        final BytesReference content,
        final XContentType xContentType,
        final long timestamp,
        final long intervalMillis
    ) throws IOException {

        // MonitoringBulkRequest accepts a body request that has the same format as the BulkRequest
        new BulkRequestParser(false, RestApiVersion.current()).parse(
            content,
            null,
            null,
            null,
            null,
            null,
            true,
            xContentType,
            (indexRequest, type) -> {
                // we no longer accept non-timestamped indexes from Kibana, LS, or Beats because we do not use the data
                // and it was duplicated anyway; by simply dropping it, we allow BWC for older clients that still send it
                if (MonitoringIndex.from(indexRequest.index()) != MonitoringIndex.TIMESTAMPED) {
                    return;
                }
                final BytesReference source = indexRequest.source();
                if (source.length() == 0) {
                    throw new IllegalArgumentException(
                        "source is missing for monitoring document [" + indexRequest.index() + "][" + type + "][" + indexRequest.id() + "]"
                    );
                }

                // builds a new monitoring document based on the index request
                add(new MonitoringBulkDoc(system, type, indexRequest.id(), timestamp, intervalMillis, source, xContentType));
            },
            updateRequest -> { throw new IllegalArgumentException("monitoring bulk requests should only contain index requests"); },
            deleteRequest -> {
                throw new IllegalArgumentException("monitoring bulk requests should only contain index requests");
            }
        );

        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(docs);
    }
}
