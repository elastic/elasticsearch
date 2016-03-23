/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A monitoring bulk request holds one or more {@link MonitoringBulkDoc}s.
 * <p>
 * Every monitoring document added to the request is associated to a monitoring system id and version. If this {id, version} pair is
 * supported by the monitoring plugin, the monitoring documents will be indexed in a single batch using a normal bulk request.
 * <p>
 * The monitoring {id, version} pair is used by {org.elasticsearch.marvel.agent.resolver.MonitoringIndexNameResolver} to resolve the index,
 * type and id of the final document to be indexed. A {@link MonitoringBulkDoc} can also hold its own index/type/id values but there's no
 * guarantee that these information will be effectively used.
 */
public class MonitoringBulkRequest extends ActionRequest<MonitoringBulkRequest> {

    final List<MonitoringBulkDoc> docs = new ArrayList<>();

    /**
     * @return the list of monitoring documents to be indexed
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
            if (Strings.hasLength(doc.getMonitoringId()) == false) {
                validationException = addValidationError("monitored system id is missing for monitoring document [" + i + "]",
                        validationException);
            }
            if (Strings.hasLength(doc.getMonitoringVersion()) == false) {
                validationException = addValidationError("monitored system version is missing for monitoring document [" + i + "]",
                        validationException);
            }
            if (Strings.hasLength(doc.getType()) == false) {
                validationException = addValidationError("type is missing for monitoring document [" + i + "]",
                        validationException);
            }
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
    public MonitoringBulkRequest add(BytesReference content, String defaultMonitoringId, String defaultMonitoringVersion,
                                     String defaultIndex, String defaultType) throws Exception {
        // MonitoringBulkRequest accepts a body request that has the same format as the BulkRequest:
        // instead of duplicating the parsing logic here we use a new BulkRequest instance to parse the content.
        BulkRequest bulkRequest = Requests.bulkRequest().add(content, defaultIndex, defaultType);

        for (ActionRequest request : bulkRequest.requests()) {
            if (request instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) request;

                // builds a new monitoring document based on the index request
                MonitoringBulkDoc doc = new MonitoringBulkDoc(defaultMonitoringId, defaultMonitoringVersion);
                doc.setIndex(indexRequest.index());
                doc.setType(indexRequest.type());
                doc.setId(indexRequest.id());
                doc.setSource(indexRequest.source());
                add(doc);
            } else {
                throw new IllegalArgumentException("monitoring bulk requests should only contain index requests");
            }
        }
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            add(new MonitoringBulkDoc(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(docs.size());
        for (MonitoringBulkDoc doc : docs) {
            doc.writeTo(out);
        }
    }
}
