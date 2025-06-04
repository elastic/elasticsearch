/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.ingest;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Holds the end result of what a pipeline did to sample document provided via the simulate api.
 */
public final class SimulateDocumentBaseResult implements SimulateDocumentResult {
    private final WriteableIngestDocument ingestDocument;
    private final Exception failure;

    public static final ConstructingObjectParser<SimulateDocumentBaseResult, Void> PARSER = new ConstructingObjectParser<>(
        "simulate_document_base_result",
        true,
        a -> {
            if (a[1] == null) {
                assert a[0] != null;
                return new SimulateDocumentBaseResult(((WriteableIngestDocument) a[0]).getIngestDocument());
            } else {
                assert a[0] == null;
                return new SimulateDocumentBaseResult((ElasticsearchException) a[1]);
            }
        }
    );
    static {
        PARSER.declareObject(
            optionalConstructorArg(),
            WriteableIngestDocument.INGEST_DOC_PARSER,
            new ParseField(WriteableIngestDocument.DOC_FIELD)
        );
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> ElasticsearchException.fromXContent(p), new ParseField("error"));
    }

    public SimulateDocumentBaseResult(IngestDocument ingestDocument) {
        Exception failure = null;
        WriteableIngestDocument wid = null;
        if (ingestDocument != null) {
            try {
                wid = new WriteableIngestDocument(ingestDocument);
            } catch (Exception ex) {
                failure = ex;
            }
        }
        this.ingestDocument = wid;
        this.failure = failure;
    }

    public SimulateDocumentBaseResult(Exception failure) {
        this.ingestDocument = null;
        this.failure = failure;
    }

    /**
     * Read from a stream.
     */
    public SimulateDocumentBaseResult(StreamInput in) throws IOException {
        failure = in.readException();
        ingestDocument = in.readOptionalWriteable(WriteableIngestDocument::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeException(failure);
        out.writeOptionalWriteable(ingestDocument);
    }

    public IngestDocument getIngestDocument() {
        if (ingestDocument == null) {
            return null;
        }
        return ingestDocument.getIngestDocument();
    }

    public Exception getFailure() {
        return failure;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (failure == null && ingestDocument == null) {
            builder.nullValue();
            return builder;
        }

        builder.startObject();
        if (failure == null) {
            ingestDocument.toXContent(builder, params);
        } else {
            ElasticsearchException.generateFailureXContent(builder, params, failure, true);
        }
        builder.endObject();
        return builder;
    }

    public static SimulateDocumentBaseResult fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
