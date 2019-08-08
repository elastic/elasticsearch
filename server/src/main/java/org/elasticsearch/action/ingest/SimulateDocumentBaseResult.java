/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.ingest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.ingest.IngestDocument;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Holds the end result of what a pipeline did to sample document provided via the simulate api.
 */
public final class SimulateDocumentBaseResult implements SimulateDocumentResult {
    private final WriteableIngestDocument ingestDocument;
    private final Exception failure;

    public static final ConstructingObjectParser<SimulateDocumentBaseResult, Void> PARSER =
        new ConstructingObjectParser<>(
          "simulate_document_base_result",
          true,
          a -> {
            if (a[1] == null) {
                assert a[0] != null;
                return new SimulateDocumentBaseResult(((WriteableIngestDocument)a[0]).getIngestDocument());
            } else {
                assert a[0] == null;
                return new SimulateDocumentBaseResult((ElasticsearchException)a[1]);
            }
          }
        );
    static {
        PARSER.declareObject(
            optionalConstructorArg(),
            WriteableIngestDocument.INGEST_DOC_PARSER,
            new ParseField(WriteableIngestDocument.DOC_FIELD)
        );
        PARSER.declareObject(
            optionalConstructorArg(),
            (p, c) -> ElasticsearchException.fromXContent(p),
            new ParseField("error")
        );
    }

    public SimulateDocumentBaseResult(IngestDocument ingestDocument) {
        if (ingestDocument != null) {
            this.ingestDocument = new WriteableIngestDocument(ingestDocument);
        } else {
            this.ingestDocument = null;
        }
        this.failure = null;
    }

    public SimulateDocumentBaseResult(Exception failure) {
        this.ingestDocument = null;
        this.failure = failure;
    }

    /**
     * Read from a stream.
     */
    public SimulateDocumentBaseResult(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_7_4_0)) {
            failure = in.readException();
            ingestDocument = in.readOptionalWriteable(WriteableIngestDocument::new);
        } else {
            if (in.readBoolean()) {
                ingestDocument = null;
                failure = in.readException();
            } else {
                ingestDocument = new WriteableIngestDocument(in);
                failure = null;
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_7_4_0)) {
            out.writeException(failure);
            out.writeOptionalWriteable(ingestDocument);
        } else {
            if (failure == null) {
                out.writeBoolean(false);
                ingestDocument.writeTo(out);
            } else {
                out.writeBoolean(true);
                out.writeException(failure);
            }
        }
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
