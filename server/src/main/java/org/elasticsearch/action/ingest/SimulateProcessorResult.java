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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class SimulateProcessorResult implements Writeable, ToXContentObject {

    private static final String IGNORED_ERROR_FIELD = "ignored_error";
    private final String processorTag;
    private final String description;
    private final WriteableIngestDocument ingestDocument;
    private final Exception failure;

    private static final ConstructingObjectParser<ElasticsearchException, Void> IGNORED_ERROR_PARSER =
        new ConstructingObjectParser<>(
            "ignored_error_parser",
            true,
            a -> (ElasticsearchException)a[0]
        );
    static {
        IGNORED_ERROR_PARSER.declareObject(
            constructorArg(),
            (p, c) -> ElasticsearchException.fromXContent(p),
            new ParseField("error")
        );
    }

    public static final ConstructingObjectParser<SimulateProcessorResult, Void> PARSER =
        new ConstructingObjectParser<>(
            "simulate_processor_result",
            true,
            a -> {
                String processorTag = a[0] == null ? null : (String)a[0];
                String description = a[1] == null ? null : (String)a[1];
                IngestDocument document = a[2] == null ? null : ((WriteableIngestDocument)a[2]).getIngestDocument();
                Exception failure = null;
                if (a[3] != null) {
                    failure = (ElasticsearchException)a[3];
                } else if (a[4] != null) {
                    failure = (ElasticsearchException)a[4];
                }
                return new SimulateProcessorResult(processorTag, description, document, failure);
            }
        );
    static {
        PARSER.declareString(optionalConstructorArg(), new ParseField(ConfigurationUtils.TAG_KEY));
        PARSER.declareString(optionalConstructorArg(), new ParseField(ConfigurationUtils.DESCRIPTION_KEY));
        PARSER.declareObject(
            optionalConstructorArg(),
            WriteableIngestDocument.INGEST_DOC_PARSER,
            new ParseField(WriteableIngestDocument.DOC_FIELD)
        );
        PARSER.declareObject(
            optionalConstructorArg(),
            IGNORED_ERROR_PARSER,
            new ParseField(IGNORED_ERROR_FIELD)
        );
        PARSER.declareObject(
            optionalConstructorArg(),
            (p, c) -> ElasticsearchException.fromXContent(p),
            new ParseField("error")
        );
    }

    public SimulateProcessorResult(String processorTag, String description, IngestDocument ingestDocument,
                                   Exception failure) {
        this.processorTag = processorTag;
        this.description = description;
        this.ingestDocument = (ingestDocument == null) ? null : new WriteableIngestDocument(ingestDocument);
        this.failure = failure;
    }

    public SimulateProcessorResult(String processorTag, String description, IngestDocument ingestDocument) {
        this(processorTag, description, ingestDocument, null);
    }

    public SimulateProcessorResult(String processorTag, String description, Exception failure) {
        this(processorTag, description, null, failure);
    }

    public SimulateProcessorResult(String processorTag, String description) {
        this(processorTag, description, null, null);
    }

    /**
     * Read from a stream.
     */
    SimulateProcessorResult(StreamInput in) throws IOException {
        this.processorTag = in.readString();
        this.ingestDocument = in.readOptionalWriteable(WriteableIngestDocument::new);
        this.failure = in.readException();
        if (in.getVersion().onOrAfter(Version.V_7_9_0)) {
            this.description = in.readOptionalString();
        } else {
            this.description = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(processorTag);
        out.writeOptionalWriteable(ingestDocument);
        out.writeException(failure);
        if (out.getVersion().onOrAfter(Version.V_7_9_0)) {
            out.writeOptionalString(description);
        }
    }

    public IngestDocument getIngestDocument() {
        if (ingestDocument == null) {
            return null;
        }
        return ingestDocument.getIngestDocument();
    }

    public String getProcessorTag() {
        return processorTag;
    }

    public Exception getFailure() {
        return failure;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (processorTag == null && failure == null && ingestDocument == null) {
            builder.nullValue();
            return builder;
        }

        builder.startObject();

        if (processorTag != null) {
            builder.field(ConfigurationUtils.TAG_KEY, processorTag);
        }

        if (description != null) {
            builder.field(ConfigurationUtils.DESCRIPTION_KEY, description);
        }

        if (failure != null && ingestDocument != null) {
            builder.startObject(IGNORED_ERROR_FIELD);
            ElasticsearchException.generateFailureXContent(builder, params, failure, true);
            builder.endObject();
        } else if (failure != null) {
            ElasticsearchException.generateFailureXContent(builder, params, failure, true);
        }

        if (ingestDocument != null) {
            ingestDocument.toXContent(builder, params);
        }

        builder.endObject();
        return builder;
    }

    public static SimulateProcessorResult fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
