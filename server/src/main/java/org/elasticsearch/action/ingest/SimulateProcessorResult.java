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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
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
    private final String conditional;
    private final boolean executed;
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
                String conditional = a[1] == null ? null : (String)a[1];
                Boolean executed = a[2] == null ? null : (Boolean) a[2];
                String description = a[3] == null ? null : (String)a[3];
                IngestDocument document = a[4] == null ? null : ((WriteableIngestDocument)a[4]).getIngestDocument();
                Exception failure = null;
                if (a[5] != null) {
                    failure = (ElasticsearchException)a[5];
                } else if (a[6] != null) {
                    failure = (ElasticsearchException)a[6];
                }
                //TODO!!
                return new SimulateProcessorResult(processorTag, description, conditional, executed, document, failure);
            }
        );
    static {
        PARSER.declareString(optionalConstructorArg(), new ParseField(ConfigurationUtils.TAG_KEY));
        PARSER.declareString(optionalConstructorArg(), new ParseField(ConfigurationUtils.CONDITIONAL_KEY));
        PARSER.declareBoolean(optionalConstructorArg(), new ParseField(ConfigurationUtils.EXECUTED_KEY));
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

    public SimulateProcessorResult(String processorTag, String description, String conditional, boolean executed, IngestDocument ingestDocument, Exception failure) {
        this.processorTag = processorTag;
        this.description = description;
        this.ingestDocument = (ingestDocument == null) ? null : new WriteableIngestDocument(ingestDocument);
        this.failure = failure;
        this.conditional = conditional;
        this.executed = executed;
    }

    public SimulateProcessorResult(String processorTag, String description, String conditional, IngestDocument ingestDocument) {
        this(processorTag, description, conditional, true, ingestDocument, null);
    }

    public SimulateProcessorResult(String processorTag, String description, String conditional, Exception failure) {
        this(processorTag, description, conditional, true, null, failure);
    }
    public SimulateProcessorResult(String processorTag, String description, String conditional, boolean executed) {
        this(processorTag, description, conditional, executed, null, null);
    }

    public SimulateProcessorResult(String processorTag, String description, String conditional) {
        this(processorTag, description, conditional, true, null, null);
    }


    /**
     * Read from a stream.
     */
    SimulateProcessorResult(StreamInput in) throws IOException {
        this.processorTag = in.readString();
        this.ingestDocument = in.readOptionalWriteable(WriteableIngestDocument::new);
        this.failure = in.readException();
        //TODO: bwc protect ?
        this.description = in.readString();
        this.conditional = in.readString();
        this.executed = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(processorTag);
        out.writeOptionalWriteable(ingestDocument);
        out.writeException(failure);
        //TODO: bwc protect, optional String ?
        out.writeString(description);
        out.writeString(conditional);
        out.writeBoolean(executed);
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

        if(Strings.isNullOrEmpty(description) == false) {
            builder.field(ConfigurationUtils.DESCRIPTION_KEY, description);
        }

        if(Strings.isNullOrEmpty(conditional) == false) {
            builder.field(ConfigurationUtils.CONDITIONAL_KEY, conditional);
        }

        builder.field(ConfigurationUtils.EXECUTED_KEY, executed);

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
