/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.ingest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Locale;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class SimulateProcessorResult implements Writeable, ToXContentObject {

    private static final String IGNORED_ERROR_FIELD = "ignored_error";
    private static final String STATUS_FIELD = "status";
    private static final String TYPE_FIELD = "processor_type";
    private static final String CONDITION_FIELD = "condition";
    private static final String RESULT_FIELD = "result";

    enum Status {
        SUCCESS,
        ERROR,
        ERROR_IGNORED,
        SKIPPED,
        DROPPED;

        @Override
        public String toString() {
            return this.name().toLowerCase(Locale.ROOT);
        }

        public static Status fromString(String string) {
            return Status.valueOf(string.toUpperCase(Locale.ROOT));
        }
    }

    private final String type;
    private final String processorTag;
    private final String description;
    private final WriteableIngestDocument ingestDocument;
    private final Exception failure;
    private final Tuple<String, Boolean> conditionalWithResult;

    private static final ConstructingObjectParser<ElasticsearchException, Void> IGNORED_ERROR_PARSER = new ConstructingObjectParser<>(
        "ignored_error_parser",
        true,
        a -> (ElasticsearchException) a[0]
    );
    static {
        IGNORED_ERROR_PARSER.declareObject(constructorArg(), (p, c) -> ElasticsearchException.fromXContent(p), new ParseField("error"));
    }

    private static final ConstructingObjectParser<Tuple<String, Boolean>, Void> IF_CONDITION_PARSER = new ConstructingObjectParser<>(
        "if_condition_parser",
        true,
        a -> {
            String condition = a[0] == null ? null : (String) a[0];
            Boolean result = a[1] == null ? null : (Boolean) a[1];
            return new Tuple<>(condition, result);
        }
    );
    static {
        IF_CONDITION_PARSER.declareString(optionalConstructorArg(), new ParseField(CONDITION_FIELD));
        IF_CONDITION_PARSER.declareBoolean(optionalConstructorArg(), new ParseField(RESULT_FIELD));
    }

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<SimulateProcessorResult, Void> PARSER = new ConstructingObjectParser<>(
        "simulate_processor_result",
        true,
        a -> {
            String type = (String) a[0];
            String processorTag = a[1] == null ? null : (String) a[1];
            String description = a[2] == null ? null : (String) a[2];
            Tuple<String, Boolean> conditionalWithResult = a[3] == null ? null : (Tuple<String, Boolean>) a[3];
            IngestDocument document = a[4] == null ? null : ((WriteableIngestDocument) a[4]).getIngestDocument();
            Exception failure = null;
            if (a[5] != null) {
                failure = (ElasticsearchException) a[5];
            } else if (a[6] != null) {
                failure = (ElasticsearchException) a[6];
            }

            return new SimulateProcessorResult(type, processorTag, description, document, failure, conditionalWithResult);
        }
    );
    static {
        PARSER.declareString(optionalConstructorArg(), new ParseField(TYPE_FIELD));
        PARSER.declareString(optionalConstructorArg(), new ParseField(ConfigurationUtils.TAG_KEY));
        PARSER.declareString(optionalConstructorArg(), new ParseField(ConfigurationUtils.DESCRIPTION_KEY));
        PARSER.declareObject(optionalConstructorArg(), IF_CONDITION_PARSER, new ParseField("if"));
        PARSER.declareObject(
            optionalConstructorArg(),
            WriteableIngestDocument.INGEST_DOC_PARSER,
            new ParseField(WriteableIngestDocument.DOC_FIELD)
        );
        PARSER.declareObject(optionalConstructorArg(), IGNORED_ERROR_PARSER, new ParseField(IGNORED_ERROR_FIELD));
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> ElasticsearchException.fromXContent(p), new ParseField("error"));
    }

    public SimulateProcessorResult(
        String type,
        String processorTag,
        String description,
        IngestDocument ingestDocument,
        Exception failure,
        Tuple<String, Boolean> conditionalWithResult
    ) {
        this.processorTag = processorTag;
        this.description = description;
        this.ingestDocument = (ingestDocument == null) ? null : new WriteableIngestDocument(ingestDocument);
        this.failure = failure;
        this.conditionalWithResult = conditionalWithResult;
        this.type = type;
    }

    public SimulateProcessorResult(
        String type,
        String processorTag,
        String description,
        IngestDocument ingestDocument,
        Tuple<String, Boolean> conditionalWithResult
    ) {
        this(type, processorTag, description, ingestDocument, null, conditionalWithResult);
    }

    public SimulateProcessorResult(
        String type,
        String processorTag,
        String description,
        Exception failure,
        Tuple<String, Boolean> conditionalWithResult
    ) {
        this(type, processorTag, description, null, failure, conditionalWithResult);
    }

    public SimulateProcessorResult(String type, String processorTag, String description, Tuple<String, Boolean> conditionalWithResult) {
        this(type, processorTag, description, null, null, conditionalWithResult);
    }

    /**
     * Read from a stream.
     */
    SimulateProcessorResult(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
            this.processorTag = in.readOptionalString();
        } else {
            this.processorTag = in.readString();
        }
        this.ingestDocument = in.readOptionalWriteable(WriteableIngestDocument::new);
        this.failure = in.readException();
        this.description = in.readOptionalString();
        this.type = in.readString();
        boolean hasConditional = in.readBoolean();
        if (hasConditional) {
            this.conditionalWithResult = new Tuple<>(in.readString(), in.readBoolean());
        } else {
            this.conditionalWithResult = null; // no condition exists
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
            out.writeOptionalString(processorTag);
        } else {
            out.writeString(processorTag);
        }
        out.writeOptionalWriteable(ingestDocument);
        out.writeException(failure);
        out.writeOptionalString(description);
        out.writeString(type);
        out.writeBoolean(conditionalWithResult != null);
        if (conditionalWithResult != null) {
            out.writeString(conditionalWithResult.v1());
            out.writeBoolean(conditionalWithResult.v2());
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

    public Tuple<String, Boolean> getConditionalWithResult() {
        return conditionalWithResult;
    }

    public String getType() {
        return type;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (type != null) {
            builder.field(TYPE_FIELD, type);
        }

        builder.field(STATUS_FIELD, getStatus(type));

        if (description != null) {
            builder.field(ConfigurationUtils.DESCRIPTION_KEY, description);
        }

        if (processorTag != null) {
            builder.field(ConfigurationUtils.TAG_KEY, processorTag);
        }

        if (conditionalWithResult != null) {
            builder.startObject("if");
            builder.field(CONDITION_FIELD, conditionalWithResult.v1());
            builder.field(RESULT_FIELD, conditionalWithResult.v2());
            builder.endObject();
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

    Status getStatus(String type) {
        // if no condition, or condition passed
        if (conditionalWithResult == null || (conditionalWithResult != null && conditionalWithResult.v2())) {
            if (failure != null) {
                if (ingestDocument == null) {
                    return Status.ERROR;
                } else {
                    return Status.ERROR_IGNORED;
                }
            } else if (ingestDocument == null && "pipeline".equals(type) == false) {
                return Status.DROPPED;
            }
            return Status.SUCCESS;
        } else { // has condition that failed the check
            return Status.SKIPPED;
        }
    }

    @Override
    public String toString() {
        return "SimulateProcessorResult{"
            + "type='"
            + type
            + '\''
            + ", processorTag='"
            + processorTag
            + '\''
            + ", description='"
            + description
            + '\''
            + ", ingestDocument="
            + ingestDocument
            + ", failure="
            + failure
            + ", conditionalWithResult="
            + conditionalWithResult
            + '}';
    }
}
