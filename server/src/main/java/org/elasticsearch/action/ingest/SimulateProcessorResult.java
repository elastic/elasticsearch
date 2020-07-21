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
import org.elasticsearch.common.collect.Tuple;
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
import java.util.Locale;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class SimulateProcessorResult implements Writeable, ToXContentObject {

    private static final String IGNORED_ERROR_FIELD = "ignored_error";
    private static final String STATUS_FIELD = "status";
    private static final String TYPE_FIELD = "processor_type";

    private enum Status {
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
                String type = (String) a[0];
                String processorTag = a[1] == null ? null : (String)a[1];
                String description = a[2] == null ? null : (String)a[2];
                IngestDocument document = a[3] == null ? null : ((WriteableIngestDocument)a[3]).getIngestDocument();
                Exception failure = null;
                if (a[4] != null) {
                    failure = (ElasticsearchException)a[4];
                } else if (a[5] != null) {
                    failure = (ElasticsearchException)a[5];
                }
                //TODO:
                Tuple<String, Boolean> conditionalWithResult = null;
                //Status status = Status.fromString((String) a[6]);
                //FIXME

                return new SimulateProcessorResult(type, processorTag, description, document, failure, conditionalWithResult);
            }
        );
    static {
        PARSER.declareString(optionalConstructorArg(), new ParseField(TYPE_FIELD));
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
        PARSER.declareString(optionalConstructorArg(), new ParseField(STATUS_FIELD)); //fixme ... don't acutually need the status field, only the if condition object
    }

    public SimulateProcessorResult(String type, String processorTag, String description, IngestDocument ingestDocument,
                                   Exception failure, Tuple<String, Boolean> conditionalWithResult) {
        this.processorTag = processorTag;
        this.description = description;
        this.ingestDocument = (ingestDocument == null) ? null : new WriteableIngestDocument(ingestDocument);
        this.failure = failure;
        this.conditionalWithResult = conditionalWithResult;
        this.type = type;
    }

    public SimulateProcessorResult(String type, String processorTag, String description, IngestDocument ingestDocument,
                                   Tuple<String, Boolean> conditionalWithResult) {
        this(type, processorTag, description, ingestDocument, null, conditionalWithResult);
    }

    public SimulateProcessorResult(String type, String processorTag, String description, Exception failure,
                                   Tuple<String, Boolean> conditionalWithResult ) {
        this(type, processorTag, description, null, failure, conditionalWithResult);
    }

    public SimulateProcessorResult(String type, String processorTag, String description, Tuple<String, Boolean> conditionalWithResult) {
        this(type, processorTag, description, null, null, conditionalWithResult);
    }

    /**
     * Read from a stream.
     */
    SimulateProcessorResult(StreamInput in) throws IOException {
        this.processorTag = in.readString();
        this.ingestDocument = in.readOptionalWriteable(WriteableIngestDocument::new);
        this.failure = in.readException();
        if (in.getVersion().onOrAfter(Version.V_7_9_0)) {
            this.type = in.readString();
            this.description = in.readOptionalString();
            boolean hasConditional = in.readBoolean();
            if (hasConditional) {
                this.conditionalWithResult = new Tuple<>(in.readString(), in.readBoolean());
            } else{
                this.conditionalWithResult = null; //no condition exists
            }
        } else {
            this.description = null;
            this.conditionalWithResult = null;
            this.type = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(processorTag);
        out.writeOptionalWriteable(ingestDocument);
        out.writeException(failure);
        if (out.getVersion().onOrAfter(Version.V_7_9_0)) {
            out.writeString(type);
            out.writeOptionalString(description);
            out.writeBoolean(conditionalWithResult != null);
            if (conditionalWithResult != null) {
                out.writeString(conditionalWithResult.v1());
                out.writeBoolean(conditionalWithResult.v2());
            }
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
        builder.startObject();


        if(type != null){
            builder.field(TYPE_FIELD, type);
        }

        builder.field(STATUS_FIELD, getStatus());

        if (description != null) {
            builder.field(ConfigurationUtils.DESCRIPTION_KEY, description);
        }

        if (processorTag != null) {
            builder.field(ConfigurationUtils.TAG_KEY, processorTag);
        }

        if(conditionalWithResult != null){
            builder.startObject("if");
            builder.field("condition", conditionalWithResult.v1());
            builder.field("result", conditionalWithResult.v2());
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

    //TODO: clean this up and this
    private Status getStatus() {
        if (conditionalWithResult != null) {
            if (conditionalWithResult.v2() == false) {
                return Status.SKIPPED;
            }
            return getStatusTrueCondition();
        }
        return getStatusTrueCondition();
    }
    private Status getStatusTrueCondition(){
        if (failure != null) {
            if (ingestDocument == null) {
                return Status.ERROR;
            } else {
                return Status.ERROR_IGNORED;
            }
        } else if (ingestDocument == null) {
            return Status.DROPPED;
        }
        return Status.SUCCESS;
    }
}
