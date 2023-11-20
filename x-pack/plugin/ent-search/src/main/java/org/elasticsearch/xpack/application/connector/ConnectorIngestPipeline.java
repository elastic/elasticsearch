/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class ConnectorIngestPipeline implements Writeable, ToXContentObject {

    private final Boolean extractBinaryContent;
    private final String name;
    private final Boolean reduceWhitespace;
    private final Boolean runMlInference;

    private ConnectorIngestPipeline(Boolean extractBinaryContent, String name, Boolean reduceWhitespace, Boolean runMlInference) {
        this.extractBinaryContent = extractBinaryContent;
        this.name = name;
        this.reduceWhitespace = reduceWhitespace;
        this.runMlInference = runMlInference;
    }

    public ConnectorIngestPipeline(StreamInput in) throws IOException {
        this.extractBinaryContent = in.readBoolean();
        this.name = in.readString();
        this.reduceWhitespace = in.readBoolean();
        this.runMlInference = in.readBoolean();
    }

    public static final ParseField EXTRACT_BINARY_CONTENT_FIELD = new ParseField("extract_binary_content");
    public static final ParseField NAME_FIELD = new ParseField("name");
    public static final ParseField REDUCE_WHITESPACE_FIELD = new ParseField("reduce_whitespace");
    public static final ParseField RUN_ML_INFERENCE_FIELD = new ParseField("run_ml_inference");

    private static final ConstructingObjectParser<ConnectorIngestPipeline, Void> PARSER = new ConstructingObjectParser<>(
        "connector_ingest_pipeline",
        true,
        args -> new Builder().setExtractBinaryContent((Boolean) args[0])
            .setName((String) args[1])
            .setReduceWhitespace((Boolean) args[2])
            .setRunMlInference((Boolean) args[3])
            .build()
    );

    static {
        PARSER.declareBoolean(constructorArg(), EXTRACT_BINARY_CONTENT_FIELD);
        PARSER.declareString(constructorArg(), NAME_FIELD);
        PARSER.declareBoolean(constructorArg(), REDUCE_WHITESPACE_FIELD);
        PARSER.declareBoolean(constructorArg(), RUN_ML_INFERENCE_FIELD);
    }

    public static ConnectorIngestPipeline fromXContentBytes(BytesReference source, XContentType xContentType) {
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, xContentType)) {
            return ConnectorIngestPipeline.fromXContent(parser);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse: " + source.utf8ToString(), e);
        }
    }

    public static ConnectorIngestPipeline fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.field(EXTRACT_BINARY_CONTENT_FIELD.getPreferredName(), extractBinaryContent);
                if (name != null) {
                    builder.field(NAME_FIELD.getPreferredName(), name);
                }
                builder.field(REDUCE_WHITESPACE_FIELD.getPreferredName(), reduceWhitespace);
                builder.field(RUN_ML_INFERENCE_FIELD.getPreferredName(), runMlInference);
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(extractBinaryContent);
        out.writeString(name);
        out.writeBoolean(reduceWhitespace);
        out.writeBoolean(runMlInference);
    }

    public static class Builder {

        private Boolean extractBinaryContent;
        private String name;
        private Boolean reduceWhitespace;
        private Boolean runMlInference;

        public Builder setExtractBinaryContent(Boolean extractBinaryContent) {
            this.extractBinaryContent = extractBinaryContent;
            return this;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setReduceWhitespace(Boolean reduceWhitespace) {
            this.reduceWhitespace = reduceWhitespace;
            return this;
        }

        public Builder setRunMlInference(Boolean runMlInference) {
            this.runMlInference = runMlInference;
            return this;
        }

        public ConnectorIngestPipeline build() {
            return new ConnectorIngestPipeline(extractBinaryContent, name, reduceWhitespace, runMlInference);
        }
    }

}
