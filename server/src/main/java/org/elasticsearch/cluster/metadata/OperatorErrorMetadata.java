/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Metadata class to hold the operator set keys for each operator handler
 *
 */
public class OperatorErrorMetadata implements SimpleDiffable<OperatorErrorMetadata>, ToXContentFragment {
    private final Long version;
    private final List<String> errors;

    public OperatorErrorMetadata(Long version, List<String> errors) {
        this.version = version;
        this.errors = errors;
    }

    public Long version() {
        return this.version;
    }

    public List<String> errors() {
        return this.errors;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(version);
        out.writeCollection(errors, StreamOutput::writeString);
    }

    public static OperatorErrorMetadata readFrom(StreamInput in) throws IOException {
        Builder builder = new Builder().version(in.readLong()).errors(in.readList(StreamInput::readString));
        return builder.build();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Builder.toXContent(this, builder, params);
        return builder;
    }

    public static Diff<OperatorErrorMetadata> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(OperatorErrorMetadata::readFrom, in);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private static final String ERRORS = "errors";
        private static final String VERSION = "version";

        private Long version;
        private List<String> errors;

        public Builder() {
            this.version = 0L;
            this.errors = new ArrayList<>();
        }

        public Builder version(Long version) {
            this.version = version;
            return this;
        }

        public Builder errors(List<String> errors) {
            this.errors = errors;
            return this;
        }

        public OperatorErrorMetadata build() {
            return new OperatorErrorMetadata(version, Collections.unmodifiableList(errors));
        }

        /**
         * Serializes the metadata to xContent
         *
         * @param metadata
         * @param builder
         * @param params
         */
        public static void toXContent(OperatorErrorMetadata metadata, XContentBuilder builder, Params params)
            throws IOException {
            builder.startObject();
            builder.field(VERSION, metadata.version);
            builder.stringListField(ERRORS, metadata.errors);
            builder.endObject();
        }

        /**
         * Reads the metadata from xContent
         *
         * @param parser
         * @return
         * @throws IOException
         */
        public static OperatorErrorMetadata fromXContent(XContentParser parser) throws IOException {
            Builder builder = new Builder();

            String currentFieldName = parser.currentName();
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if (ERRORS.equals(currentFieldName)) {
                        List<String> errors = new ArrayList<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            errors.add(parser.text());
                        }
                        builder.errors(errors);
                    }
                } else if (token.isValue()) {
                    if (VERSION.equals(currentFieldName)) {
                        builder.version(parser.longValue());
                    }
                }
            }
            return builder.build();
        }
    }
}
