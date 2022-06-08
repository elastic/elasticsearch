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
 * Metadata class to hold error information about errors encountered
 * while applying a cluster state update for a given namespace.
 * <p>
 * This information is held by the OperatorMetadata class.
 */
public record OperatorErrorMetadata(
    Long version,
    org.elasticsearch.cluster.metadata.OperatorErrorMetadata.ErrorKind errorKind,
    List<String> errors
) implements SimpleDiffable<OperatorErrorMetadata>, ToXContentFragment {
    /**
     * Contructs an operator metadata
     *
     * @param version   the metadata version which failed to apply
     * @param errorKind the kind of error we encountered while processing
     * @param errors    the list of errors encountered during parsing and validation of the metadata
     */
    public OperatorErrorMetadata {}

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(version);
        out.writeString(errorKind.getKindValue());
        out.writeCollection(errors, StreamOutput::writeString);
    }

    public static OperatorErrorMetadata readFrom(StreamInput in) throws IOException {
        Builder builder = new Builder().version(in.readLong())
            .errorKind(ErrorKind.of(in.readString()))
            .errors(in.readList(StreamInput::readString));
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

    /**
     * Builder class for the OperatorErrorMetadata
     */
    public static class Builder {
        private static final String ERRORS = "errors";
        private static final String VERSION = "version";
        private static final String ERROR_KIND = "error_kind";

        private Long version;
        private List<String> errors;
        private ErrorKind errorKind;

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

        public Builder errorKind(ErrorKind errorKind) {
            this.errorKind = errorKind;
            return this;
        }

        public OperatorErrorMetadata build() {
            return new OperatorErrorMetadata(version, errorKind, Collections.unmodifiableList(errors));
        }

        /**
         * Serializes the error metadata to xContent
         *
         * @param metadata
         * @param builder
         * @param params
         */
        public static void toXContent(OperatorErrorMetadata metadata, XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(VERSION, metadata.version);
            builder.field(ERROR_KIND, metadata.errorKind.getKindValue());
            builder.stringListField(ERRORS, metadata.errors);
            builder.endObject();
        }

        /**
         * Reads the error metadata from xContent
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
                    } else if (ERROR_KIND.equals(currentFieldName)) {
                        builder.errorKind(ErrorKind.of(parser.text()));
                    }
                }
            }
            return builder.build();
        }
    }

    /**
     * Enum for kinds of errors we might encounter while processing operator cluster state updates.
     */
    public enum ErrorKind {
        PARSING("parsing"),
        VALIDATION("validation"),
        TRANSIENT("transient");

        private final String kind;

        ErrorKind(String kind) {
            this.kind = kind;
        }

        public String getKindValue() {
            return kind;
        }

        public static ErrorKind of(String kind) {
            for (var report : values()) {
                if (report.kind.equals(kind)) {
                    return report;
                }
            }
            throw new IllegalArgumentException("kind not supported [" + kind + "]");
        }
    }
}
