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
 * A metadata class to hold error information about errors encountered
 * while applying a cluster state update for a given namespace.
 * <p>
 * This information is held by the {@link OperatorMetadata} class.
 */
public record OperatorErrorMetadata(
    Long version,
    org.elasticsearch.cluster.metadata.OperatorErrorMetadata.ErrorKind errorKind,
    List<String> errors
) implements SimpleDiffable<OperatorErrorMetadata>, ToXContentFragment {
    /**
     * Contructs an operator error metadata
     *
     * @param version   the metadata version of the content which failed to apply
     * @param errorKind the kind of error we encountered while processing
     * @param errors    the list of errors encountered during parsing and validation of the operator content
     */
    public OperatorErrorMetadata {}

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(version);
        out.writeString(errorKind.getKindValue());
        out.writeCollection(errors, StreamOutput::writeString);
    }

    /**
     * Reads an {@link OperatorErrorMetadata} from a {@link StreamInput}
     * @param in the {@link StreamInput} to read from
     * @return {@link OperatorErrorMetadata}
     * @throws IOException
     */
    public static OperatorErrorMetadata readFrom(StreamInput in) throws IOException {
        Builder builder = new Builder().version(in.readLong())
            .errorKind(ErrorKind.of(in.readString()))
            .errors(in.readList(StreamInput::readString));
        return builder.build();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Builder.toXContent(this, builder);
        return builder;
    }

    /**
     * Reads an {@link OperatorErrorMetadata} {@link Diff} from {@link StreamInput}
     * @param in the {@link StreamInput} to read the diff from
     * @return a {@link Diff} of {@link OperatorErrorMetadata}
     * @throws IOException
     */
    public static Diff<OperatorErrorMetadata> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(OperatorErrorMetadata::readFrom, in);
    }

    /**
     * Helper method to construct an {@link OperatorErrorMetadata} {@link Builder}
     * @return {@link Builder}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder class for {@link OperatorErrorMetadata}
     */
    public static class Builder {
        private static final String ERRORS = "errors";
        private static final String VERSION = "version";
        private static final String ERROR_KIND = "error_kind";

        private Long version;
        private List<String> errors;
        private ErrorKind errorKind;

        /**
         * Default constructor
         */
        public Builder() {
            this.version = 0L;
            this.errors = new ArrayList<>();
        }

        /**
         * Set the error metadata version
         * @param version the metadata version of the operator content that failed to apply
         * @return {@link Builder}
         */
        public Builder version(Long version) {
            this.version = version;
            return this;
        }

        /**
         * Set the list of errors we encountered while processing an operator state content
         * @param errors a list of all errors
         * @return {@link Builder}
         */
        public Builder errors(List<String> errors) {
            this.errors = errors;
            return this;
        }

        /**
         * Set the validation error kind
         * @param errorKind one of {@link ErrorKind}
         * @return {@link Builder}
         */
        public Builder errorKind(ErrorKind errorKind) {
            this.errorKind = errorKind;
            return this;
        }

        /**
         * Builds the {@link OperatorErrorMetadata}
         * @return {@link OperatorErrorMetadata}
         */
        public OperatorErrorMetadata build() {
            return new OperatorErrorMetadata(version, errorKind, Collections.unmodifiableList(errors));
        }

        /**
         * Serializes an {@link OperatorErrorMetadata} to xContent
         *
         * @param metadata {@link OperatorErrorMetadata}
         * @param builder {@link XContentBuilder}
         */
        public static void toXContent(OperatorErrorMetadata metadata, XContentBuilder builder) throws IOException {
            builder.startObject();
            builder.field(VERSION, metadata.version);
            builder.field(ERROR_KIND, metadata.errorKind.getKindValue());
            builder.stringListField(ERRORS, metadata.errors);
            builder.endObject();
        }

        /**
         * Reads an {@link OperatorErrorMetadata} from xContent
         *
         * @param parser {@link XContentParser}
         * @return {@link OperatorErrorMetadata}
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

        /**
         * Returns the String value for this enum value
         * @return the String value for the enum
         */
        public String getKindValue() {
            return kind;
        }

        /**
         * Helper method to construct {@link ErrorKind} from a String. The JDK default implementation
         * throws incomprehensible error.
         * @param kind String value
         * @return {@link ErrorKind}
         */
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
