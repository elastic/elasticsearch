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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A metadata class to hold error information about errors encountered
 * while applying a cluster state update for a given namespace.
 * <p>
 * This information is held by the {@link ReservedStateMetadata} class.
 */
public record ReservedStateErrorMetadata(Long version, ErrorKind errorKind, List<String> errors)
    implements
        SimpleDiffable<ReservedStateErrorMetadata>,
        ToXContentFragment {

    static final ParseField ERRORS = new ParseField("errors");
    static final ParseField VERSION = new ParseField("version");
    static final ParseField ERROR_KIND = new ParseField("error_kind");

    /**
     * Constructs a reserved state error metadata
     *
     * @param version   the metadata version of the content which failed to apply
     * @param errorKind the kind of error we encountered while processing
     * @param errors    the list of errors encountered during parsing and validation of the reserved state content
     */
    public ReservedStateErrorMetadata {}

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(version);
        out.writeString(errorKind.getKindValue());
        out.writeCollection(errors, StreamOutput::writeString);
    }

    /**
     * Reads an {@link ReservedStateErrorMetadata} from a {@link StreamInput}
     *
     * @param in the {@link StreamInput} to read from
     * @return {@link ReservedStateErrorMetadata}
     * @throws IOException
     */
    public static ReservedStateErrorMetadata readFrom(StreamInput in) throws IOException {
        return new ReservedStateErrorMetadata(in.readLong(), ErrorKind.of(in.readString()), in.readList(StreamInput::readString));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(VERSION.getPreferredName(), version);
        builder.field(ERROR_KIND.getPreferredName(), errorKind.getKindValue());
        builder.stringListField(ERRORS.getPreferredName(), errors);
        builder.endObject();
        return builder;
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ReservedStateErrorMetadata, Void> PARSER = new ConstructingObjectParser<>(
        "reserved_state_error_metadata",
        (a) -> new ReservedStateErrorMetadata((Long) a[0], ErrorKind.of((String) a[1]), (List<String>) a[2])
    );

    static {
        PARSER.declareLong(constructorArg(), VERSION);
        PARSER.declareString(constructorArg(), ERROR_KIND);
        PARSER.declareStringArray(constructorArg(), ERRORS);
    }

    /**
     * Reads an {@link ReservedStateErrorMetadata} from xContent
     *
     * @param parser {@link XContentParser}
     * @return {@link ReservedStateErrorMetadata}
     */
    public static ReservedStateErrorMetadata fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    /**
     * Reads an {@link ReservedStateErrorMetadata} {@link Diff} from {@link StreamInput}
     *
     * @param in the {@link StreamInput} to read the diff from
     * @return a {@link Diff} of {@link ReservedStateErrorMetadata}
     * @throws IOException
     */
    public static Diff<ReservedStateErrorMetadata> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(ReservedStateErrorMetadata::readFrom, in);
    }

    /**
     * Enum for kinds of errors we might encounter while processing reserved cluster state updates.
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
         *
         * @return the String value for the enum
         */
        public String getKindValue() {
            return kind;
        }

        /**
         * Helper method to construct {@link ErrorKind} from a String.
         *
         * The JDK default implementation throws incomprehensible error.
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
