/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * Holds the data stream failure store metadata that enable or disable the failure store of a data stream. Currently, it
 * supports the following configurations only explicitly enabling or disabling the failure store
 */
public record DataStreamFailureStore(Boolean enabled) implements SimpleDiffable<DataStreamFailureStore>, ToXContentObject {
    public static final String FAILURE_STORE = "failure_store";
    public static final String ENABLED = "enabled";

    public static final ParseField ENABLED_FIELD = new ParseField(ENABLED);

    public static final ConstructingObjectParser<DataStreamFailureStore, Void> PARSER = new ConstructingObjectParser<>(
        FAILURE_STORE,
        false,
        (args, unused) -> new DataStreamFailureStore((Boolean) args[0])
    );

    static {
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ENABLED_FIELD);
    }

    /**
     *  @param enabled, true when the failure is enabled, false when it's disabled, null when it depends on other configuration. Currently,
     *                  null value is not supported because there are no other arguments
     * @throws IllegalArgumentException when all the constructor arguments are null
     */
    public DataStreamFailureStore {
        if (enabled == null) {
            throw new IllegalArgumentException("Failure store configuration should have at least one non-null configuration value.");
        }
    }

    public DataStreamFailureStore(StreamInput in) throws IOException {
        this(in.readOptionalBoolean());
    }

    public static Diff<DataStreamFailureStore> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(DataStreamFailureStore::new, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalBoolean(enabled);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (enabled != null) {
            builder.field(ENABLED_FIELD.getPreferredName(), enabled);
        }
        builder.endObject();
        return builder;
    }

    public static DataStreamFailureStore fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * This class is only used in template configuration. It wraps the fields of {@link DataStreamFailureStore} with {@link ResettableValue}
     * to allow a user to signal when they want to reset any previously encountered values during template composition. Furthermore, it
     * provides the method {@link #merge(Template, Template)} that dictates how two templates can be composed.
     */
    public record Template(ResettableValue<Boolean> enabled) implements Writeable, ToXContentObject {

        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<Template, Void> PARSER = new ConstructingObjectParser<>(
            "failure_store_template",
            false,
            (args, unused) -> new Template(args[0] == null ? ResettableValue.undefined() : (ResettableValue<Boolean>) args[0])
        );

        static {
            PARSER.declareField(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> p.currentToken() == XContentParser.Token.VALUE_NULL
                    ? ResettableValue.reset()
                    : ResettableValue.create(p.booleanValue()),
                ENABLED_FIELD,
                ObjectParser.ValueType.BOOLEAN_OR_NULL
            );
        }

        public Template {
            if (enabled.get() == null) {
                throw new IllegalArgumentException("Failure store configuration should have at least one non-null configuration value.");
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            ResettableValue.write(out, enabled, StreamOutput::writeBoolean);
        }

        public static Template read(StreamInput in) throws IOException {
            ResettableValue<Boolean> enabled = ResettableValue.read(in, StreamInput::readBoolean);
            return new Template(enabled);
        }

        /**
         * Converts the template to XContent, depending on the XContent.Params set by {@link ResettableValue#hideResetValues(Params)}
         * it may or may not display any explicit nulls when the value is to be reset.
         */
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            enabled.toXContent(builder, params, ENABLED_FIELD.getPreferredName());
            builder.endObject();
            return builder;
        }

        public static Template fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        /**
         * Returns a template which has the value of the initial template updated with the values of the update.
         * Note: for now it's a trivial composition because we have only one non-null field.
         * @return the composed template
         */
        public static Template merge(Template ignored, Template update) {
            return update;
        }

        public DataStreamFailureStore toFailureStore() {
            return new DataStreamFailureStore(enabled.get());
        }

        @Override
        public String toString() {
            return Strings.toString(this, true, true);
        }
    }
}
