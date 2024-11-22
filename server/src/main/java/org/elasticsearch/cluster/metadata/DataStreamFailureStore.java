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
import org.elasticsearch.cluster.metadata.Template.ExplicitlyNullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

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
     * This class is only used in template configuration. It allows us to represent explicit null values and denotes that the fields
     * of the failure can be merged.
     */
    public static class Template implements Writeable, ToXContentObject {

        private final ExplicitlyNullable<Boolean> enabled;

        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<DataStreamFailureStore.Template, Void> PARSER = new ConstructingObjectParser<>(
            "failure_store_template",
            false,
            (args, unused) -> new DataStreamFailureStore.Template((ExplicitlyNullable<Boolean>) args[0])
        );

        static {
            PARSER.declareField(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> p.currentToken() == XContentParser.Token.VALUE_NULL
                    ? ExplicitlyNullable.empty()
                    : ExplicitlyNullable.create(p.booleanValue()),
                ENABLED_FIELD,
                ObjectParser.ValueType.BOOLEAN_OR_NULL
            );
        }

        public Template(ExplicitlyNullable<Boolean> enabled) {
            if (enabled == null || enabled.isNull()) {
                throw new IllegalArgumentException("Failure store configuration should have at least one non-null configuration value.");
            }
            this.enabled = enabled;
        }

        public ExplicitlyNullable<Boolean> enabled() {
            return enabled;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            org.elasticsearch.cluster.metadata.Template.ExplicitlyNullable.write(out, enabled, StreamOutput::writeBoolean);
        }

        public static Template read(StreamInput in) throws IOException {
            ExplicitlyNullable<Boolean> enabled = org.elasticsearch.cluster.metadata.Template.ExplicitlyNullable.read(
                in,
                StreamInput::readBoolean
            );
            return new Template(enabled);
        }

        /**
         * Converts the template to XContent, when the parameter {@link ExplicitlyNullable#SKIP_EXPLICIT_NULLS} is set to true, it will not
         * display any explicit nulls
         */
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            boolean skipExplicitNulls = params.paramAsBoolean(
                org.elasticsearch.cluster.metadata.Template.ExplicitlyNullable.SKIP_EXPLICIT_NULLS,
                false
            );
            builder.startObject();
            if (enabled != null) {
                if (enabled.isNull() == false) {
                    builder.field(ENABLED_FIELD.getPreferredName(), enabled.get());
                } else if (skipExplicitNulls == false) {
                    builder.nullField(ENABLED_FIELD.getPreferredName());
                }
            }
            builder.endObject();
            return builder;
        }

        public static DataStreamFailureStore.Template fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        /**
         * Returns a template that has the initial values overridden by the new template
         * @param template the new template to merge with
         */
        public Template mergeWith(Template template) {
            return template;
        }

        @Nullable
        public DataStreamFailureStore toFailureStore() {
            return new DataStreamFailureStore(enabled == null ? null : enabled.get());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Template template = (Template) o;
            return Objects.equals(enabled, template.enabled);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(enabled);
        }

        @Override
        public String toString() {
            return Strings.toString(this, true, true);
        }
    }
}
