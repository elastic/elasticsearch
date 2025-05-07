/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
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

/**
 * Holds the data stream failure store metadata that enable or disable the failure store of a data stream. Currently, it
 * supports the following configurations only explicitly enabling or disabling the failure store
 */
public record DataStreamFailureStore(@Nullable Boolean enabled, @Nullable DataStreamLifecycle lifecycle)
    implements
        SimpleDiffable<DataStreamFailureStore>,
        ToXContentObject {

    public static final String FAILURE_STORE = "failure_store";
    public static final String ENABLED = "enabled";
    public static final String LIFECYCLE = "lifecycle";
    private static final String EMPTY_FAILURE_STORE_ERROR_MESSAGE =
        "Failure store configuration should have at least one non-null configuration value.";

    public static final ParseField ENABLED_FIELD = new ParseField(ENABLED);
    public static final ParseField LIFECYCLE_FIELD = new ParseField(LIFECYCLE);

    public static final ConstructingObjectParser<DataStreamFailureStore, Void> PARSER = new ConstructingObjectParser<>(
        FAILURE_STORE,
        false,
        (args, unused) -> new DataStreamFailureStore((Boolean) args[0], (DataStreamLifecycle) args[1])
    );

    static {
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ENABLED_FIELD);
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, unused) -> DataStreamLifecycle.failureLifecycleFromXContent(p),
            LIFECYCLE_FIELD
        );
    }

    /**
     *  @param enabled, true when the failure is enabled, false when it's disabled, null when it depends on other configuration. Currently,
     *                  null value is not supported because there are no other arguments
     * @throws IllegalArgumentException when all the constructor arguments are null
     */
    public DataStreamFailureStore {
        if (enabled == null && lifecycle == null) {
            throw new IllegalArgumentException(EMPTY_FAILURE_STORE_ERROR_MESSAGE);
        }
        assert lifecycle == null || lifecycle.targetsFailureStore() : "Invalid type for failures lifecycle";
    }

    public DataStreamFailureStore(StreamInput in) throws IOException {
        this(
            in.readOptionalBoolean(),
            in.getTransportVersion().onOrAfter(TransportVersions.INTRODUCE_FAILURES_LIFECYCLE_BACKPORT_8_19)
                ? in.readOptionalWriteable(DataStreamLifecycle::new)
                : null
        );
    }

    public static Diff<DataStreamFailureStore> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(DataStreamFailureStore::new, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalBoolean(enabled);
        if (out.getTransportVersion().onOrAfter(TransportVersions.INTRODUCE_FAILURES_LIFECYCLE_BACKPORT_8_19)) {
            out.writeOptionalWriteable(lifecycle);
        }
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
        if (lifecycle != null) {
            builder.field(LIFECYCLE_FIELD.getPreferredName(), lifecycle);
        }
        builder.endObject();
        return builder;
    }

    public static DataStreamFailureStore fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * This class is only used in template configuration. It wraps the fields of {@link DataStreamFailureStore} with {@link ResettableValue}
     * to allow a user to signal when they want to reset any previously encountered values during template composition.
     */
    public record Template(ResettableValue<Boolean> enabled, ResettableValue<DataStreamLifecycle.Template> lifecycle)
        implements
            Writeable,
            ToXContentObject {

        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<Template, Void> PARSER = new ConstructingObjectParser<>(
            "failure_store_template",
            false,
            (args, unused) -> new Template(
                args[0] == null ? ResettableValue.undefined() : (ResettableValue<Boolean>) args[0],
                args[1] == null ? ResettableValue.undefined() : (ResettableValue<DataStreamLifecycle.Template>) args[1]
            )
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
            PARSER.declareField(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> p.currentToken() == XContentParser.Token.VALUE_NULL
                    ? ResettableValue.reset()
                    : ResettableValue.create(DataStreamLifecycle.Template.failuresLifecycleTemplateFromXContent(p)),
                LIFECYCLE_FIELD,
                ObjectParser.ValueType.OBJECT_OR_NULL
            );
        }

        public Template(@Nullable Boolean enabled, @Nullable DataStreamLifecycle.Template lifecycle) {
            this(ResettableValue.create(enabled), ResettableValue.create(lifecycle));
        }

        public Template {
            if (enabled.isDefined() == false && lifecycle.isDefined() == false) {
                throw new IllegalArgumentException(EMPTY_FAILURE_STORE_ERROR_MESSAGE);
            }
            assert lifecycle.get() == null || lifecycle.mapAndGet(l -> l.toDataStreamLifecycle().targetsFailureStore())
                : "Invalid lifecycle type in failure store template";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            ResettableValue.write(out, enabled, StreamOutput::writeBoolean);
            if (out.getTransportVersion().onOrAfter(TransportVersions.INTRODUCE_FAILURES_LIFECYCLE_BACKPORT_8_19)) {
                ResettableValue.write(out, lifecycle, (o, v) -> v.writeTo(o));
            }
        }

        public static Template read(StreamInput in) throws IOException {
            ResettableValue<Boolean> enabled = ResettableValue.read(in, StreamInput::readBoolean);
            ResettableValue<DataStreamLifecycle.Template> lifecycle = ResettableValue.undefined();
            if (in.getTransportVersion().onOrAfter(TransportVersions.INTRODUCE_FAILURES_LIFECYCLE_BACKPORT_8_19)) {
                lifecycle = ResettableValue.read(in, DataStreamLifecycle.Template::read);
            }
            return new Template(enabled, lifecycle);
        }

        /**
         * Converts the template to XContent, depending on the XContent.Params set by {@link ResettableValue#hideResetValues(Params)}
         * it may or may not display any explicit nulls when the value is to be reset.
         */
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            enabled.toXContent(builder, params, ENABLED_FIELD.getPreferredName());
            lifecycle.toXContent(builder, params, LIFECYCLE_FIELD.getPreferredName());
            builder.endObject();
            return builder;
        }

        public static Template fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        @Override
        public String toString() {
            return Strings.toString(this, true, true);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(Template template) {
        return new Builder(template);
    }

    public static Builder builder(DataStreamFailureStore failureStore) {
        return new Builder(failureStore);
    }

    /**
     * Builder that is able to create either a DataStreamFailureStore or its respective Template.
     * Furthermore, its composeTemplate method during template composition.
     */
    public static class Builder {
        private Boolean enabled = null;
        private DataStreamLifecycle.Builder lifecycleBuilder = null;

        private Builder() {}

        private Builder(Template template) {
            if (template != null) {
                enabled = template.enabled.get();
                lifecycleBuilder = template.lifecycle.mapAndGet(l -> DataStreamLifecycle.builder(l));
            }
        }

        private Builder(DataStreamFailureStore failureStore) {
            if (failureStore != null) {
                enabled = failureStore.enabled;
                lifecycleBuilder = failureStore.lifecycle == null ? null : DataStreamLifecycle.builder(failureStore.lifecycle);
            }
        }

        public Builder enabled(Boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        /**
         * Composes the provided enabled value with the current one. Because enabled is a resettable boolean, if it is defined
         * it will overwrite the current value.
         */
        public Builder enabled(ResettableValue<Boolean> enabled) {
            if (enabled.isDefined()) {
                this.enabled = enabled.get();
            }
            return this;
        }

        public Builder lifecycle(DataStreamLifecycle lifecycle) {
            this.lifecycleBuilder = lifecycle == null ? null : DataStreamLifecycle.builder(lifecycle);
            return this;
        }

        /**
         * Composes the provided lifecycle value with the current one. Because lifecycle is a resettable template that can be merged,
         * if it is defined it will delegate to {@link DataStreamLifecycle.Builder#composeTemplate(DataStreamLifecycle.Template)} to
         * correctly compose the contents.
         */
        public Builder lifecycle(ResettableValue<DataStreamLifecycle.Template> lifecycle) {
            if (lifecycle.shouldReset()) {
                this.lifecycleBuilder = null;
            } else if (lifecycle.isDefined()) {
                if (this.lifecycleBuilder == null) {
                    this.lifecycleBuilder = DataStreamLifecycle.builder(lifecycle.get());
                } else {
                    this.lifecycleBuilder.composeTemplate(lifecycle.get());
                }
            }
            return this;

        }

        /**
         * Composes the provided failure store template with this builder.
         */
        public Builder composeTemplate(DataStreamFailureStore.Template failureStore) {
            this.enabled(failureStore.enabled());
            this.lifecycle(failureStore.lifecycle());
            return this;
        }

        /**
         * Builds a valid DataStreamFailureStore configuration.
         * @return the object or null if all the values were null.
         */
        @Nullable
        public DataStreamFailureStore build() {
            if (enabled == null && lifecycleBuilder == null) {
                return null;
            }
            return new DataStreamFailureStore(enabled, lifecycleBuilder == null ? null : lifecycleBuilder.build());
        }

        /**
         * Builds a valid template for the DataStreamFailureStore configuration.
         * @return the template or null if all the values were null.
         */
        @Nullable
        public DataStreamFailureStore.Template buildTemplate() {
            if (enabled == null && lifecycleBuilder == null) {
                return null;
            }
            return new Template(enabled, lifecycleBuilder == null ? null : lifecycleBuilder.buildTemplate());
        }
    }
}
