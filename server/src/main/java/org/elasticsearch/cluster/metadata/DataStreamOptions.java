/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.cluster.metadata.DataStreamFailureStore.FAILURE_STORE;

/**
 * Holds data stream dedicated configuration options. Currently, it supports the following configurations:
 * - failure store
 * - derived metrics
 */
public record DataStreamOptions(@Nullable DataStreamFailureStore failureStore, @Nullable DataStreamDerivedMetrics derivedMetrics)
    implements
        SimpleDiffable<DataStreamOptions>,
        ToXContentObject {

    public static final ParseField FAILURE_STORE_FIELD = new ParseField(FAILURE_STORE);
    public static final ParseField DERIVED_METRICS_FIELD = new ParseField("derived_metrics");
    public static final DataStreamOptions FAILURE_STORE_ENABLED = new DataStreamOptions(new DataStreamFailureStore(true, null), null);
    public static final DataStreamOptions FAILURE_STORE_DISABLED = new DataStreamOptions(new DataStreamFailureStore(false, null), null);
    public static final DataStreamOptions EMPTY = new DataStreamOptions(null);

    public static final ConstructingObjectParser<DataStreamOptions, Void> PARSER = new ConstructingObjectParser<>(
        "options",
        false,
        (args, unused) -> new DataStreamOptions((DataStreamFailureStore) args[0], (DataStreamDerivedMetrics) args[1])
    );

    static {
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> DataStreamFailureStore.fromXContent(p),
            FAILURE_STORE_FIELD
        );
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> DataStreamDerivedMetrics.fromXContent(p),
            DERIVED_METRICS_FIELD
        );
    }

    private static final TransportVersion INTRODUCE_FAILURES_LIFECYCLE = TransportVersion.fromName("introduce_failures_lifecycle");
    public static final TransportVersion DERIVED_METRICS_IN_DATA_STREAM_OPTIONS = TransportVersion.fromName(
        "derived_metrics_in_data_stream_options"
    );

    public DataStreamOptions(@Nullable DataStreamFailureStore failureStore) {
        this(failureStore, null);
    }

    public static DataStreamOptions read(StreamInput in) throws IOException {
        DataStreamFailureStore failureStore = in.readOptionalWriteable(DataStreamFailureStore::new);
        DataStreamDerivedMetrics derivedMetrics = in.getTransportVersion().supports(DERIVED_METRICS_IN_DATA_STREAM_OPTIONS)
            ? in.readOptionalWriteable(DataStreamDerivedMetrics::new)
            : null;
        return new DataStreamOptions(failureStore, derivedMetrics);
    }

    public static Diff<DataStreamOptions> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(DataStreamOptions::read, in);
    }

    /**
     * @return true if none of the options are defined
     */
    public boolean isEmpty() {
        return failureStore == null && derivedMetrics == null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(INTRODUCE_FAILURES_LIFECYCLE) || failureStore == null || failureStore().enabled() != null) {
            out.writeOptionalWriteable(failureStore);
        } else {
            // When communicating with older versions we need to ensure we do not sent an invalid failure store config.
            // If the enabled flag is not defined, we treat it as null.
            out.writeOptionalWriteable(null);
        }
        if (out.getTransportVersion().supports(DERIVED_METRICS_IN_DATA_STREAM_OPTIONS)) {
            out.writeOptionalWriteable(derivedMetrics);
        }
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (failureStore != null) {
            builder.field(FAILURE_STORE_FIELD.getPreferredName(), failureStore);
        }
        if (derivedMetrics != null) {
            builder.field(DERIVED_METRICS_FIELD.getPreferredName(), derivedMetrics);
        }
        builder.endObject();
        return builder;
    }

    public static DataStreamOptions fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * This class is only used in template configuration. It wraps the fields of {@link DataStreamOptions} with {@link ResettableValue}
     * to allow a user to signal when they want to reset any previously encountered values during template composition.
     */
    public record Template(
        ResettableValue<DataStreamFailureStore.Template> failureStore,
        ResettableValue<DataStreamDerivedMetrics.Template> derivedMetrics
    ) implements Writeable, ToXContentObject {
        public static final Template EMPTY = new Template(ResettableValue.undefined(), ResettableValue.undefined());

        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<Template, Void> PARSER = new ConstructingObjectParser<>(
            "data_stream_options_template",
            false,
            (args, unused) -> new Template(
                args[0] == null ? ResettableValue.undefined() : (ResettableValue<DataStreamFailureStore.Template>) args[0],
                args[1] == null ? ResettableValue.undefined() : (ResettableValue<DataStreamDerivedMetrics.Template>) args[1]
            )
        );

        static {
            PARSER.declareObjectOrNull(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, s) -> ResettableValue.create(DataStreamFailureStore.Template.fromXContent(p)),
                ResettableValue.reset(),
                FAILURE_STORE_FIELD
            );
            PARSER.declareObjectOrNull(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, s) -> ResettableValue.create(DataStreamDerivedMetrics.Template.fromXContent(p)),
                ResettableValue.reset(),
                DERIVED_METRICS_FIELD
            );
        }

        public Template(DataStreamFailureStore.Template template) {
            this(ResettableValue.create(template), ResettableValue.undefined());
        }

        public Template(ResettableValue<DataStreamFailureStore.Template> failureStore) {
            this(failureStore, ResettableValue.undefined());
        }

        public Template(DataStreamDerivedMetrics.Template derivedMetrics) {
            this(ResettableValue.undefined(), ResettableValue.create(derivedMetrics));
        }

        public Template {
            assert failureStore != null : "Template does not accept null values, please use Resettable.undefined()";
            assert derivedMetrics != null : "Template does not accept null values, please use Resettable.undefined()";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().supports(INTRODUCE_FAILURES_LIFECYCLE)
                || failureStore.get() == null
                || failureStore().mapAndGet(DataStreamFailureStore.Template::enabled).get() != null) {
                ResettableValue.write(out, failureStore, (o, v) -> v.writeTo(o));
                // When communicating with older versions we need to ensure we do not sent an invalid failure store config.
            } else {
                // If the enabled flag is not defined, we treat failure store as not defined, if reset we treat the failure store as reset
                ResettableValue<DataStreamFailureStore.Template> bwcFailureStore = failureStore.get().enabled().shouldReset()
                    ? ResettableValue.reset()
                    : ResettableValue.undefined();
                ResettableValue.write(out, bwcFailureStore, (o, v) -> v.writeTo(o));
            }
            if (out.getTransportVersion().supports(DERIVED_METRICS_IN_DATA_STREAM_OPTIONS)) {
                ResettableValue.write(out, derivedMetrics, (o, v) -> v.writeTo(o));
            }
        }

        public static Template read(StreamInput in) throws IOException {
            ResettableValue<DataStreamFailureStore.Template> failureStore = ResettableValue.read(in, DataStreamFailureStore.Template::read);
            ResettableValue<DataStreamDerivedMetrics.Template> derivedMetrics = in.getTransportVersion()
                .supports(DERIVED_METRICS_IN_DATA_STREAM_OPTIONS)
                    ? ResettableValue.read(in, DataStreamDerivedMetrics.Template::read)
                    : ResettableValue.undefined();
            return new Template(failureStore, derivedMetrics);
        }

        public static Template fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        /**
         * Converts the template to XContent, depending on the {@param params} set by {@link ResettableValue#hideResetValues(Params)}
         * it may or may not display any explicit nulls when the value is to be reset.
         */
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            failureStore.toXContent(builder, params, FAILURE_STORE_FIELD.getPreferredName());
            derivedMetrics.toXContent(builder, params, DERIVED_METRICS_FIELD.getPreferredName());
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return Strings.toString(this, true, true);
        }
    }

    public static Builder builder(Template template) {
        return new Builder(template);
    }

    /**
     * Builds and composes the data stream options or the respective template.
     */
    public static class Builder {
        private DataStreamFailureStore.Builder failureStore = null;
        private DataStreamDerivedMetrics.Builder derivedMetrics = null;

        public Builder(Template template) {
            if (template != null && template.failureStore().get() != null) {
                failureStore = DataStreamFailureStore.builder(template.failureStore().get());
            }
            if (template != null && template.derivedMetrics().get() != null) {
                derivedMetrics = new DataStreamDerivedMetrics.Builder(template.derivedMetrics().get());
            }
        }

        public Builder(DataStreamOptions options) {
            if (options != null && options.failureStore() != null) {
                failureStore = DataStreamFailureStore.builder(options.failureStore());
            }
            if (options != null && options.derivedMetrics() != null) {
                derivedMetrics = new DataStreamDerivedMetrics.Builder(options.derivedMetrics());
            }
        }

        /**
         * Composes this builder with the values of the provided template. This is not a replacement necessarily, the
         * inner values will be merged.
         */
        public Builder composeTemplate(DataStreamOptions.Template options) {
            return failureStore(options.failureStore()).derivedMetrics(options.derivedMetrics());
        }

        /**
         * Composes the current failure store configuration with the provided value. This is not a replacement necessarily, if both
         * instance contain data the configurations are merged.
         */
        public Builder failureStore(ResettableValue<DataStreamFailureStore.Template> newFailureStore) {
            if (newFailureStore.shouldReset()) {
                failureStore = null;
            } else if (newFailureStore.isDefined()) {
                if (failureStore == null) {
                    failureStore = DataStreamFailureStore.builder(newFailureStore.get());
                } else {
                    failureStore.composeTemplate(newFailureStore.get());
                }
            }
            return this;
        }

        public Builder derivedMetrics(ResettableValue<DataStreamDerivedMetrics.Template> newDerivedMetrics) {
            if (newDerivedMetrics.shouldReset()) {
                derivedMetrics = null;
            } else if (newDerivedMetrics.isDefined()) {
                if (derivedMetrics == null) {
                    derivedMetrics = new DataStreamDerivedMetrics.Builder(newDerivedMetrics.get());
                } else {
                    derivedMetrics.composeTemplate(newDerivedMetrics.get());
                }
            }
            return this;
        }

        public Template buildTemplate() {
            return new Template(
                ResettableValue.create(failureStore == null ? null : failureStore.buildTemplate()),
                ResettableValue.create(derivedMetrics == null ? null : derivedMetrics.buildTemplate())
            );
        }

        public DataStreamOptions build() {
            return new DataStreamOptions(
                failureStore == null ? null : failureStore.build(),
                derivedMetrics == null ? null : derivedMetrics.build()
            );
        }
    }
}
