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
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.cluster.metadata.DataStreamFailureStore.FAILURE_STORE;

/**
 * Holds data stream dedicated configuration options such as failure store, (in the future lifecycle). Currently, it
 * supports the following configurations:
 * - failure store
 */
public record DataStreamOptions(@Nullable DataStreamFailureStore failureStore)
    implements
        SimpleDiffable<DataStreamOptions>,
        ToXContentObject {

    public static final ParseField FAILURE_STORE_FIELD = new ParseField(FAILURE_STORE);
    public static final DataStreamOptions FAILURE_STORE_ENABLED = new DataStreamOptions(new DataStreamFailureStore(true, null));
    public static final DataStreamOptions FAILURE_STORE_DISABLED = new DataStreamOptions(new DataStreamFailureStore(false, null));
    public static final DataStreamOptions EMPTY = new DataStreamOptions(null);

    public static final ConstructingObjectParser<DataStreamOptions, Void> PARSER = new ConstructingObjectParser<>(
        "options",
        false,
        (args, unused) -> new DataStreamOptions((DataStreamFailureStore) args[0])
    );

    static {
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> DataStreamFailureStore.fromXContent(p),
            FAILURE_STORE_FIELD
        );
    }

    public static DataStreamOptions read(StreamInput in) throws IOException {
        return new DataStreamOptions(in.readOptionalWriteable(DataStreamFailureStore::new));
    }

    public static Diff<DataStreamOptions> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(DataStreamOptions::read, in);
    }

    /**
     * @return true if none of the options are defined
     */
    public boolean isEmpty() {
        return failureStore == null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.INTRODUCE_FAILURES_LIFECYCLE)
            || out.getTransportVersion().isPatchFrom(TransportVersions.INTRODUCE_FAILURES_LIFECYCLE_BACKPORT_8_19)
            || failureStore == null
            || failureStore().enabled() != null) {
            out.writeOptionalWriteable(failureStore);
        } else {
            // When communicating with older versions we need to ensure we do not sent an invalid failure store config.
            // If the enabled flag is not defined, we treat it as null.
            out.writeOptionalWriteable(null);
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
    public record Template(ResettableValue<DataStreamFailureStore.Template> failureStore) implements Writeable, ToXContentObject {
        public static final Template EMPTY = new Template(ResettableValue.undefined());

        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<Template, Void> PARSER = new ConstructingObjectParser<>(
            "data_stream_options_template",
            false,
            (args, unused) -> new Template(
                args[0] == null ? ResettableValue.undefined() : (ResettableValue<DataStreamFailureStore.Template>) args[0]
            )
        );

        static {
            PARSER.declareObjectOrNull(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, s) -> ResettableValue.create(DataStreamFailureStore.Template.fromXContent(p)),
                ResettableValue.reset(),
                FAILURE_STORE_FIELD
            );
        }

        public Template(DataStreamFailureStore.Template template) {
            this(ResettableValue.create(template));
        }

        public Template {
            assert failureStore != null : "Template does not accept null values, please use Resettable.undefined()";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().onOrAfter(TransportVersions.INTRODUCE_FAILURES_LIFECYCLE)
                || out.getTransportVersion().isPatchFrom(TransportVersions.INTRODUCE_FAILURES_LIFECYCLE_BACKPORT_8_19)
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
        }

        public static Template read(StreamInput in) throws IOException {
            ResettableValue<DataStreamFailureStore.Template> failureStore = ResettableValue.read(in, DataStreamFailureStore.Template::read);
            return new Template(failureStore);
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

        public Builder(Template template) {
            if (template != null && template.failureStore().get() != null) {
                failureStore = DataStreamFailureStore.builder(template.failureStore().get());
            }
        }

        public Builder(DataStreamOptions options) {
            if (options != null && options.failureStore() != null) {
                failureStore = DataStreamFailureStore.builder(options.failureStore());
            }
        }

        /**
         * Composes this builder with the values of the provided template. This is not a replacement necessarily, the
         * inner values will be merged.
         */
        public Builder composeTemplate(DataStreamOptions.Template options) {
            return failureStore(options.failureStore());
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

        public Template buildTemplate() {
            return new Template(failureStore == null ? null : failureStore.buildTemplate());
        }

        public DataStreamOptions build() {
            return new DataStreamOptions(failureStore == null ? null : failureStore.build());
        }
    }
}
