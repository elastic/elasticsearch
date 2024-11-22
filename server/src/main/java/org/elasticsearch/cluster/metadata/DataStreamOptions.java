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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.cluster.metadata.DataStreamFailureStore.FAILURE_STORE;

/**
 * Holds data stream dedicated configuration options such as failure store, (in the future lifecycle). Currently, it
 * supports the following configurations:
 * - failure store
 */
public class DataStreamOptions implements SimpleDiffable<DataStreamOptions>, ToXContentObject {

    public static final ParseField FAILURE_STORE_FIELD = new ParseField(FAILURE_STORE);
    public static final DataStreamOptions FAILURE_STORE_ENABLED = new DataStreamOptions(new DataStreamFailureStore(true));
    public static final DataStreamOptions FAILURE_STORE_DISABLED = new DataStreamOptions(new DataStreamFailureStore(false));
    public static final DataStreamOptions EMPTY = new DataStreamOptions(null);

    public static final ConstructingObjectParser<DataStreamOptions, Void> PARSER = new ConstructingObjectParser<>(
        "options",
        false,
        (args, unused) -> new DataStreamOptions((DataStreamFailureStore) args[0])
    );
    @Nullable
    private final DataStreamFailureStore failureStore;

    public DataStreamOptions(@Nullable DataStreamFailureStore failureStore) {
        this.failureStore = failureStore;
    }

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

    /**
     * Determines if this data stream has its failure store enabled or not. Currently, the failure store
     * is enabled only when a user has explicitly requested it.
     *
     * @return true, if the user has explicitly enabled the failure store.
     */
    public boolean isFailureStoreEnabled() {
        return failureStore != null && Boolean.TRUE.equals(failureStore.enabled());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(failureStore);
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

    @Nullable
    public DataStreamFailureStore failureStore() {
        return failureStore;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (DataStreamOptions) obj;
        return Objects.equals(this.failureStore, that.failureStore);
    }

    @Override
    public int hashCode() {
        return Objects.hash(failureStore);
    }

    /**
     * This class is only used in template configuration. It allows us to represent explicit null values and denotes that the fields
     * of the failure can be merged.
     */
    public static class Template implements Writeable, ToXContentObject {
        public static final Template EMPTY = new Template();

        @Nullable
        private final org.elasticsearch.cluster.metadata.Template.ExplicitlyNullable<DataStreamFailureStore.Template> failureStore;

        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<DataStreamOptions.Template, Void> PARSER = new ConstructingObjectParser<>(
            "data_stream_options_template",
            false,
            (args, unused) -> new DataStreamOptions.Template(
                (org.elasticsearch.cluster.metadata.Template.ExplicitlyNullable<DataStreamFailureStore.Template>) args[0]
            )
        );

        static {
            PARSER.declareObjectOrNull(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, s) -> org.elasticsearch.cluster.metadata.Template.ExplicitlyNullable.create(
                    DataStreamFailureStore.Template.fromXContent(p)
                ),
                org.elasticsearch.cluster.metadata.Template.ExplicitlyNullable.empty(),
                FAILURE_STORE_FIELD
            );
        }

        private Template() {
            this.failureStore = null;
        }

        public Template(org.elasticsearch.cluster.metadata.Template.ExplicitlyNullable<DataStreamFailureStore.Template> failureStore) {
            this.failureStore = failureStore;
        }

        public org.elasticsearch.cluster.metadata.Template.ExplicitlyNullable<DataStreamFailureStore.Template> failureStore() {
            return failureStore;
        }

        public boolean isFailureStoreDefined() {
            return failureStore != null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            org.elasticsearch.cluster.metadata.Template.ExplicitlyNullable.write(out, failureStore, (o, v) -> v.writeTo(o));
        }

        public static Template read(StreamInput in) throws IOException {
            org.elasticsearch.cluster.metadata.Template.ExplicitlyNullable<DataStreamFailureStore.Template> failureStore =
                org.elasticsearch.cluster.metadata.Template.ExplicitlyNullable.read(in, DataStreamFailureStore.Template::read);
            return new Template(failureStore);
        }

        public static Template fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        /**
         * Converts the template to XContent, when the parameter {@link org.elasticsearch.cluster.metadata.Template.ExplicitlyNullable#SKIP_EXPLICIT_NULLS} is set to true, it will not
         * display any explicit nulls
         */
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            boolean skipExplicitNulls = params.paramAsBoolean(
                org.elasticsearch.cluster.metadata.Template.ExplicitlyNullable.SKIP_EXPLICIT_NULLS,
                false
            );
            builder.startObject();
            if (failureStore != null) {
                if (failureStore.isNull() == false) {
                    builder.field(FAILURE_STORE_FIELD.getPreferredName(), failureStore.get());
                } else if (skipExplicitNulls == false) {
                    builder.nullField(FAILURE_STORE_FIELD.getPreferredName());
                }
            }
            builder.endObject();
            return builder;
        }

        @Nullable
        public DataStreamOptions toDataStreamOptions() {
            return new DataStreamOptions(
                failureStore == null ? null : failureStore.applyAndGet(DataStreamFailureStore.Template::toFailureStore)
            );
        }

        public static Builder builder(Template template) {
            return new Builder(template);
        }

        public static class Builder {
            private org.elasticsearch.cluster.metadata.Template.ExplicitlyNullable<DataStreamFailureStore.Template> failureStore = null;

            public Builder(Template template) {
                if (template != null) {
                    failureStore = template.failureStore();
                }
            }

            public Builder updateFailureStore(
                org.elasticsearch.cluster.metadata.Template.ExplicitlyNullable<DataStreamFailureStore.Template> failureStore
            ) {
                DataStreamFailureStore.Template current = this.failureStoreValue();
                this.failureStore = current == null ? failureStore : failureStore.map(current::mergeWith);
                return this;
            }

            public DataStreamFailureStore.Template failureStoreValue() {
                return failureStore == null ? null : failureStore.get();
            }

            public Template build() {
                return new Template(failureStore);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Template template = (Template) o;
            return Objects.equals(failureStore, template.failureStore);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(failureStore);
        }

        @Override
        public String toString() {
            return Strings.toString(this, true, true);
        }
    }

}
