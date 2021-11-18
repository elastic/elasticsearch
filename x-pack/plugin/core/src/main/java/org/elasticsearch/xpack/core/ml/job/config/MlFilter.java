/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MlStrings;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

public class MlFilter implements ToXContentObject, Writeable {

    /**
     * The max number of items allowed per filter.
     * Limiting the number of items protects users
     * from running into excessive overhead due to
     * filters using too much memory and lookups
     * becoming too expensive.
     */
    private static final int MAX_ITEMS = 10000;

    public static final String DOCUMENT_ID_PREFIX = "filter_";

    public static final String FILTER_TYPE = "filter";

    public static final ParseField TYPE = new ParseField("type");
    public static final ParseField ID = new ParseField("filter_id");
    public static final ParseField DESCRIPTION = new ParseField("description");
    public static final ParseField ITEMS = new ParseField("items");

    // For QueryPage
    public static final ParseField RESULTS_FIELD = new ParseField("filters");

    public static final ObjectParser<Builder, Void> STRICT_PARSER = createParser(false);
    public static final ObjectParser<Builder, Void> LENIENT_PARSER = createParser(true);

    private static ObjectParser<Builder, Void> createParser(boolean ignoreUnknownFields) {
        ObjectParser<Builder, Void> parser = new ObjectParser<>(TYPE.getPreferredName(), ignoreUnknownFields, Builder::new);

        parser.declareString((builder, s) -> {}, TYPE);
        parser.declareString(Builder::setId, ID);
        parser.declareStringOrNull(Builder::setDescription, DESCRIPTION);
        parser.declareStringArray(Builder::setItems, ITEMS);

        return parser;
    }

    private final String id;
    private final String description;
    private final SortedSet<String> items;

    private MlFilter(String id, String description, SortedSet<String> items) {
        this.id = Objects.requireNonNull(id);
        this.description = description;
        this.items = Objects.requireNonNull(items);
    }

    public MlFilter(StreamInput in) throws IOException {
        id = in.readString();
        description = in.readOptionalString();
        items = new TreeSet<>(Arrays.asList(in.readStringArray()));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeOptionalString(description);
        out.writeStringArray(items.toArray(new String[items.size()]));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID.getPreferredName(), id);
        if (description != null) {
            builder.field(DESCRIPTION.getPreferredName(), description);
        }
        builder.field(ITEMS.getPreferredName(), items);
        if (params.paramAsBoolean(ToXContentParams.FOR_INTERNAL_STORAGE, false)) {
            builder.field(TYPE.getPreferredName(), FILTER_TYPE);
        }
        builder.endObject();
        return builder;
    }

    public String getId() {
        return id;
    }

    public String getDescription() {
        return description;
    }

    public SortedSet<String> getItems() {
        return Collections.unmodifiableSortedSet(items);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if ((obj instanceof MlFilter) == false) {
            return false;
        }

        MlFilter other = (MlFilter) obj;
        return id.equals(other.id) && Objects.equals(description, other.description) && items.equals(other.items);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, description, items);
    }

    public String documentId() {
        return documentId(id);
    }

    public static String documentId(String filterId) {
        return DOCUMENT_ID_PREFIX + filterId;
    }

    public static Builder builder(String filterId) {
        return new Builder().setId(filterId);
    }

    public static class Builder {

        private String id;
        private String description;
        private SortedSet<String> items = new TreeSet<>();

        private Builder() {}

        public Builder setId(String id) {
            this.id = id;
            return this;
        }

        @Nullable
        public String getId() {
            return id;
        }

        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder setItems(SortedSet<String> items) {
            this.items = items;
            return this;
        }

        public Builder setItems(List<String> items) {
            this.items = new TreeSet<>(items);
            return this;
        }

        public Builder setItems(String... items) {
            setItems(Arrays.asList(items));
            return this;
        }

        public MlFilter build() {
            ExceptionsHelper.requireNonNull(id, MlFilter.ID.getPreferredName());
            ExceptionsHelper.requireNonNull(items, MlFilter.ITEMS.getPreferredName());
            if (MlStrings.isValidId(id) == false) {
                throw ExceptionsHelper.badRequestException(Messages.getMessage(Messages.INVALID_ID, ID.getPreferredName(), id));
            }
            if (items.size() > MAX_ITEMS) {
                throw ExceptionsHelper.badRequestException(Messages.getMessage(Messages.FILTER_CONTAINS_TOO_MANY_ITEMS, id, MAX_ITEMS));
            }
            return new MlFilter(id, description, items);
        }
    }
}
