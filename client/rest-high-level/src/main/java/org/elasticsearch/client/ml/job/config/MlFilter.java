/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * An MlFilter Object
 *
 * A filter contains a list of strings.
 * It can be used by one or more jobs.
 *
 * Specifically, filters are referenced in the custom_rules property of detector configuration objects.
 */
public class MlFilter implements ToXContentObject {

    public static final ParseField TYPE = new ParseField("type");
    public static final ParseField ID = new ParseField("filter_id");
    public static final ParseField DESCRIPTION = new ParseField("description");
    public static final ParseField ITEMS = new ParseField("items");

    // For QueryPage
    public static final ParseField RESULTS_FIELD = new ParseField("filters");

    public static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>(TYPE.getPreferredName(), true, Builder::new);

    static {
        PARSER.declareString((builder, s) -> {}, TYPE);
        PARSER.declareString(Builder::setId, ID);
        PARSER.declareStringOrNull(Builder::setDescription, DESCRIPTION);
        PARSER.declareStringArray(Builder::setItems, ITEMS);
    }

    private final String id;
    private final String description;
    private final SortedSet<String> items;

    private MlFilter(String id, String description, SortedSet<String> items) {
        this.id = Objects.requireNonNull(id);
        this.description = description;
        this.items = Collections.unmodifiableSortedSet(items);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID.getPreferredName(), id);
        if (description != null) {
            builder.field(DESCRIPTION.getPreferredName(), description);
        }
        builder.field(ITEMS.getPreferredName(), items);
        // Don't include TYPE as it's fixed
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
        return items;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj instanceof MlFilter == false) {
            return false;
        }

        MlFilter other = (MlFilter) obj;
        return id.equals(other.id) && Objects.equals(description, other.description) && items.equals(other.items);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, description, items);
    }

    /**
     * Creates a new Builder object for creating an MlFilter object
     * @param filterId The ID of the filter to create
     */
    public static Builder builder(String filterId) {
        return new Builder().setId(filterId);
    }

    public static class Builder {

        private String id;
        private String description;
        private SortedSet<String> items = new TreeSet<>();

        private Builder() {
        }

        /**
         * Set the ID of the filter
         * @param id The id desired
         */
        public Builder setId(String id) {
            this.id = Objects.requireNonNull(id);
            return this;
        }

        @Nullable
        public String getId() {
            return id;
        }

        /**
         * Set the description of the filter
         * @param description The description desired
         */
        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder setItems(SortedSet<String> items) {
            this.items = Objects.requireNonNull(items);
            return this;
        }

        public Builder setItems(List<String> items) {
            this.items = new TreeSet<>(items);
            return this;
        }

        /**
         * The items of the filter.
         *
         * A wildcard * can be used at the beginning or the end of an item. Up to 10000 items are allowed in each filter.
         *
         * @param items String list of items to be applied in the filter
         */
        public Builder setItems(String... items) {
            setItems(Arrays.asList(items));
            return this;
        }

        public MlFilter build() {
            return new MlFilter(id, description, items);
        }
    }
}
