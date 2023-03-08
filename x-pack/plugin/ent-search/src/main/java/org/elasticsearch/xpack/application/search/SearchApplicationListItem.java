/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * This class is used for returning information for lists of search applications, to avoid including all
 * {@link SearchApplication} information which can be retrieved using subsequent Get Search Application requests.
 */
public class SearchApplicationListItem implements Writeable, ToXContentObject {

    public static final ParseField NAME_FIELD = new ParseField("name");
    public static final ParseField INDICES_FIELD = new ParseField("indices");
    public static final ParseField SEARCH_ALIAS_NAME_FIELD = new ParseField("search_alias");
    public static final ParseField ANALYTICS_COLLECTION_NAME_FIELD = new ParseField("analytics_collection_name");
    private final String name;
    private final String[] indices;
    private final String searchAlias;
    private final String analyticsCollectionName;

    public SearchApplicationListItem(String name, String[] indices, String searchAlias, @Nullable String analyticsCollectionName) {
        this.name = name;
        this.indices = indices;
        this.searchAlias = searchAlias;
        this.analyticsCollectionName = analyticsCollectionName;
    }

    public SearchApplicationListItem(StreamInput in) throws IOException {
        this.name = in.readString();
        this.indices = in.readStringArray();
        this.searchAlias = in.readString();
        this.analyticsCollectionName = in.readOptionalString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME_FIELD.getPreferredName(), name);
        builder.field(INDICES_FIELD.getPreferredName(), indices);
        builder.field(SEARCH_ALIAS_NAME_FIELD.getPreferredName(), searchAlias);
        if (analyticsCollectionName != null) {
            builder.field(ANALYTICS_COLLECTION_NAME_FIELD.getPreferredName(), analyticsCollectionName);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeStringArray(indices);
        out.writeString(searchAlias);
        out.writeOptionalString(analyticsCollectionName);
    }

    public String name() {
        return name;
    }

    public String[] indices() {
        return indices;
    }

    public String searchAlias() {
        return searchAlias;
    }

    public String analyticsCollectionName() {
        return analyticsCollectionName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchApplicationListItem item = (SearchApplicationListItem) o;
        return name.equals(item.name)
            && Arrays.equals(indices, item.indices)
            && Objects.equals(searchAlias, item.searchAlias)
            && Objects.equals(analyticsCollectionName, item.analyticsCollectionName);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(name, searchAlias, analyticsCollectionName);
        result = 31 * result + Arrays.hashCode(indices);
        return result;
    }
}
