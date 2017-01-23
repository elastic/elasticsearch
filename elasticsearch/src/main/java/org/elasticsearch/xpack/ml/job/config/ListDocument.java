/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class ListDocument extends ToXContentToBytes implements Writeable {
    public static final ParseField TYPE = new ParseField("list");
    public static final ParseField ID = new ParseField("id");
    public static final ParseField ITEMS = new ParseField("items");

    // For QueryPage
    public static final ParseField RESULTS_FIELD = new ParseField("lists");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<ListDocument, Void> PARSER = new ConstructingObjectParser<>(
            TYPE.getPreferredName(), a -> new ListDocument((String) a[0], (List<String>) a[1]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ID);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), ITEMS);
    }

    private final String id;
    private final List<String> items;

    public ListDocument(String id, List<String> items) {
        this.id = Objects.requireNonNull(id, ID.getPreferredName() + " must not be null");
        this.items = Objects.requireNonNull(items, ITEMS.getPreferredName() + " must not be null");
    }

    public ListDocument(StreamInput in) throws IOException {
        id = in.readString();
        items = Arrays.asList(in.readStringArray());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeStringArray(items.toArray(new String[items.size()]));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID.getPreferredName(), id);
        builder.field(ITEMS.getPreferredName(), items);
        builder.endObject();
        return builder;
    }

    public String getId() {
        return id;
    }

    public List<String> getItems() {
        return new ArrayList<>(items);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof ListDocument)) {
            return false;
        }

        ListDocument other = (ListDocument) obj;
        return id.equals(other.id) && items.equals(other.items);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, items);
    }
}