/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.builder;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A specification of a join field that fetches fields from another index using _id.
 * @see SearchSourceBuilder#joinFields()
 */
public final class JoinField implements Writeable, ToXContentFragment {

    private static final ParseField TYPE_FIELD = new ParseField("type");
    private static final ParseField NAME_FIELD = new ParseField("name");
    private static final ParseField INDEX_FIELD = new ParseField("index");
    private static final ParseField KEY_FIELD = new ParseField("key");
    private static final ParseField FIELDS_FIELD = new ParseField("fields");

    public static final String LOOKUP_TYPE = "lookup";

    public static <V> ConstructingObjectParser<V, Void> registerXContentParser(ConstructingObjectParser<V, Void> parser) {
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), TYPE_FIELD);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), NAME_FIELD);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), INDEX_FIELD);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), KEY_FIELD);
        parser.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), FIELDS_FIELD);
        return parser;
    }

    private final String name;
    private final String index;
    private final String keyField;
    private final String[] fields;

    public JoinField(String name, String index, String keyField, List<String> fields) {
        this.name = Objects.requireNonNull(name, "[name] of a join field must be specified");
        this.index = Objects.requireNonNull(index, "[index] of a join field must be specified");
        this.keyField = Objects.requireNonNull(keyField, "[key] of a join field must be specified");
        this.fields = fields != null ? fields.toArray(String[]::new) : Strings.EMPTY_ARRAY;
    }

    public JoinField(StreamInput in) throws IOException {
        this.name = in.readString();
        this.index = in.readString();
        this.keyField = in.readString();
        this.fields = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(index);
        out.writeString(keyField);
        out.writeStringArray(fields);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TYPE_FIELD.getPreferredName(), LOOKUP_TYPE);
        builder.field(NAME_FIELD.getPreferredName(), name);
        builder.field(INDEX_FIELD.getPreferredName(), index);
        builder.field(KEY_FIELD.getPreferredName(), keyField);
        if (fields.length > 0) {
            builder.field(FIELDS_FIELD.getPreferredName(), fields);
        }
        builder.endObject();
        return builder;
    }

    public String getName() {
        return name;
    }

    public String getIndex() {
        return index;
    }

    public String getKeyField() {
        return keyField;
    }

    @Nullable
    public String[] getFields() {
        return fields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JoinField that = (JoinField) o;
        return name.equals(that.name) && index.equals(that.index) && keyField.equals(that.keyField) && Arrays.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(name, index, keyField);
        result = 31 * result + Arrays.hashCode(fields);
        return result;
    }
}
