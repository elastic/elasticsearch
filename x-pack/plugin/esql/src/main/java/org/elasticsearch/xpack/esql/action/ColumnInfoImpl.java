/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.InstantiatingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ParserConstructor;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.UnionTypedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class ColumnInfoImpl implements ColumnInfo {

    public static final InstantiatingObjectParser<ColumnInfoImpl, Void> PARSER;
    static {
        InstantiatingObjectParser.Builder<ColumnInfoImpl, Void> parser = InstantiatingObjectParser.builder(
            "esql/column_info",
            true,
            ColumnInfoImpl.class
        );
        parser.declareString(constructorArg(), new ParseField("name"));
        parser.declareString(constructorArg(), new ParseField("type"));
        parser.declareStringArray(optionalConstructorArg(), new ParseField("original_types"));
        PARSER = parser.build();
    }

    private static final TransportVersion ESQL_REPORT_ORIGINAL_TYPES = TransportVersion.fromName("esql_report_original_types");
    private static final TransportVersion ESQL_COLUMN_META = TransportVersion.fromName("esql_column_meta");

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if ((o instanceof ColumnInfoImpl that)) {
            return Objects.equals(name, that.name)
                && Objects.equals(type, that.type)
                && Objects.equals(originalTypes, that.originalTypes)
                && Objects.equals(meta, that.meta);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, originalTypes, meta);
    }

    public static ColumnInfo fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String name;
    private final DataType type;
    /**
     * If this field is unsupported this contains the underlying ES types. If there
     * is a type conflict this will have many elements, some or all of which may
     * be actually supported types.
     */
    @Nullable
    private final List<String> originalTypes;

    @Nullable
    private final DataType suggestedCast;

    @Nullable
    private final Map<String, Object> meta;

    @ParserConstructor
    public ColumnInfoImpl(String name, String type, @Nullable List<String> originalTypes) {
        this(name, DataType.fromEs(type), originalTypes, null);
    }

    public ColumnInfoImpl(String name, String type, @Nullable List<String> originalTypes, @Nullable Map<String, Object> meta) {
        this(name, DataType.fromEs(type), originalTypes, meta);
    }

    public ColumnInfoImpl(String name, DataType type, @Nullable List<String> originalTypes) {
        this(name, type, originalTypes, null);
    }

    public ColumnInfoImpl(String name, DataType type, @Nullable List<String> originalTypes, @Nullable Map<String, Object> meta) {
        this.name = name;
        this.type = type;
        this.originalTypes = originalTypes;
        this.suggestedCast = calculateSuggestedCast(this.originalTypes);
        this.meta = meta;
    }

    private static DataType calculateSuggestedCast(List<String> originalTypes) {
        if (originalTypes == null) {
            return null;
        }
        return DataType.suggestedCast(
            originalTypes.stream().map(DataType::fromTypeName).filter(Objects::nonNull).collect(Collectors.toSet())
        );
    }

    /**
     * The column an output attribute becomes, including the {@code original_types} it declares.
     * <p>
     * Two kinds of column declare original types and they mean different things, which is why the choice lives here
     * rather than once per transport action. For an unsupported column the list is diagnostic - the raw mapping type
     * names that disagreed, unwidened and in no particular order - so it is sorted for the reader. For a union column
     * the list is the leg order: a value's tag is an index into it, and the block the data node built indexes the same
     * carried list. So this reports the order the field holds rather than imposing one, which is what keeps the single
     * owner single. The canonical order happens to be sorted, so that is a statement about where the order comes from
     * rather than one you could tell apart by reading one response.
     * </p>
     */
    public static ColumnInfoImpl forAttribute(Attribute attribute, @Nullable Map<String, Object> meta) {
        return new ColumnInfoImpl(attribute.name(), attribute.dataType(), originalTypesOf(attribute), meta);
    }

    @Nullable
    private static List<String> originalTypesOf(Attribute attribute) {
        if (attribute instanceof UnsupportedAttribute unsupported) {
            // Sorted so they are easier to test against and prettier. Nothing reads them by position.
            List<String> types = new ArrayList<>(unsupported.originalTypes());
            Collections.sort(types);
            return types;
        }
        List<DataType> legTypes = UnionTypedExpression.legTypesOf(attribute);
        if (legTypes != null) {
            // The mapping type name, as field_caps spells it, because that is the vocabulary this field already
            // speaks for an unsupported column and a client should not have to know two. It differs from the type's
            // own name for exactly one leg type today, `date` against `datetime`, and the renderer's lookup accepts
            // both; UnionEsFieldTests pins that for every leg type rather than for the one that differs.
            return legTypes.stream().map(DataType::esType).toList();
        }
        return null;
    }

    public ColumnInfoImpl(StreamInput in) throws IOException {
        this.name = in.readString();
        this.type = DataType.fromEs(in.readString());
        if (in.getTransportVersion().supports(ESQL_REPORT_ORIGINAL_TYPES)) {
            this.originalTypes = in.readOptionalStringCollectionAsList();
            this.suggestedCast = calculateSuggestedCast(this.originalTypes);
        } else {
            this.originalTypes = null;
            this.suggestedCast = null;
        }
        if (in.getTransportVersion().supports(ESQL_COLUMN_META)) {
            this.meta = in.readOptional(StreamInput::readGenericMap);
        } else {
            this.meta = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(type.outputType());
        if (out.getTransportVersion().supports(ESQL_REPORT_ORIGINAL_TYPES)) {
            out.writeOptionalStringCollection(originalTypes);
        }
        if (out.getTransportVersion().supports(ESQL_COLUMN_META)) {
            out.writeOptional(StreamOutput::writeGenericMap, meta);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("name", name);
        builder.field("type", type.outputType());
        if (originalTypes != null) {
            builder.field("original_types", originalTypes);
        }
        if (suggestedCast != null) {
            builder.field("suggested_cast", suggestedCast.typeName());
        }
        if (meta != null) {
            builder.field("_meta", meta);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String outputType() {
        return type.outputType();
    }

    public DataType type() {
        return type;
    }

    @Nullable
    public List<String> originalTypes() {
        return originalTypes;
    }

    @Nullable
    public Map<String, Object> meta() {
        return meta;
    }

    public String toString() {
        return "ColumnInfoImpl{" + "name='" + name + '\'' + ", type=" + type + ", originalTypes=" + originalTypes + ", meta=" + meta + '}';
    }
}
