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
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Implementation of column metadata information for ESQL query results.
 * <p>
 * This class provides detailed information about a column in an ESQL query result, including
 * its name, data type, and original Elasticsearch types. It supports serialization for
 * network transport and XContent for REST API responses.
 * </p>
 * <p>
 * When a column type cannot be determined or there's a type conflict across multiple indices,
 * the {@code originalTypes} field will contain the underlying Elasticsearch type names. The
 * {@code suggestedCast} field provides a recommended cast type when applicable.
 * </p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Creating column info for a simple integer column
 * ColumnInfo columnInfo = new ColumnInfoImpl("age", DataType.INTEGER, null);
 *
 * // Creating column info with original types (for unsupported or conflicting types)
 * List<String> originalTypes = List.of("integer", "long");
 * ColumnInfo columnInfo = new ColumnInfoImpl("value", DataType.UNSUPPORTED, originalTypes);
 *
 * // Accessing column information
 * String name = columnInfo.name();
 * String type = columnInfo.outputType();
 *
 * // Deserializing from XContent
 * ColumnInfo columnInfo = ColumnInfoImpl.fromXContent(parser);
 * }</pre>
 */
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if ((o instanceof ColumnInfoImpl that)) {
            return Objects.equals(name, that.name) && Objects.equals(type, that.type) && Objects.equals(originalTypes, that.originalTypes);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, originalTypes);
    }

    /**
     * Deserializes column information from an XContent parser.
     * <p>
     * This method is used to parse column metadata from REST API responses.
     * </p>
     *
     * @param parser the XContent parser to read from
     * @return a ColumnInfo instance parsed from the XContent
     */
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

    /**
     * Constructs column information from string-based type name (used by XContent parser).
     *
     * @param name the column name
     * @param type the type name as a string (Elasticsearch type format)
     * @param originalTypes optional list of original Elasticsearch types when there's a conflict or unsupported type
     */
    @ParserConstructor
    public ColumnInfoImpl(String name, String type, @Nullable List<String> originalTypes) {
        this(name, DataType.fromEs(type), originalTypes);
    }

    /**
     * Constructs column information with a data type object.
     * <p>
     * This is the primary constructor that initializes all fields including the suggested cast
     * based on the original types.
     * </p>
     *
     * @param name the column name
     * @param type the data type of the column
     * @param originalTypes optional list of original Elasticsearch types when there's a conflict or unsupported type
     */
    public ColumnInfoImpl(String name, DataType type, @Nullable List<String> originalTypes) {
        this.name = name;
        this.type = type;
        this.originalTypes = originalTypes;
        this.suggestedCast = calculateSuggestedCast(this.originalTypes);
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
     * Constructs column information by deserializing from a stream input.
     * <p>
     * This constructor handles backward compatibility by checking the transport version
     * to determine whether to read the originalTypes field.
     * </p>
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs during deserialization
     */
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
    }

    /**
     * Serializes this column information to a stream output.
     * <p>
     * This method handles backward compatibility by checking the transport version
     * to determine whether to write the originalTypes field.
     * </p>
     *
     * @param out the stream output to write to
     * @throws IOException if an I/O error occurs during serialization
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(type.outputType());
        if (out.getTransportVersion().supports(ESQL_REPORT_ORIGINAL_TYPES)) {
            out.writeOptionalStringCollection(originalTypes);
        }
    }

    /**
     * Serializes this column information to XContent format.
     * <p>
     * The output includes:
     * <ul>
     *   <li>name - the column name</li>
     *   <li>type - the output type name</li>
     *   <li>original_types - (optional) list of original ES types if present</li>
     *   <li>suggested_cast - (optional) suggested cast type if applicable</li>
     * </ul>
     * </p>
     *
     * @param builder the XContent builder to write to
     * @param params the serialization parameters
     * @return the XContent builder for method chaining
     * @throws IOException if an I/O error occurs during serialization
     */
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
        builder.endObject();
        return builder;
    }

    /**
     * Returns the column name.
     *
     * @return the name of the column
     */
    @Override
    public String name() {
        return name;
    }

    /**
     * Returns the output type name for this column.
     * <p>
     * This is the type name as it should be displayed in API responses.
     * </p>
     *
     * @return the output type name
     */
    @Override
    public String outputType() {
        return type.outputType();
    }

    /**
     * Returns the data type of this column.
     *
     * @return the DataType enum value for this column
     */
    public DataType type() {
        return type;
    }

    /**
     * Returns the list of original Elasticsearch types, if present.
     * <p>
     * This field is populated when:
     * <ul>
     *   <li>The column has an unsupported type</li>
     *   <li>There's a type conflict across multiple indices</li>
     * </ul>
     * </p>
     *
     * @return the list of original type names, or null if not applicable
     */
    @Nullable
    public List<String> originalTypes() {
        return originalTypes;
    }

    public String toString() {
        return "ColumnInfoImpl{" + "name='" + name + '\'' + ", type=" + type + ", originalTypes=" + originalTypes + '}';
    }
}
