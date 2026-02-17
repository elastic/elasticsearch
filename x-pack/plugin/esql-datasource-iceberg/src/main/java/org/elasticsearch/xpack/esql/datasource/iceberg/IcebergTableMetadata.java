/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasource.iceberg;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Metadata for an Iceberg table or Parquet file.
 * Contains schema information resolved from Iceberg/Parquet metadata.
 */
public class IcebergTableMetadata implements ExternalSourceMetadata {

    private final String tablePath;
    private final Schema schema;
    private final List<Attribute> attributes;
    private final S3Configuration s3Config;
    private final String sourceType;
    private final String metadataLocation; // For Iceberg tables, stores the metadata file location

    public IcebergTableMetadata(String tablePath, Schema schema, S3Configuration s3Config, String sourceType) {
        this(tablePath, schema, s3Config, sourceType, null);
    }

    public IcebergTableMetadata(String tablePath, Schema schema, S3Configuration s3Config, String sourceType, String metadataLocation) {
        Check.notNull(tablePath, "tablePath must not be null");
        Check.notNull(schema, "schema must not be null");
        Check.notNull(sourceType, "sourceType must not be null");
        this.tablePath = tablePath;
        this.schema = schema;
        this.s3Config = s3Config;
        this.sourceType = sourceType;
        this.metadataLocation = metadataLocation;
        this.attributes = buildAttributes();
    }

    private List<Attribute> buildAttributes() {
        List<Attribute> attrs = new ArrayList<>();
        for (Types.NestedField field : schema.columns()) {
            DataType esqlType = mapIcebergTypeToEsql(field.type());
            // Skip unsupported types (MAP, STRUCT, etc.)
            if (esqlType != null && esqlType != DataType.UNSUPPORTED) {
                attrs.add(new ReferenceAttribute(Source.EMPTY, field.name(), esqlType));
            }
        }
        return attrs;
    }

    /**
     * Map Iceberg/Parquet types to ESQL DataTypes.
     * Basic type mapping - can be extended for more complex types.
     * <p>
     * For LIST types, returns the element type since ESQL handles multi-values implicitly.
     * This allows multi-value fields in Parquet to be queried naturally in ESQL.
     */
    private static DataType mapIcebergTypeToEsql(Type icebergType) {
        if (icebergType.isPrimitiveType()) {
            return mapPrimitiveType(icebergType.asPrimitiveType());
        }

        // Handle LIST types - extract element type for multi-value fields
        if (icebergType.typeId() == Type.TypeID.LIST) {
            Types.ListType listType = (Types.ListType) icebergType;
            Type elementType = listType.elementType();
            // Recursively map the element type (handles nested lists and primitive elements)
            return mapIcebergTypeToEsql(elementType);
        }

        // For other complex types (MAP, STRUCT), return UNSUPPORTED for now
        return DataType.UNSUPPORTED;
    }

    /**
     * Map Iceberg primitive types to ESQL DataTypes.
     */
    private static DataType mapPrimitiveType(Type.PrimitiveType primitiveType) {
        switch (primitiveType.typeId()) {
            case BOOLEAN:
                return DataType.BOOLEAN;
            case INTEGER:
                return DataType.INTEGER;
            case LONG:
                return DataType.LONG;
            case FLOAT:
                return DataType.DOUBLE; // ESQL uses DOUBLE for float types
            case DOUBLE:
                return DataType.DOUBLE;
            case STRING:
                return DataType.KEYWORD;
            case TIMESTAMP:
                return DataType.DATETIME;
            case DATE:
                return DataType.DATETIME;
            case BINARY:
            case FIXED:
                // Binary types could map to KEYWORD for now
                return DataType.KEYWORD;
            case DECIMAL:
                return DataType.DOUBLE; // Simplified mapping - decimals converted to doubles
            default:
                return DataType.UNSUPPORTED;
        }
    }

    @Override
    public String tablePath() {
        return tablePath;
    }

    @Override
    public List<Attribute> attributes() {
        return attributes;
    }

    @Override
    public String sourceType() {
        return sourceType;
    }

    /**
     * Returns the Iceberg schema for this table.
     * This is the native Iceberg schema, not the ESQL schema.
     */
    public Schema icebergSchema() {
        return schema;
    }

    @Override
    public List<Attribute> schema() {
        return attributes;
    }

    @Override
    public String location() {
        return tablePath;
    }

    public S3Configuration s3Config() {
        return s3Config;
    }

    public String metadataLocation() {
        return metadataLocation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IcebergTableMetadata that = (IcebergTableMetadata) o;
        // Compare schema by structure (sameSchema) rather than object identity
        return Objects.equals(tablePath, that.tablePath) && schema.sameSchema(that.schema) && Objects.equals(sourceType, that.sourceType);
    }

    @Override
    public int hashCode() {
        // Use schema's schemaId for hash code since sameSchema compares by structure
        return Objects.hash(tablePath, schema.schemaId(), sourceType);
    }

    @Override
    public String toString() {
        return "IcebergTableMetadata{tablePath='" + tablePath + "', sourceType='" + sourceType + "', fields=" + attributes.size() + "}";
    }
}
