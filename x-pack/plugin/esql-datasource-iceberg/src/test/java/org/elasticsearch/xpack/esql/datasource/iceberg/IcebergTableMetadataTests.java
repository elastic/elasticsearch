/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.iceberg;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;

/**
 * Unit tests for IcebergTableMetadata.
 * Tests schema conversion from Iceberg types to ESQL DataTypes and metadata accessors.
 */
public class IcebergTableMetadataTests extends ESTestCase {

    public void testBooleanTypeMapping() {
        Schema schema = new Schema(Types.NestedField.required(1, "active", Types.BooleanType.get()));
        IcebergTableMetadata metadata = new IcebergTableMetadata("s3://bucket/table", schema, null, "iceberg");

        List<Attribute> attributes = metadata.attributes();
        assertEquals(1, attributes.size());
        assertEquals("active", attributes.get(0).name());
        assertEquals(DataType.BOOLEAN, attributes.get(0).dataType());
    }

    public void testIntegerTypeMapping() {
        Schema schema = new Schema(Types.NestedField.required(1, "count", Types.IntegerType.get()));
        IcebergTableMetadata metadata = new IcebergTableMetadata("s3://bucket/table", schema, null, "iceberg");

        List<Attribute> attributes = metadata.attributes();
        assertEquals(1, attributes.size());
        assertEquals("count", attributes.get(0).name());
        assertEquals(DataType.INTEGER, attributes.get(0).dataType());
    }

    public void testLongTypeMapping() {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
        IcebergTableMetadata metadata = new IcebergTableMetadata("s3://bucket/table", schema, null, "iceberg");

        List<Attribute> attributes = metadata.attributes();
        assertEquals(1, attributes.size());
        assertEquals("id", attributes.get(0).name());
        assertEquals(DataType.LONG, attributes.get(0).dataType());
    }

    public void testFloatTypeMapping() {
        Schema schema = new Schema(Types.NestedField.required(1, "temperature", Types.FloatType.get()));
        IcebergTableMetadata metadata = new IcebergTableMetadata("s3://bucket/table", schema, null, "iceberg");

        List<Attribute> attributes = metadata.attributes();
        assertEquals(1, attributes.size());
        assertEquals("temperature", attributes.get(0).name());
        assertEquals(DataType.DOUBLE, attributes.get(0).dataType()); // Float maps to DOUBLE
    }

    public void testDoubleTypeMapping() {
        Schema schema = new Schema(Types.NestedField.required(1, "score", Types.DoubleType.get()));
        IcebergTableMetadata metadata = new IcebergTableMetadata("s3://bucket/table", schema, null, "iceberg");

        List<Attribute> attributes = metadata.attributes();
        assertEquals(1, attributes.size());
        assertEquals("score", attributes.get(0).name());
        assertEquals(DataType.DOUBLE, attributes.get(0).dataType());
    }

    public void testStringTypeMapping() {
        Schema schema = new Schema(Types.NestedField.required(1, "name", Types.StringType.get()));
        IcebergTableMetadata metadata = new IcebergTableMetadata("s3://bucket/table", schema, null, "iceberg");

        List<Attribute> attributes = metadata.attributes();
        assertEquals(1, attributes.size());
        assertEquals("name", attributes.get(0).name());
        assertEquals(DataType.KEYWORD, attributes.get(0).dataType());
    }

    public void testTimestampTypeMapping() {
        Schema schema = new Schema(Types.NestedField.required(1, "created_at", Types.TimestampType.withoutZone()));
        IcebergTableMetadata metadata = new IcebergTableMetadata("s3://bucket/table", schema, null, "iceberg");

        List<Attribute> attributes = metadata.attributes();
        assertEquals(1, attributes.size());
        assertEquals("created_at", attributes.get(0).name());
        assertEquals(DataType.DATETIME, attributes.get(0).dataType());
    }

    public void testDateTypeMapping() {
        Schema schema = new Schema(Types.NestedField.required(1, "birth_date", Types.DateType.get()));
        IcebergTableMetadata metadata = new IcebergTableMetadata("s3://bucket/table", schema, null, "iceberg");

        List<Attribute> attributes = metadata.attributes();
        assertEquals(1, attributes.size());
        assertEquals("birth_date", attributes.get(0).name());
        assertEquals(DataType.DATETIME, attributes.get(0).dataType());
    }

    public void testBinaryTypeMapping() {
        Schema schema = new Schema(Types.NestedField.required(1, "data", Types.BinaryType.get()));
        IcebergTableMetadata metadata = new IcebergTableMetadata("s3://bucket/table", schema, null, "iceberg");

        List<Attribute> attributes = metadata.attributes();
        assertEquals(1, attributes.size());
        assertEquals("data", attributes.get(0).name());
        assertEquals(DataType.KEYWORD, attributes.get(0).dataType());
    }

    public void testDecimalTypeMapping() {
        Schema schema = new Schema(Types.NestedField.required(1, "price", Types.DecimalType.of(10, 2)));
        IcebergTableMetadata metadata = new IcebergTableMetadata("s3://bucket/table", schema, null, "iceberg");

        List<Attribute> attributes = metadata.attributes();
        assertEquals(1, attributes.size());
        assertEquals("price", attributes.get(0).name());
        assertEquals(DataType.DOUBLE, attributes.get(0).dataType()); // Decimal maps to DOUBLE
    }

    public void testListTypeMapping() {
        // List of integers - should map to INTEGER (element type)
        Schema schema = new Schema(Types.NestedField.required(1, "scores", Types.ListType.ofRequired(2, Types.IntegerType.get())));
        IcebergTableMetadata metadata = new IcebergTableMetadata("s3://bucket/table", schema, null, "iceberg");

        List<Attribute> attributes = metadata.attributes();
        assertEquals(1, attributes.size());
        assertEquals("scores", attributes.get(0).name());
        assertEquals(DataType.INTEGER, attributes.get(0).dataType()); // Element type
    }

    public void testListOfStringsTypeMapping() {
        Schema schema = new Schema(Types.NestedField.required(1, "tags", Types.ListType.ofRequired(2, Types.StringType.get())));
        IcebergTableMetadata metadata = new IcebergTableMetadata("s3://bucket/table", schema, null, "iceberg");

        List<Attribute> attributes = metadata.attributes();
        assertEquals(1, attributes.size());
        assertEquals("tags", attributes.get(0).name());
        assertEquals(DataType.KEYWORD, attributes.get(0).dataType());
    }

    public void testMapTypeReturnsUnsupported() {
        Schema schema = new Schema(
            Types.NestedField.required(1, "properties", Types.MapType.ofRequired(2, 3, Types.StringType.get(), Types.StringType.get()))
        );
        IcebergTableMetadata metadata = new IcebergTableMetadata("s3://bucket/table", schema, null, "iceberg");

        // Maps return UNSUPPORTED, so no attributes are added
        List<Attribute> attributes = metadata.attributes();
        assertEquals(0, attributes.size());
    }

    public void testStructTypeReturnsUnsupported() {
        Schema schema = new Schema(
            Types.NestedField.required(
                1,
                "address",
                Types.StructType.of(
                    Types.NestedField.required(2, "street", Types.StringType.get()),
                    Types.NestedField.required(3, "city", Types.StringType.get())
                )
            )
        );
        IcebergTableMetadata metadata = new IcebergTableMetadata("s3://bucket/table", schema, null, "iceberg");

        // Structs return UNSUPPORTED, so no attributes are added
        List<Attribute> attributes = metadata.attributes();
        assertEquals(0, attributes.size());
    }

    public void testMultipleColumns() {
        Schema schema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "active", Types.BooleanType.get()),
            Types.NestedField.required(4, "score", Types.DoubleType.get())
        );
        IcebergTableMetadata metadata = new IcebergTableMetadata("s3://bucket/table", schema, null, "iceberg");

        List<Attribute> attributes = metadata.attributes();
        assertEquals(4, attributes.size());

        assertEquals("id", attributes.get(0).name());
        assertEquals(DataType.LONG, attributes.get(0).dataType());

        assertEquals("name", attributes.get(1).name());
        assertEquals(DataType.KEYWORD, attributes.get(1).dataType());

        assertEquals("active", attributes.get(2).name());
        assertEquals(DataType.BOOLEAN, attributes.get(2).dataType());

        assertEquals("score", attributes.get(3).name());
        assertEquals(DataType.DOUBLE, attributes.get(3).dataType());
    }

    public void testTablePathAccessor() {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
        String tablePath = "s3://my-bucket/my-table";
        IcebergTableMetadata metadata = new IcebergTableMetadata(tablePath, schema, null, "iceberg");

        assertEquals(tablePath, metadata.tablePath());
        assertEquals(tablePath, metadata.location());
    }

    public void testSourceTypeAccessor() {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
        IcebergTableMetadata metadata = new IcebergTableMetadata("s3://bucket/table", schema, null, "iceberg");

        assertEquals("iceberg", metadata.sourceType());
    }

    public void testIcebergSchemaAccessor() {
        Schema schema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get())
        );
        IcebergTableMetadata metadata = new IcebergTableMetadata("s3://bucket/table", schema, null, "iceberg");

        assertSame(schema, metadata.icebergSchema());
    }

    public void testSchemaAccessor() {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
        IcebergTableMetadata metadata = new IcebergTableMetadata("s3://bucket/table", schema, null, "iceberg");

        assertSame(metadata.attributes(), metadata.schema());
    }

    public void testS3ConfigAccessor() {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
        S3Configuration s3Config = S3Configuration.fromFields("accessKey", "secretKey", "endpoint", "us-east-1");
        IcebergTableMetadata metadata = new IcebergTableMetadata("s3://bucket/table", schema, s3Config, "iceberg");

        assertSame(s3Config, metadata.s3Config());
    }

    public void testMetadataLocationAccessor() {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
        String metadataLocation = "s3://bucket/table/metadata/v1.metadata.json";
        IcebergTableMetadata metadata = new IcebergTableMetadata("s3://bucket/table", schema, null, "iceberg", metadataLocation);

        assertEquals(metadataLocation, metadata.metadataLocation());
    }

    public void testMetadataLocationNullByDefault() {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
        IcebergTableMetadata metadata = new IcebergTableMetadata("s3://bucket/table", schema, null, "iceberg");

        assertNull(metadata.metadataLocation());
    }

    public void testEqualsAndHashCode() {
        Schema schema1 = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
        Schema schema2 = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

        IcebergTableMetadata metadata1 = new IcebergTableMetadata("s3://bucket/table", schema1, null, "iceberg");
        IcebergTableMetadata metadata2 = new IcebergTableMetadata("s3://bucket/table", schema2, null, "iceberg");

        assertEquals(metadata1, metadata2);
        assertEquals(metadata1.hashCode(), metadata2.hashCode());
    }

    public void testNotEqualsDifferentPath() {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

        IcebergTableMetadata metadata1 = new IcebergTableMetadata("s3://bucket/table1", schema, null, "iceberg");
        IcebergTableMetadata metadata2 = new IcebergTableMetadata("s3://bucket/table2", schema, null, "iceberg");

        assertNotEquals(metadata1, metadata2);
    }

    public void testNotEqualsDifferentSourceType() {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

        IcebergTableMetadata metadata1 = new IcebergTableMetadata("s3://bucket/table", schema, null, "iceberg");
        IcebergTableMetadata metadata2 = new IcebergTableMetadata("s3://bucket/table", schema, null, "parquet");

        assertNotEquals(metadata1, metadata2);
    }

    public void testToString() {
        Schema schema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get())
        );
        IcebergTableMetadata metadata = new IcebergTableMetadata("s3://bucket/table", schema, null, "iceberg");

        String toString = metadata.toString();
        assertTrue(toString.contains("s3://bucket/table"));
        assertTrue(toString.contains("iceberg"));
        assertTrue(toString.contains("2")); // fields count
    }
}
