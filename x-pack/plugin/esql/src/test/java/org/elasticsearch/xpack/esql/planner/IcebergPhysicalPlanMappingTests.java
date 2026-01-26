/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.planner;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.datalake.iceberg.IcebergTableMetadata;
import org.elasticsearch.xpack.esql.datasources.s3.S3Configuration;
import org.elasticsearch.xpack.esql.plan.logical.IcebergRelation;
import org.elasticsearch.xpack.esql.plan.physical.IcebergSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.mapper.LocalMapper;

import java.util.List;

/**
 * Tests for mapping IcebergRelation logical plan to IcebergSourceExec physical plan.
 */
public class IcebergPhysicalPlanMappingTests extends ESTestCase {

    public void testMapIcebergRelationToIcebergSourceExec() {
        // Create a mock Iceberg schema
        Schema schema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "age", Types.IntegerType.get()),
            Types.NestedField.optional(4, "salary", Types.DoubleType.get())
        );

        String tablePath = "s3://bucket/warehouse/testdb.users";
        S3Configuration s3Config = S3Configuration.fromFields("accessKey123", "secretKey123", null, "us-west-2");
        IcebergTableMetadata metadata = new IcebergTableMetadata(tablePath, schema, s3Config, "iceberg");

        // Get attributes from metadata (they are built automatically)
        List<Attribute> attributes = metadata.attributes();

        // Create IcebergRelation
        IcebergRelation logicalRelation = new IcebergRelation(Source.EMPTY, tablePath, metadata, attributes);

        // Map to physical plan using LocalMapper
        PhysicalPlan physicalPlan = LocalMapper.INSTANCE.map(logicalRelation);

        // Verify the result is IcebergSourceExec
        assertNotNull(physicalPlan);
        assertTrue("Expected IcebergSourceExec but got " + physicalPlan.getClass(), physicalPlan instanceof IcebergSourceExec);

        IcebergSourceExec sourceExec = (IcebergSourceExec) physicalPlan;

        // Verify properties
        assertEquals(tablePath, sourceExec.tablePath());
        assertEquals("iceberg", sourceExec.sourceType());
        assertEquals(s3Config, sourceExec.s3Config());

        // Verify attributes
        List<Attribute> outputAttrs = sourceExec.output();
        assertEquals(4, outputAttrs.size());
        assertEquals("id", outputAttrs.get(0).name());
        assertEquals(DataType.LONG, outputAttrs.get(0).dataType());
        assertEquals("name", outputAttrs.get(1).name());
        assertEquals(DataType.KEYWORD, outputAttrs.get(1).dataType());
        assertEquals("age", outputAttrs.get(2).name());
        assertEquals(DataType.INTEGER, outputAttrs.get(2).dataType());
        assertEquals("salary", outputAttrs.get(3).name());
        assertEquals(DataType.DOUBLE, outputAttrs.get(3).dataType());
    }

    public void testMapIcebergRelationForParquetFile() {
        // Create a mock Parquet schema
        Schema schema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "value", Types.DoubleType.get())
        );

        String tablePath = "s3://bucket/data/file.parquet";
        S3Configuration s3Config = null; // Using default AWS credentials
        IcebergTableMetadata metadata = new IcebergTableMetadata(tablePath, schema, s3Config, "parquet");

        // Get attributes from metadata
        List<Attribute> attributes = metadata.attributes();

        // Create IcebergRelation
        IcebergRelation logicalRelation = new IcebergRelation(Source.EMPTY, tablePath, metadata, attributes);

        // Map to physical plan using LocalMapper
        PhysicalPlan physicalPlan = LocalMapper.INSTANCE.map(logicalRelation);

        // Verify the result is IcebergSourceExec
        assertTrue(physicalPlan instanceof IcebergSourceExec);

        IcebergSourceExec sourceExec = (IcebergSourceExec) physicalPlan;

        // Verify properties
        assertEquals(tablePath, sourceExec.tablePath());
        assertEquals("parquet", sourceExec.sourceType());
        assertNull("Expected null S3 config (using default credentials)", sourceExec.s3Config());

        // Verify attributes
        List<Attribute> outputAttrs = sourceExec.output();
        assertEquals(2, outputAttrs.size());
        assertEquals("id", outputAttrs.get(0).name());
        assertEquals("value", outputAttrs.get(1).name());
    }

    public void testIcebergSourceExecConstructorFromRelation() {
        // Create a simple Iceberg schema
        Schema schema = new Schema(Types.NestedField.required(1, "col", Types.StringType.get()));

        String tablePath = "s3://bucket/table";
        S3Configuration s3Config = S3Configuration.fromFields("key", "secret", "http://localhost:9000", "us-east-1");
        IcebergTableMetadata metadata = new IcebergTableMetadata(tablePath, schema, s3Config, "iceberg");

        List<Attribute> attributes = metadata.attributes();

        IcebergRelation relation = new IcebergRelation(Source.EMPTY, tablePath, metadata, attributes);

        // Test the convenience constructor that takes IcebergRelation
        IcebergSourceExec sourceExec = new IcebergSourceExec(relation);

        // Verify all properties are correctly transferred
        assertEquals(tablePath, sourceExec.tablePath());
        assertEquals(s3Config, sourceExec.s3Config());
        assertEquals("iceberg", sourceExec.sourceType());
        assertEquals(attributes, sourceExec.output());
    }

    public void testIcebergSourceExecEquality() {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
        String tablePath = "s3://bucket/table";
        S3Configuration s3Config = S3Configuration.fromFields("key", "secret", null, null);
        IcebergTableMetadata metadata = new IcebergTableMetadata(tablePath, schema, s3Config, "iceberg");
        List<Attribute> attributes = metadata.attributes();

        IcebergSourceExec exec1 = new IcebergSourceExec(Source.EMPTY, tablePath, s3Config, "iceberg", attributes, null, null);
        IcebergSourceExec exec2 = new IcebergSourceExec(Source.EMPTY, tablePath, s3Config, "iceberg", attributes, null, null);

        // Test equality
        assertEquals(exec1, exec2);
        assertEquals(exec1.hashCode(), exec2.hashCode());

        // Test inequality with different table path
        IcebergSourceExec exec3 = new IcebergSourceExec(Source.EMPTY, "s3://other/table", s3Config, "iceberg", attributes, null, null);
        assertNotEquals(exec1, exec3);

        // Test inequality with different source type
        IcebergSourceExec exec4 = new IcebergSourceExec(Source.EMPTY, tablePath, s3Config, "parquet", attributes, null, null);
        assertNotEquals(exec1, exec4);
    }

    public void testIcebergSourceExecNodeString() {
        Schema schema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get())
        );

        String tablePath = "s3://bucket/warehouse/table";
        IcebergTableMetadata metadata = new IcebergTableMetadata(tablePath, schema, null, "iceberg");
        List<Attribute> attributes = metadata.attributes();

        IcebergSourceExec sourceExec = new IcebergSourceExec(Source.EMPTY, tablePath, null, "iceberg", attributes, null, null);

        // Verify node string contains table path, source type, and attributes
        String nodeString = sourceExec.nodeString(Node.NodeStringFormat.FULL);
        assertTrue("Node string should contain table path", nodeString.contains(tablePath));
        assertTrue("Node string should contain source type", nodeString.contains("iceberg"));
    }
}
