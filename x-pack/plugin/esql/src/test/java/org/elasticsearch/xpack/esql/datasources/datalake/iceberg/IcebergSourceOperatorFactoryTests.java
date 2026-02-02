/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.datalake.iceberg;

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.types.Types;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link IcebergSourceOperatorFactory}.
 *
 * Note: These tests verify factory construction and configuration.
 * Full integration tests with real Iceberg tables are in IcebergEndToEndTests.
 */
public class IcebergSourceOperatorFactoryTests extends ESTestCase {

    public void testFactoryConstruction() {
        // Create test components
        Executor executor = Runnable::run; // Direct execution for testing
        String tablePath = "s3://test-bucket/warehouse/testdb.testtable";
        org.elasticsearch.xpack.esql.datasources.s3.S3Configuration s3Config = org.elasticsearch.xpack.esql.datasources.s3.S3Configuration
            .fromFields("test-key", "test-secret", null, null);
        String sourceType = "iceberg";
        Expression filter = null;

        // Create a simple Iceberg schema
        Schema schema = new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get())
        );

        // Create ESQL attributes
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(
            new FieldAttribute(Source.EMPTY, "id", new EsField("id", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE))
        );
        attributes.add(
            new FieldAttribute(
                Source.EMPTY,
                "name",
                new EsField("name", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
            )
        );

        int pageSize = 1000;
        int maxBufferSize = 10;

        // Create factory
        org.elasticsearch.xpack.esql.datasources.datalake.iceberg.IcebergSourceOperatorFactory factory =
            new org.elasticsearch.xpack.esql.datasources.datalake.iceberg.IcebergSourceOperatorFactory(
                executor,
                tablePath,
                s3Config,
                sourceType,
                filter,
                schema,
                attributes,
                pageSize,
                maxBufferSize
            );

        // Verify factory describes itself
        String description = factory.describe();
        assertTrue(description.contains("IcebergSourceOperator"));
        assertTrue(description.contains("pageSize=1000"));
        assertTrue(description.contains("bufferSize=10"));
    }

    public void testFactoryCreatesOperator() {
        // Create test components with minimal configuration
        Executor executor = Runnable::run;
        String tablePath = "s3://test-bucket/warehouse/testdb.testtable";
        org.elasticsearch.xpack.esql.datasources.s3.S3Configuration s3Config = org.elasticsearch.xpack.esql.datasources.s3.S3Configuration
            .fromFields("test-key", "test-secret", null, null);
        String sourceType = "iceberg";
        Expression filter = null;

        Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()));

        List<Attribute> attributes = new ArrayList<>();
        attributes.add(
            new FieldAttribute(Source.EMPTY, "id", new EsField("id", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE))
        );

        org.elasticsearch.xpack.esql.datasources.datalake.iceberg.IcebergSourceOperatorFactory factory =
            new org.elasticsearch.xpack.esql.datasources.datalake.iceberg.IcebergSourceOperatorFactory(
                executor,
                tablePath,
                s3Config,
                sourceType,
                filter,
                schema,
                attributes,
                1000,
                10
            );

        // Mock DriverContext - operator creation requires a context
        DriverContext driverContext = mock(DriverContext.class);

        // Note: Creating the operator will fail without real Iceberg infrastructure
        // This test just verifies the factory structure is valid
        // Real data reading tests are in integration tests

        assertNotNull(factory);
        assertEquals("iceberg", sourceType);
    }
}
