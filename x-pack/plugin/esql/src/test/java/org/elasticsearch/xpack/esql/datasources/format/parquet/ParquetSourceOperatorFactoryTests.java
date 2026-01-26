/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.format.parquet;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.s3.S3Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link ParquetSourceOperatorFactory}.
 * 
 * Note: These tests verify factory construction and configuration.
 * Full integration tests with real Parquet files are in integration test suites.
 */
public class ParquetSourceOperatorFactoryTests extends ESTestCase {

    public void testFactoryConstruction() {
        // Create test components
        Executor executor = Runnable::run; // Direct execution for testing
        String filePath = "s3://test-bucket/data/test.parquet";
        S3Configuration s3Config = S3Configuration.fromFields("test-key", "test-secret", null, null);
        
        // Create a simple Iceberg schema (derived from Parquet schema)
        Schema schema = new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get())
        );
        
        // Create ESQL attributes
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new FieldAttribute(Source.EMPTY, "id", new EsField("id", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE)));
        attributes.add(new FieldAttribute(Source.EMPTY, "name", new EsField("name", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)));
        
        int pageSize = 1000;
        int maxBufferSize = 10;
        
        // Create factory
        ParquetSourceOperatorFactory factory = new ParquetSourceOperatorFactory(
            executor,
            filePath,
            s3Config,
            schema,
            attributes,
            pageSize,
            maxBufferSize
        );
        
        // Verify factory describes itself
        String description = factory.describe();
        assertTrue(description.contains("ParquetSourceOperator"));
        assertTrue(description.contains("pageSize=1000"));
        assertTrue(description.contains("bufferSize=10"));
    }
    
    public void testFactoryDescriptionContainsFilePath() {
        // Create test components with minimal configuration
        Executor executor = Runnable::run;
        String filePath = "s3://my-bucket/warehouse/data.parquet";
        S3Configuration s3Config = S3Configuration.fromFields("test-key", "test-secret", null, null);
        
        Schema schema = new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get())
        );
        
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new FieldAttribute(Source.EMPTY, "id", new EsField("id", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE)));
        
        ParquetSourceOperatorFactory factory = new ParquetSourceOperatorFactory(
            executor,
            filePath,
            s3Config,
            schema,
            attributes,
            1000,
            10
        );
        
        // Verify description contains the file path
        String description = factory.describe();
        assertTrue(description.contains("s3://my-bucket/warehouse/data.parquet"));
    }
    
    public void testFactoryWithDifferentPageSizes() {
        Executor executor = Runnable::run;
        String filePath = "s3://test-bucket/data/test.parquet";
        S3Configuration s3Config = S3Configuration.fromFields("test-key", "test-secret", null, null);
        
        Schema schema = new Schema(
            Types.NestedField.optional(1, "value", Types.DoubleType.get())
        );
        
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new FieldAttribute(Source.EMPTY, "value", new EsField("value", DataType.DOUBLE, Map.of(), true, EsField.TimeSeriesFieldType.NONE)));
        
        // Test with different page sizes
        int[] pageSizes = {100, 500, 1000, 5000};
        
        for (int pageSize : pageSizes) {
            ParquetSourceOperatorFactory factory = new ParquetSourceOperatorFactory(
                executor,
                filePath,
                s3Config,
                schema,
                attributes,
                pageSize,
                10
            );
            
            String description = factory.describe();
            assertTrue("Description should contain pageSize=" + pageSize, 
                description.contains("pageSize=" + pageSize));
        }
    }
}
