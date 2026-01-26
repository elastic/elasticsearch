/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.analysis;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.datasources.datalake.iceberg.IcebergTableMetadata;
import org.elasticsearch.xpack.esql.datasources.s3.S3Configuration;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.IcebergRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyPolicyResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;

public class IcebergAnalyzerTests extends ESTestCase {

    @Override
    protected boolean enableWarningsCheck() {
        // Disable warnings check as analyzer adds implicit limit warnings
        return false;
    }

    private Analyzer makeAnalyzer(ExternalSourceResolution externalSourceResolution) {
        var config = EsqlTestUtils.TEST_CFG;
        var context = new AnalyzerContext(
            config,
            new EsqlFunctionRegistry(),
            null,
            Map.of(),
            Map.of(),
            emptyPolicyResolution(),
            null,
            externalSourceResolution,
            org.elasticsearch.TransportVersion.current(),
            UnmappedResolution.FAIL
        );
        return new Analyzer(context, TEST_VERIFIER);
    }

    public void testResolveIcebergRelationWithMetadata() {
        // Create a mock Iceberg schema
        Schema schema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "age", Types.IntegerType.get()),
            Types.NestedField.optional(4, "salary", Types.DoubleType.get())
        );

        String tablePath = "s3://bucket/warehouse/testdb.users";
        IcebergTableMetadata metadata = new IcebergTableMetadata(tablePath, schema, null, "iceberg");

        // Create ExternalSourceResolution with the metadata
        Map<String, ExternalSourceMetadata> resolved = new HashMap<>();
        resolved.put(tablePath, metadata);
        ExternalSourceResolution externalSourceResolution = new ExternalSourceResolution(resolved);

        // Create UnresolvedExternalRelation
        UnresolvedExternalRelation unresolved = new UnresolvedExternalRelation(
            Source.EMPTY,
            new Literal(Source.EMPTY, new org.apache.lucene.util.BytesRef(tablePath), DataType.KEYWORD),
            Map.of()
        );

        // Analyze
        Analyzer analyzer = makeAnalyzer(externalSourceResolution);
        LogicalPlan result = analyzer.analyze(unresolved);

        // Find the IcebergRelation in the tree (may be wrapped by Limit)
        IcebergRelation icebergRelation = findIcebergRelation(result);
        assertNotNull("Expected to find IcebergRelation in plan tree", icebergRelation);
        
        assertEquals(tablePath, icebergRelation.tablePath());
        assertEquals(metadata, icebergRelation.metadata());

        // Verify attributes
        List<Attribute> attrs = icebergRelation.output();
        assertEquals(4, attrs.size());

        // Check attribute names and types
        assertEquals("id", attrs.get(0).name());
        assertEquals(DataType.LONG, attrs.get(0).dataType());

        assertEquals("name", attrs.get(1).name());
        assertEquals(DataType.KEYWORD, attrs.get(1).dataType());

        assertEquals("age", attrs.get(2).name());
        assertEquals(DataType.INTEGER, attrs.get(2).dataType());

        assertEquals("salary", attrs.get(3).name());
        assertEquals(DataType.DOUBLE, attrs.get(3).dataType());
    }

    private IcebergRelation findIcebergRelation(LogicalPlan plan) {
        if (plan instanceof IcebergRelation) {
            return (IcebergRelation) plan;
        }
        for (LogicalPlan child : plan.children()) {
            IcebergRelation found = findIcebergRelation(child);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    public void testUnresolvedExternalRelationStaysUnresolvedWithoutMetadata() {
        // Create empty ExternalSourceResolution (no metadata)
        ExternalSourceResolution externalSourceResolution = ExternalSourceResolution.EMPTY;

        String tablePath = "s3://bucket/warehouse/testdb.users";

        // Create UnresolvedExternalRelation
        UnresolvedExternalRelation unresolved = new UnresolvedExternalRelation(
            Source.EMPTY,
            new Literal(Source.EMPTY, new org.apache.lucene.util.BytesRef(tablePath), DataType.KEYWORD),
            Map.of()
        );

        // Analyze - should throw VerificationException because metadata is not available
        Analyzer analyzer = makeAnalyzer(externalSourceResolution);
        org.elasticsearch.xpack.esql.VerificationException exception = expectThrows(
            org.elasticsearch.xpack.esql.VerificationException.class,
            () -> analyzer.analyze(unresolved)
        );
        
        // Verify the error message mentions the unknown table
        assertTrue(
            "Expected error message to mention unknown Iceberg table",
            exception.getMessage().contains("Unknown Iceberg table") || exception.getMessage().contains(tablePath)
        );
    }

    public void testResolveIcebergRelationWithParquetFile() {
        // Create a mock Parquet schema
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()), Types.NestedField.optional(2, "value", Types.DoubleType.get()));

        String tablePath = "s3://bucket/data/file.parquet";
        IcebergTableMetadata metadata = new IcebergTableMetadata(tablePath, schema, null, "parquet");

        // Create ExternalSourceResolution with the metadata
        Map<String, ExternalSourceMetadata> resolved = new HashMap<>();
        resolved.put(tablePath, metadata);
        ExternalSourceResolution externalSourceResolution = new ExternalSourceResolution(resolved);

        // Create UnresolvedExternalRelation
        UnresolvedExternalRelation unresolved = new UnresolvedExternalRelation(
            Source.EMPTY,
            new Literal(Source.EMPTY, new org.apache.lucene.util.BytesRef(tablePath), DataType.KEYWORD),
            Map.of()
        );

        // Analyze
        Analyzer analyzer = makeAnalyzer(externalSourceResolution);
        LogicalPlan result = analyzer.analyze(unresolved);

        // Find the IcebergRelation in the tree (may be wrapped by Limit)
        IcebergRelation icebergRelation = findIcebergRelation(result);
        assertNotNull("Expected to find IcebergRelation in plan tree", icebergRelation);
        
        assertEquals(tablePath, icebergRelation.tablePath());
        assertEquals("parquet", icebergRelation.metadata().sourceType());

        // Verify attributes
        List<Attribute> attrs = icebergRelation.output();
        assertEquals(2, attrs.size());
        assertEquals("id", attrs.get(0).name());
        assertEquals("value", attrs.get(1).name());
    }
}
