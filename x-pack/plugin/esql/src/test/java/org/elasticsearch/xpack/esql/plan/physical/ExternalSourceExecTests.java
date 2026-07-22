/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;

import java.util.List;
import java.util.Map;

/**
 * Focused tests for {@link ExternalSourceExec#withAttributes(List)}: the attribute swap used by
 * {@code InsertExternalFieldExtraction} to narrow the source's projection while preserving every
 * other transient field (pushed filter, pushed limit, splits, etc.).
 */
public class ExternalSourceExecTests extends ESTestCase {

    public void testWithAttributesPreservesAllOtherFields() {
        FieldAttribute keep = field("keep", DataType.LONG);
        FieldAttribute drop = field("drop", DataType.INTEGER);
        FieldAttribute another = field("other", DataType.KEYWORD);
        ExternalSourceExec original = new ExternalSourceExec(
            Source.EMPTY,
            "s3://bucket/file.parquet",
            "parquet",
            List.of(keep, drop, another),
            Map.of("endpoint", "https://example.com"),
            Map.of("schema_version", 1),
            null,
            42 // estimatedRowSize
        );

        MetadataAttribute rowPosition = new MetadataAttribute(Source.EMPTY, ColumnExtractor.ROW_POSITION_COLUMN, DataType.LONG, false);
        List<Attribute> narrowed = List.of(keep, rowPosition);
        ExternalSourceExec rewritten = original.withAttributes(narrowed);

        assertNotSame(original, rewritten);
        assertEquals(narrowed, rewritten.output());
        assertEquals(original.sourcePath(), rewritten.sourcePath());
        assertEquals(original.sourceType(), rewritten.sourceType());
        assertEquals(original.config(), rewritten.config());
        assertEquals(original.sourceMetadata(), rewritten.sourceMetadata());
        assertSame(original.pushedFilter(), rewritten.pushedFilter());
        assertEquals(original.pushedExpressions(), rewritten.pushedExpressions());
        assertEquals(original.pushedLimit(), rewritten.pushedLimit());
        assertEquals(original.estimatedRowSize(), rewritten.estimatedRowSize());
        assertEquals(original.fileList(), rewritten.fileList());
        assertEquals(original.splits(), rewritten.splits());
        // Original must remain unchanged — withAttributes returns a new node, never mutates.
        assertEquals(List.of(keep, drop, another), original.output());
    }

    public void testWithAttributesAcceptsEmptyList() {
        // Edge case: narrowing to an empty list (only meaningful when the optimizer immediately
        // appends _rowPosition, but the constructor must accept the call without throwing).
        ExternalSourceExec original = new ExternalSourceExec(
            Source.EMPTY,
            "file:///x.parquet",
            "parquet",
            List.of(field("a", DataType.LONG)),
            Map.of(),
            Map.of(),
            null,
            10
        );
        ExternalSourceExec narrowed = original.withAttributes(List.of());
        assertEquals(List.of(), narrowed.output());
    }

    public void testNodePropertiesOmitsSecretsSecureStringPath() {
        // Dataset path: config map carries SecureString values.
        Map<String, Object> secretConfig = Map.of("secret_key", new SecureString("S3CR3T_DO_NOT_LEAK_SecureString".toCharArray()));
        Map<String, Object> secretSourceMetadata = Map.of("token", new SecureString("TOKEN_DO_NOT_LEAK".toCharArray()));
        ExternalSourceExec exec = new ExternalSourceExec(
            Source.EMPTY,
            "s3://bucket/file.parquet",
            "parquet",
            List.of(field("id", DataType.LONG)),
            secretConfig,
            secretSourceMetadata,
            null,
            42
        );

        assertFalse("nodeProperties() must not contain the config map", exec.nodeProperties().contains(secretConfig));
        assertFalse("nodeProperties() must not contain the sourceMetadata map", exec.nodeProperties().contains(secretSourceMetadata));
        String rendered = exec.nodeString() + " " + exec.toString();
        assertFalse("EXPLAIN output must not contain the secret value", rendered.contains("S3CR3T_DO_NOT_LEAK_SecureString"));
        assertFalse("EXPLAIN output must not contain the token value", rendered.contains("TOKEN_DO_NOT_LEAK"));
    }

    public void testNodePropertiesOmitsSecretsPlainStringPath() {
        // Inline EXTERNAL path: config map carries plain String values (foldOptionLiterals output).
        Map<String, Object> secretConfig = Map.of("secret_key", "PLAINTEXT_DO_NOT_LEAK_String");
        ExternalSourceExec exec = new ExternalSourceExec(
            Source.EMPTY,
            "s3://bucket/file.parquet",
            "parquet",
            List.of(field("id", DataType.LONG)),
            secretConfig,
            Map.of(),
            null,
            null
        );

        assertFalse("nodeProperties() must not contain the config map", exec.nodeProperties().contains(secretConfig));
        String rendered = exec.nodeString() + " " + exec.toString();
        assertFalse("EXPLAIN output must not contain the secret value", rendered.contains("PLAINTEXT_DO_NOT_LEAK_String"));
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }
}
