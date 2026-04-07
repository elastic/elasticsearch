/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EnumSerializationTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SchemaReconciliationTests extends ESTestCase {

    // === schemaWiden() tests ===

    public void testSchemaWidenSameType() {
        for (DataType type : List.of(
            DataType.INTEGER,
            DataType.LONG,
            DataType.DOUBLE,
            DataType.BOOLEAN,
            DataType.KEYWORD,
            DataType.DATETIME,
            DataType.DATE_NANOS
        )) {
            assertThat(SchemaReconciliation.schemaWiden(type, type), equalTo(type));
        }
    }

    public void testSchemaWidenIntegerToLong() {
        assertThat(SchemaReconciliation.schemaWiden(DataType.INTEGER, DataType.LONG), equalTo(DataType.LONG));
    }

    public void testSchemaWidenIntegerToDouble() {
        assertThat(SchemaReconciliation.schemaWiden(DataType.INTEGER, DataType.DOUBLE), equalTo(DataType.DOUBLE));
    }

    public void testSchemaWidenDatetimeToDateNanos() {
        assertThat(SchemaReconciliation.schemaWiden(DataType.DATETIME, DataType.DATE_NANOS), equalTo(DataType.DATE_NANOS));
    }

    public void testSchemaWidenIsCommutative() {
        assertThat(SchemaReconciliation.schemaWiden(DataType.LONG, DataType.INTEGER), equalTo(DataType.LONG));
        assertThat(SchemaReconciliation.schemaWiden(DataType.DOUBLE, DataType.INTEGER), equalTo(DataType.DOUBLE));
        assertThat(SchemaReconciliation.schemaWiden(DataType.DATE_NANOS, DataType.DATETIME), equalTo(DataType.DATE_NANOS));
    }

    public void testSchemaWidenLongToDoubleRejected() {
        assertThat(SchemaReconciliation.schemaWiden(DataType.LONG, DataType.DOUBLE), nullValue());
    }

    public void testSchemaWidenUnsignedLongRejected() {
        assertThat(SchemaReconciliation.schemaWiden(DataType.UNSIGNED_LONG, DataType.INTEGER), nullValue());
        assertThat(SchemaReconciliation.schemaWiden(DataType.UNSIGNED_LONG, DataType.LONG), nullValue());
        assertThat(SchemaReconciliation.schemaWiden(DataType.UNSIGNED_LONG, DataType.DOUBLE), nullValue());
    }

    public void testSchemaWidenIncompatibleTypes() {
        assertThat(SchemaReconciliation.schemaWiden(DataType.INTEGER, DataType.KEYWORD), nullValue());
        assertThat(SchemaReconciliation.schemaWiden(DataType.KEYWORD, DataType.BOOLEAN), nullValue());
        assertThat(SchemaReconciliation.schemaWiden(DataType.LONG, DataType.KEYWORD), nullValue());
        assertThat(SchemaReconciliation.schemaWiden(DataType.DOUBLE, DataType.KEYWORD), nullValue());
    }

    // === STRICT reconciliation tests ===

    public void testStrictMatchingSchemas() {
        List<Attribute> schema = List.of(attr("id", DataType.INTEGER), attr("name", DataType.KEYWORD));
        StoragePath f1 = path("s3://b/f1.parquet");
        StoragePath f2 = path("s3://b/f2.parquet");

        Map<StoragePath, SourceMetadata> metadata = orderedMap(f1, meta(schema), f2, meta(schema));
        SchemaReconciliation.Result result = SchemaReconciliation.reconcileStrict(f1, metadata);

        assertThat(result.unifiedSchema().size(), equalTo(2));
        assertThat(result.unifiedSchema().get(0).name(), equalTo("id"));
        assertThat(result.unifiedSchema().get(1).name(), equalTo("name"));
        assertThat(result.perFileInfo().size(), equalTo(2));

        SchemaReconciliation.ColumnMapping mapping = result.perFileInfo().get(f1).mapping();
        assertThat(mapping, notNullValue());
        assertTrue(mapping.isIdentity());
    }

    public void testStrictColumnCountMismatch() {
        List<Attribute> schema1 = List.of(attr("id", DataType.INTEGER), attr("name", DataType.KEYWORD));
        List<Attribute> schema2 = List.of(attr("id", DataType.INTEGER));

        StoragePath f1 = path("s3://b/f1.parquet");
        StoragePath f2 = path("s3://b/f2.parquet");

        Map<StoragePath, SourceMetadata> metadata = orderedMap(f1, meta(schema1), f2, meta(schema2));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> SchemaReconciliation.reconcileStrict(f1, metadata));
        assertThat(e.getMessage(), containsString("expected 2 columns"));
        assertThat(e.getMessage(), containsString("found 1 columns"));
        assertThat(e.getMessage(), containsString("f2.parquet"));
        assertThat(e.getMessage(), containsString("union_by_name"));
    }

    public void testStrictTypeMismatch() {
        List<Attribute> schema1 = List.of(attr("salary", DataType.INTEGER));
        List<Attribute> schema2 = List.of(attr("salary", DataType.LONG));

        StoragePath f1 = path("s3://b/f1.parquet");
        StoragePath f2 = path("s3://b/f2.parquet");

        Map<StoragePath, SourceMetadata> metadata = orderedMap(f1, meta(schema1), f2, meta(schema2));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> SchemaReconciliation.reconcileStrict(f1, metadata));
        assertThat(e.getMessage(), containsString("salary"));
        assertThat(e.getMessage(), containsString("long"));
        assertThat(e.getMessage(), containsString("integer"));
    }

    public void testStrictNameMismatch() {
        List<Attribute> schema1 = List.of(attr("id", DataType.INTEGER));
        List<Attribute> schema2 = List.of(attr("key", DataType.INTEGER));

        StoragePath f1 = path("s3://b/f1.parquet");
        StoragePath f2 = path("s3://b/f2.parquet");

        Map<StoragePath, SourceMetadata> metadata = orderedMap(f1, meta(schema1), f2, meta(schema2));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> SchemaReconciliation.reconcileStrict(f1, metadata));
        assertThat(e.getMessage(), containsString("[key]"));
        assertThat(e.getMessage(), containsString("[id]"));
    }

    public void testStrictNullabilityTolerated() {
        List<Attribute> schema1 = List.of(attrNullable("id", DataType.INTEGER, Nullability.TRUE));
        List<Attribute> schema2 = List.of(attrNullable("id", DataType.INTEGER, Nullability.FALSE));

        StoragePath f1 = path("s3://b/f1.parquet");
        StoragePath f2 = path("s3://b/f2.parquet");

        Map<StoragePath, SourceMetadata> metadata = orderedMap(f1, meta(schema1), f2, meta(schema2));
        SchemaReconciliation.Result result = SchemaReconciliation.reconcileStrict(f1, metadata);
        assertThat(result.unifiedSchema().size(), equalTo(1));
    }

    // === UNION_BY_NAME reconciliation tests ===

    public void testUnionByNameIdenticalSchemas() {
        List<Attribute> schema = List.of(attr("id", DataType.INTEGER), attr("name", DataType.KEYWORD));
        StoragePath f1 = path("s3://b/f1.parquet");
        StoragePath f2 = path("s3://b/f2.parquet");

        Map<StoragePath, SourceMetadata> metadata = orderedMap(f1, meta(schema), f2, meta(schema));
        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);

        assertThat(result.unifiedSchema().size(), equalTo(2));
        assertThat(result.unifiedSchema().get(0).name(), equalTo("id"));
        assertThat(result.unifiedSchema().get(1).name(), equalTo("name"));
    }

    public void testUnionByNameAddedColumn() {
        List<Attribute> schema1 = List.of(attr("id", DataType.INTEGER), attr("name", DataType.KEYWORD));
        List<Attribute> schema2 = List.of(attr("id", DataType.INTEGER), attr("name", DataType.KEYWORD), attr("bonus", DataType.DOUBLE));

        StoragePath f1 = path("s3://b/f1.parquet");
        StoragePath f2 = path("s3://b/f2.parquet");

        Map<StoragePath, SourceMetadata> metadata = orderedMap(f1, meta(schema1), f2, meta(schema2));
        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);

        assertThat(result.unifiedSchema().size(), equalTo(3));
        assertThat(result.unifiedSchema().get(2).name(), equalTo("bonus"));
        assertThat(result.unifiedSchema().get(2).nullable(), equalTo(Nullability.TRUE));

        SchemaReconciliation.ColumnMapping mapping1 = result.perFileInfo().get(f1).mapping();
        assertThat(mapping1, notNullValue());
        assertThat(mapping1.localIndex(2), equalTo(-1));

        SchemaReconciliation.ColumnMapping mapping2 = result.perFileInfo().get(f2).mapping();
        assertThat(mapping2, notNullValue());
        assertThat(mapping2.localIndex(2), equalTo(2));
    }

    public void testUnionByNameRemovedColumn() {
        List<Attribute> schema1 = List.of(attr("id", DataType.INTEGER), attr("name", DataType.KEYWORD), attr("dept", DataType.KEYWORD));
        List<Attribute> schema2 = List.of(attr("id", DataType.INTEGER), attr("name", DataType.KEYWORD));

        StoragePath f1 = path("s3://b/f1.parquet");
        StoragePath f2 = path("s3://b/f2.parquet");

        Map<StoragePath, SourceMetadata> metadata = orderedMap(f1, meta(schema1), f2, meta(schema2));
        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);

        assertThat(result.unifiedSchema().size(), equalTo(3));

        SchemaReconciliation.ColumnMapping mapping1 = result.perFileInfo().get(f1).mapping();
        assertTrue(mapping1.isIdentity());

        SchemaReconciliation.ColumnMapping mapping2 = result.perFileInfo().get(f2).mapping();
        assertThat(mapping2.localIndex(2), equalTo(-1));
    }

    public void testUnionByNameTypeWideningIntToLong() {
        List<Attribute> schema1 = List.of(attr("id", DataType.INTEGER));
        List<Attribute> schema2 = List.of(attr("id", DataType.LONG));

        StoragePath f1 = path("s3://b/f1.parquet");
        StoragePath f2 = path("s3://b/f2.parquet");

        Map<StoragePath, SourceMetadata> metadata = orderedMap(f1, meta(schema1), f2, meta(schema2));
        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);

        assertThat(result.unifiedSchema().get(0).dataType(), equalTo(DataType.LONG));

        SchemaReconciliation.ColumnMapping mapping1 = result.perFileInfo().get(f1).mapping();
        assertThat(mapping1.hasCasts(), equalTo(true));
        assertThat(mapping1.cast(0), equalTo(DataType.LONG));
    }

    public void testUnionByNameTypeWideningIntToDouble() {
        List<Attribute> schema1 = List.of(attr("val", DataType.INTEGER));
        List<Attribute> schema2 = List.of(attr("val", DataType.DOUBLE));

        StoragePath f1 = path("s3://b/f1.parquet");
        StoragePath f2 = path("s3://b/f2.parquet");

        Map<StoragePath, SourceMetadata> metadata = orderedMap(f1, meta(schema1), f2, meta(schema2));
        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);

        assertThat(result.unifiedSchema().get(0).dataType(), equalTo(DataType.DOUBLE));

        SchemaReconciliation.ColumnMapping mapping1 = result.perFileInfo().get(f1).mapping();
        assertThat(mapping1.hasCasts(), equalTo(true));
        assertThat(mapping1.cast(0), equalTo(DataType.DOUBLE));

        SchemaReconciliation.ColumnMapping mapping2 = result.perFileInfo().get(f2).mapping();
        assertThat(mapping2.hasCasts(), equalTo(false));
    }

    public void testUnionByNameTypeWideningDatetimeToDateNanos() {
        List<Attribute> schema1 = List.of(attr("ts", DataType.DATETIME));
        List<Attribute> schema2 = List.of(attr("ts", DataType.DATE_NANOS));

        StoragePath f1 = path("s3://b/f1.parquet");
        StoragePath f2 = path("s3://b/f2.parquet");

        Map<StoragePath, SourceMetadata> metadata = orderedMap(f1, meta(schema1), f2, meta(schema2));
        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);

        assertThat(result.unifiedSchema().get(0).dataType(), equalTo(DataType.DATE_NANOS));

        SchemaReconciliation.ColumnMapping mapping1 = result.perFileInfo().get(f1).mapping();
        assertThat(mapping1.hasCasts(), equalTo(true));
        assertThat(mapping1.cast(0), equalTo(DataType.DATE_NANOS));

        SchemaReconciliation.ColumnMapping mapping2 = result.perFileInfo().get(f2).mapping();
        assertThat(mapping2.hasCasts(), equalTo(false));
    }

    public void testUnionByNameLongToDoubleRejected() {
        List<Attribute> schema1 = List.of(attr("val", DataType.LONG));
        List<Attribute> schema2 = List.of(attr("val", DataType.DOUBLE));

        StoragePath f1 = path("s3://b/f1.parquet");
        StoragePath f2 = path("s3://b/f2.parquet");

        Map<StoragePath, SourceMetadata> metadata = orderedMap(f1, meta(schema1), f2, meta(schema2));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SchemaReconciliation.reconcileUnionByName(metadata)
        );
        assertThat(e.getMessage(), containsString("val"));
        assertThat(e.getMessage(), containsString("No compatible supertype"));
    }

    public void testUnionByNameColumnOrdering() {
        List<Attribute> schema1 = List.of(attr("b", DataType.INTEGER), attr("a", DataType.KEYWORD));
        List<Attribute> schema2 = List.of(attr("c", DataType.DOUBLE), attr("b", DataType.INTEGER));

        StoragePath f1 = path("s3://b/f1.parquet");
        StoragePath f2 = path("s3://b/f2.parquet");

        Map<StoragePath, SourceMetadata> metadata = orderedMap(f1, meta(schema1), f2, meta(schema2));
        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);

        assertThat(result.unifiedSchema().get(0).name(), equalTo("b"));
        assertThat(result.unifiedSchema().get(1).name(), equalTo("a"));
        assertThat(result.unifiedSchema().get(2).name(), equalTo("c"));
    }

    public void testUnionByNameThreeFilesTransitiveWidening() {
        List<Attribute> schema1 = List.of(attr("val", DataType.INTEGER));
        List<Attribute> schema2 = List.of(attr("val", DataType.INTEGER));
        List<Attribute> schema3 = List.of(attr("val", DataType.LONG));

        StoragePath f1 = path("s3://b/f1.parquet");
        StoragePath f2 = path("s3://b/f2.parquet");
        StoragePath f3 = path("s3://b/f3.parquet");

        Map<StoragePath, SourceMetadata> metadata = new LinkedHashMap<>();
        metadata.put(f1, meta(schema1));
        metadata.put(f2, meta(schema2));
        metadata.put(f3, meta(schema3));

        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);
        assertThat(result.unifiedSchema().get(0).dataType(), equalTo(DataType.LONG));
    }

    // === Config parsing tests ===

    public void testParseSchemaResolutionNull() {
        assertThat(ExternalSourceResolver.parseSchemaResolution(null), equalTo(FormatReader.SchemaResolution.FIRST_FILE_WINS));
    }

    public void testParseSchemaResolutionEmpty() {
        assertThat(ExternalSourceResolver.parseSchemaResolution(Map.of()), equalTo(FormatReader.SchemaResolution.FIRST_FILE_WINS));
    }

    public void testParseSchemaResolutionFirstFileWins() {
        assertThat(
            ExternalSourceResolver.parseSchemaResolution(Map.of("schema_resolution", "first_file_wins")),
            equalTo(FormatReader.SchemaResolution.FIRST_FILE_WINS)
        );
    }

    public void testParseSchemaResolutionStrict() {
        assertThat(
            ExternalSourceResolver.parseSchemaResolution(Map.of("schema_resolution", "strict")),
            equalTo(FormatReader.SchemaResolution.STRICT)
        );
    }

    public void testParseSchemaResolutionUnionByName() {
        assertThat(
            ExternalSourceResolver.parseSchemaResolution(Map.of("schema_resolution", "union_by_name")),
            equalTo(FormatReader.SchemaResolution.UNION_BY_NAME)
        );
    }

    public void testParseSchemaResolutionCaseInsensitive() {
        assertThat(
            ExternalSourceResolver.parseSchemaResolution(Map.of("schema_resolution", "UNION_BY_NAME")),
            equalTo(FormatReader.SchemaResolution.UNION_BY_NAME)
        );
    }

    public void testParseSchemaResolutionInvalid() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ExternalSourceResolver.parseSchemaResolution(Map.of("schema_resolution", "invalid"))
        );
        assertThat(e.getMessage(), containsString("Unknown schema_resolution value"));
    }

    // === Duplicate column detection tests ===

    public void testStrictDuplicateColumnRejected() {
        List<Attribute> schema = List.of(attr("id", DataType.INTEGER), attr("id", DataType.KEYWORD));
        StoragePath f1 = path("s3://b/f1.parquet");

        Map<StoragePath, SourceMetadata> metadata = orderedMap(f1, meta(schema), f1, meta(schema));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> SchemaReconciliation.reconcileStrict(f1, metadata));
        assertThat(e.getMessage(), containsString("duplicate column name [id]"));
    }

    public void testUnionByNameDuplicateColumnRejected() {
        List<Attribute> schema = List.of(attr("id", DataType.INTEGER), attr("id", DataType.KEYWORD));
        StoragePath f1 = path("s3://b/f1.parquet");

        Map<StoragePath, SourceMetadata> metadata = new LinkedHashMap<>();
        metadata.put(f1, meta(schema));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SchemaReconciliation.reconcileUnionByName(metadata)
        );
        assertThat(e.getMessage(), containsString("duplicate column name [id]"));
    }

    // === Single-file reconciliation tests ===

    public void testStrictSingleFile() {
        List<Attribute> schema = List.of(attr("id", DataType.INTEGER));
        StoragePath f1 = path("s3://b/f1.parquet");

        Map<StoragePath, SourceMetadata> metadata = new LinkedHashMap<>();
        metadata.put(f1, meta(schema));

        SchemaReconciliation.Result result = SchemaReconciliation.reconcileStrict(f1, metadata);
        assertThat(result.unifiedSchema().size(), equalTo(1));
        assertThat(result.perFileInfo().size(), equalTo(1));
    }

    public void testUnionByNameSingleFile() {
        List<Attribute> schema = List.of(attr("id", DataType.INTEGER), attr("name", DataType.KEYWORD));
        StoragePath f1 = path("s3://b/f1.parquet");

        Map<StoragePath, SourceMetadata> metadata = new LinkedHashMap<>();
        metadata.put(f1, meta(schema));

        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);
        assertThat(result.unifiedSchema().size(), equalTo(2));
        assertThat(result.perFileInfo().get(f1).mapping().isIdentity(), equalTo(true));
    }

    // === Incompatible union types test ===

    public void testUnionByNameIntegerVsKeywordRejected() {
        List<Attribute> schema1 = List.of(attr("val", DataType.INTEGER));
        List<Attribute> schema2 = List.of(attr("val", DataType.KEYWORD));

        StoragePath f1 = path("s3://b/f1.parquet");
        StoragePath f2 = path("s3://b/f2.parquet");

        Map<StoragePath, SourceMetadata> metadata = orderedMap(f1, meta(schema1), f2, meta(schema2));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SchemaReconciliation.reconcileUnionByName(metadata)
        );
        assertThat(e.getMessage(), containsString("val"));
        assertThat(e.getMessage(), containsString("No compatible supertype"));
    }

    // === ColumnMapping tests ===

    public void testColumnMappingIdentity() {
        SchemaReconciliation.ColumnMapping mapping = new SchemaReconciliation.ColumnMapping(new int[] { 0, 1, 2 }, null);
        assertTrue(mapping.isIdentity());
        assertFalse(mapping.hasMissingColumns());
        assertFalse(mapping.hasCasts());
    }

    public void testColumnMappingWithMissing() {
        SchemaReconciliation.ColumnMapping mapping = new SchemaReconciliation.ColumnMapping(new int[] { 0, -1, 1 }, null);
        assertFalse(mapping.isIdentity());
        assertTrue(mapping.hasMissingColumns());
    }

    public void testColumnMappingPermutationNotIdentity() {
        SchemaReconciliation.ColumnMapping mapping = new SchemaReconciliation.ColumnMapping(new int[] { 1, 0, 2 }, null);
        assertFalse(mapping.isIdentity());
        assertFalse(mapping.hasMissingColumns());
        assertFalse(mapping.hasCasts());
    }

    public void testColumnMappingWithCasts() {
        SchemaReconciliation.ColumnMapping mapping = new SchemaReconciliation.ColumnMapping(
            new int[] { 0, 1 },
            new DataType[] { DataType.LONG, null }
        );
        assertFalse(mapping.isIdentity());
        assertTrue(mapping.hasCasts());
    }

    // === ColumnMapping serialization round-trip tests ===

    public void testColumnMappingRoundTripNoCasts() throws IOException {
        SchemaReconciliation.ColumnMapping original = new SchemaReconciliation.ColumnMapping(new int[] { 0, 1, 2 }, null);

        SchemaReconciliation.ColumnMapping deserialized = roundTrip(original);

        assertThat(deserialized.columnCount(), equalTo(3));
        assertThat(deserialized.localIndex(0), equalTo(0));
        assertThat(deserialized.localIndex(1), equalTo(1));
        assertThat(deserialized.localIndex(2), equalTo(2));
        assertFalse(deserialized.hasCasts());
        assertTrue(deserialized.isIdentity());
        assertThat(deserialized, equalTo(original));
    }

    public void testColumnMappingRoundTripWithCasts() throws IOException {
        SchemaReconciliation.ColumnMapping original = new SchemaReconciliation.ColumnMapping(
            new int[] { 0, 1, -1 },
            new DataType[] { DataType.LONG, null, null }
        );

        SchemaReconciliation.ColumnMapping deserialized = roundTrip(original);

        assertThat(deserialized.columnCount(), equalTo(3));
        assertThat(deserialized.localIndex(0), equalTo(0));
        assertThat(deserialized.localIndex(1), equalTo(1));
        assertThat(deserialized.localIndex(2), equalTo(-1));
        assertTrue(deserialized.hasCasts());
        assertThat(deserialized.cast(0), equalTo(DataType.LONG));
        assertThat(deserialized.cast(1), nullValue());
        assertThat(deserialized.cast(2), nullValue());
        assertThat(deserialized, equalTo(original));
    }

    public void testColumnMappingRoundTripAllCastTypes() throws IOException {
        SchemaReconciliation.ColumnMapping original = new SchemaReconciliation.ColumnMapping(
            new int[] { 0, 1, 2 },
            new DataType[] { DataType.LONG, DataType.DOUBLE, DataType.DATE_NANOS }
        );

        SchemaReconciliation.ColumnMapping deserialized = roundTrip(original);

        assertThat(deserialized.cast(0), equalTo(DataType.LONG));
        assertThat(deserialized.cast(1), equalTo(DataType.DOUBLE));
        assertThat(deserialized.cast(2), equalTo(DataType.DATE_NANOS));
        assertThat(deserialized, equalTo(original));
    }

    public void testColumnMappingRoundTripEmpty() throws IOException {
        SchemaReconciliation.ColumnMapping original = new SchemaReconciliation.ColumnMapping(new int[] {}, null);

        SchemaReconciliation.ColumnMapping deserialized = roundTrip(original);

        assertThat(deserialized.columnCount(), equalTo(0));
        assertTrue(deserialized.isIdentity());
        assertThat(deserialized, equalTo(original));
    }

    public void testColumnMappingLengthMismatchRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new SchemaReconciliation.ColumnMapping(new int[] { 0, 1 }, new DataType[] { DataType.LONG })
        );
        assertThat(e.getMessage(), containsString("cast array length [1] must match index array length [2]"));
    }

    public void testColumnMappingRoundTripWithMissingColumnsAndCasts() throws IOException {
        SchemaReconciliation.ColumnMapping original = new SchemaReconciliation.ColumnMapping(
            new int[] { 1, -1, 0, -1 },
            new DataType[] { null, null, DataType.DOUBLE, null }
        );

        SchemaReconciliation.ColumnMapping deserialized = roundTrip(original);

        assertThat(deserialized.columnCount(), equalTo(4));
        assertThat(deserialized.localIndex(0), equalTo(1));
        assertThat(deserialized.localIndex(1), equalTo(-1));
        assertThat(deserialized.localIndex(2), equalTo(0));
        assertThat(deserialized.localIndex(3), equalTo(-1));
        assertTrue(deserialized.hasMissingColumns());
        assertTrue(deserialized.hasCasts());
        assertThat(deserialized.cast(2), equalTo(DataType.DOUBLE));
        assertThat(deserialized, equalTo(original));
    }

    private static SchemaReconciliation.ColumnMapping roundTrip(SchemaReconciliation.ColumnMapping mapping) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        mapping.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        return new SchemaReconciliation.ColumnMapping(in);
    }

    public void testCastTypeEnumSerialization() {
        EnumSerializationTestUtils.assertEnumSerialization(
            SchemaReconciliation.ColumnMapping.CastType.class,
            SchemaReconciliation.ColumnMapping.CastType.NONE,
            SchemaReconciliation.ColumnMapping.CastType.LONG,
            SchemaReconciliation.ColumnMapping.CastType.DOUBLE,
            SchemaReconciliation.ColumnMapping.CastType.DATE_NANOS
        );
    }

    // === Helpers ===

    private static Attribute attr(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, null, name, type);
    }

    private static Attribute attrNullable(String name, DataType type, Nullability nullability) {
        return new ReferenceAttribute(Source.EMPTY, null, name, type, nullability, null, false);
    }

    private static StoragePath path(String s) {
        return StoragePath.of(s);
    }

    private static SourceMetadata meta(List<Attribute> schema) {
        return new SimpleMetadata(schema);
    }

    private static Map<StoragePath, SourceMetadata> orderedMap(StoragePath k1, SourceMetadata v1, StoragePath k2, SourceMetadata v2) {
        Map<StoragePath, SourceMetadata> map = new LinkedHashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        return map;
    }

    private static class SimpleMetadata implements SourceMetadata {
        private final List<Attribute> schema;

        SimpleMetadata(List<Attribute> schema) {
            this.schema = schema;
        }

        @Override
        public List<Attribute> schema() {
            return schema;
        }

        @Override
        public String sourceType() {
            return "parquet";
        }

        @Override
        public String location() {
            return "test";
        }
    }
}
