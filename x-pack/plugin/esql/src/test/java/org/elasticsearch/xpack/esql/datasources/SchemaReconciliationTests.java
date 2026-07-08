/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.HeaderWarning;
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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
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

        ColumnMapping mapping = result.perFileInfo().get(f1).mapping();
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

    /**
     * Pin the STRICT contract for the exact INTEGER-vs-KEYWORD pair that UBN now widens to KEYWORD:
     * if a future refactor accidentally routes STRICT through the widening path (or drops the
     * incompatibility check), this guards the user contract that STRICT is still the escape hatch
     * to fail-fast on type drift.
     */
    public void testStrictIntegerVsKeywordStillRejected() {
        List<Attribute> schema1 = List.of(attr("col", DataType.INTEGER));
        List<Attribute> schema2 = List.of(attr("col", DataType.KEYWORD));

        StoragePath f1 = path("s3://b/f1.csv");
        StoragePath f2 = path("s3://b/f2.csv");

        Map<StoragePath, SourceMetadata> metadata = orderedMap(f1, meta(schema1), f2, meta(schema2));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> SchemaReconciliation.reconcileStrict(f1, metadata));
        assertThat(e.getMessage(), containsString("col"));
        assertThat(e.getMessage(), containsString("integer"));
        assertThat(e.getMessage(), containsString("keyword"));
        // STRICT must not emit the UBN widening warning either.
        assertNoResponseWarnings();
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

        ColumnMapping mapping1 = result.perFileInfo().get(f1).mapping();
        assertThat(mapping1, notNullValue());
        assertThat(mapping1.localIndex(2), equalTo(-1));

        ColumnMapping mapping2 = result.perFileInfo().get(f2).mapping();
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

        ColumnMapping mapping1 = result.perFileInfo().get(f1).mapping();
        assertTrue(mapping1.isIdentity());

        ColumnMapping mapping2 = result.perFileInfo().get(f2).mapping();
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

        ColumnMapping mapping1 = result.perFileInfo().get(f1).mapping();
        assertThat(mapping1, equalTo(new ColumnMapping(new int[] { 0 }, new DataType[] { DataType.LONG })));
    }

    public void testUnionByNameTypeWideningIntToDouble() {
        List<Attribute> schema1 = List.of(attr("val", DataType.INTEGER));
        List<Attribute> schema2 = List.of(attr("val", DataType.DOUBLE));

        StoragePath f1 = path("s3://b/f1.parquet");
        StoragePath f2 = path("s3://b/f2.parquet");

        Map<StoragePath, SourceMetadata> metadata = orderedMap(f1, meta(schema1), f2, meta(schema2));
        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);

        assertThat(result.unifiedSchema().get(0).dataType(), equalTo(DataType.DOUBLE));

        ColumnMapping mapping1 = result.perFileInfo().get(f1).mapping();
        assertThat(mapping1, equalTo(new ColumnMapping(new int[] { 0 }, new DataType[] { DataType.DOUBLE })));

        ColumnMapping mapping2 = result.perFileInfo().get(f2).mapping();
        assertThat(mapping2, equalTo(new ColumnMapping(new int[] { 0 }, null)));
    }

    public void testUnionByNameTypeWideningDatetimeToDateNanos() {
        List<Attribute> schema1 = List.of(attr("ts", DataType.DATETIME));
        List<Attribute> schema2 = List.of(attr("ts", DataType.DATE_NANOS));

        StoragePath f1 = path("s3://b/f1.parquet");
        StoragePath f2 = path("s3://b/f2.parquet");

        Map<StoragePath, SourceMetadata> metadata = orderedMap(f1, meta(schema1), f2, meta(schema2));
        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);

        assertThat(result.unifiedSchema().get(0).dataType(), equalTo(DataType.DATE_NANOS));

        ColumnMapping mapping1 = result.perFileInfo().get(f1).mapping();
        assertThat(mapping1, equalTo(new ColumnMapping(new int[] { 0 }, new DataType[] { DataType.DATE_NANOS })));

        ColumnMapping mapping2 = result.perFileInfo().get(f2).mapping();
        assertThat(mapping2, equalTo(new ColumnMapping(new int[] { 0 }, null)));
    }

    public void testUnionByNameLongToDoubleWidensToKeyword() {
        // The lossy LONG + DOUBLE pair is intentionally outside the lossless table (>2^53 precision
        // loss). Under UBN it falls back to KEYWORD with a warning — louder than silent precision
        // loss and consistent with the cross-type floor in DuckDB / Spark / ClickHouse.
        List<Attribute> schema1 = List.of(attr("val", DataType.LONG));
        List<Attribute> schema2 = List.of(attr("val", DataType.DOUBLE));

        StoragePath f1 = path("s3://b/f1.parquet");
        StoragePath f2 = path("s3://b/f2.parquet");

        Map<StoragePath, SourceMetadata> metadata = orderedMap(f1, meta(schema1), f2, meta(schema2));
        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);

        assertThat(result.unifiedSchema().get(0).dataType(), equalTo(DataType.KEYWORD));
        // Both files contributed non-string types → both file mappings carry a KEYWORD cast.
        ColumnMapping m1 = result.perFileInfo().get(f1).mapping();
        ColumnMapping m2 = result.perFileInfo().get(f2).mapping();
        assertThat(m1, equalTo(new ColumnMapping(new int[] { 0 }, new DataType[] { DataType.KEYWORD })));
        assertThat(m2, equalTo(new ColumnMapping(new int[] { 0 }, new DataType[] { DataType.KEYWORD })));

        List<String> warnings = drainWarningMessages();
        assertWarningMentionsAll(warnings, "val", "long", "double", "f1.parquet", "f2.parquet");
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

    // === Cross-file scalar/object shape conflict tests (esql-planning#1050) ===
    //
    // A field that is a scalar leaf in one file's schema and a dotted-prefix parent in another's
    // (an NDJSON field that is a scalar in one file and an object in the other) must reconcile to
    // a single shape under UNION_BY_NAME — mirroring the per-file single-shape rule from
    // esql-planning#1028 (first shape wins). Before this fix, [user] and [user.id]/[user.tier]
    // never collided by name, so the unified schema fabricated both shapes and the losing file's
    // values vanished silently. See SchemaReconciliation#resolveShapeConflicts.

    public void testUnionByNameScalarVsObjectShapeConflictResolvesToScalar() {
        List<Attribute> scalarFile = List.of(attr("event", DataType.INTEGER), attr("user", DataType.KEYWORD));
        List<Attribute> objectFile = List.of(
            attr("event", DataType.INTEGER),
            attr("user.id", DataType.KEYWORD),
            attr("user.tier", DataType.KEYWORD)
        );

        StoragePath a = path("s3://b/a.ndjson");
        StoragePath b = path("s3://b/b.ndjson");
        Map<StoragePath, SourceMetadata> metadata = orderedMap(a, meta(scalarFile, "ndjson"), b, meta(objectFile, "ndjson"));
        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);

        assertThat(
            "expected exactly the first file's scalar [user] shape in the unified schema",
            userFamily(result),
            equalTo(List.of("user"))
        );
        drainWarningMessages();
    }

    /** Mirror of {@link #testUnionByNameScalarVsObjectShapeConflictResolvesToScalar}: object-shape file first. */
    public void testUnionByNameObjectVsScalarShapeConflictResolvesToObject() {
        List<Attribute> objectFile = List.of(
            attr("event", DataType.INTEGER),
            attr("user.id", DataType.KEYWORD),
            attr("user.tier", DataType.KEYWORD)
        );
        List<Attribute> scalarFile = List.of(attr("event", DataType.INTEGER), attr("user", DataType.KEYWORD));

        StoragePath a = path("s3://b/a.ndjson");
        StoragePath b = path("s3://b/b.ndjson");
        Map<StoragePath, SourceMetadata> metadata = orderedMap(a, meta(objectFile, "ndjson"), b, meta(scalarFile, "ndjson"));
        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);

        assertThat(
            "expected exactly the first file's nested [user.*] shape in the unified schema",
            userFamily(result),
            equalTo(List.of("user.id", "user.tier"))
        );
        drainWarningMessages();
    }

    /**
     * The unified schema shape is only half the fix: the losing (object-shaped) file's own
     * {@code readSchema} pin must carry the winning scalar attribute, not its own
     * [user.id]/[user.tier] sub-schema — that's what routes the file's real values through the
     * existing per-file shape-conflict/{@code ErrorPolicy} handling (elastic/esql-planning#1028)
     * at read time instead of silently vanishing.
     */
    public void testUnionByNameShapeConflictOverridesLosingFileReadSchema() {
        List<Attribute> scalarFile = List.of(attr("event", DataType.INTEGER), attr("user", DataType.KEYWORD));
        List<Attribute> objectFile = List.of(
            attr("event", DataType.INTEGER),
            attr("user.id", DataType.KEYWORD),
            attr("user.tier", DataType.KEYWORD)
        );

        StoragePath a = path("s3://b/a.ndjson");
        StoragePath b = path("s3://b/b.ndjson");
        Map<StoragePath, SourceMetadata> metadata = orderedMap(a, meta(scalarFile, "ndjson"), b, meta(objectFile, "ndjson"));
        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);

        List<Attribute> losingReadSchema = result.perFileInfo().get(b).fileSchema().attributes();
        List<String> losingNames = losingReadSchema.stream().map(Attribute::name).toList();
        assertThat(losingNames, equalTo(List.of("event", "user")));
        assertThat(losingReadSchema.get(losingNames.indexOf("user")).dataType(), equalTo(DataType.KEYWORD));

        // The winning file's own read schema is untouched.
        assertThat(result.perFileInfo().get(a).fileSchema().attributes(), equalTo(scalarFile));
        drainWarningMessages();
    }

    public void testUnionByNameShapeConflictEmitsWarning() {
        List<Attribute> scalarFile = List.of(attr("event", DataType.INTEGER), attr("user", DataType.KEYWORD));
        List<Attribute> objectFile = List.of(
            attr("event", DataType.INTEGER),
            attr("user.id", DataType.KEYWORD),
            attr("user.tier", DataType.KEYWORD)
        );

        StoragePath a = path("s3://b/a.ndjson");
        StoragePath b = path("s3://b/b.ndjson");
        Map<StoragePath, SourceMetadata> metadata = orderedMap(a, meta(scalarFile, "ndjson"), b, meta(objectFile, "ndjson"));
        SchemaReconciliation.reconcileUnionByName(metadata);

        List<String> warnings = drainWarningMessages();
        assertWarningMentionsAll(warnings, "user", "a.ndjson", "b.ndjson", "scalar", "object");
    }

    /**
     * Three files, two contributing the object shape: the winner is still "first file overall"
     * (the anchor semantics from #1028/{@code FIRST_FILE_WINS}), and every losing file — not just
     * the first one encountered — gets its own {@code readSchema} overridden.
     */
    public void testUnionByNameShapeConflictThreeFilesFirstFileWins() {
        List<Attribute> objectFile1 = List.of(attr("event", DataType.INTEGER), attr("user.id", DataType.KEYWORD));
        List<Attribute> scalarFile = List.of(attr("event", DataType.INTEGER), attr("user", DataType.KEYWORD));
        List<Attribute> objectFile2 = List.of(attr("event", DataType.INTEGER), attr("user.id", DataType.KEYWORD));

        StoragePath a = path("s3://b/a.ndjson");
        StoragePath b = path("s3://b/b.ndjson");
        StoragePath c = path("s3://b/c.ndjson");
        Map<StoragePath, SourceMetadata> metadata = new LinkedHashMap<>();
        metadata.put(a, meta(objectFile1, "ndjson"));
        metadata.put(b, meta(scalarFile, "ndjson"));
        metadata.put(c, meta(objectFile2, "ndjson"));

        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);

        assertThat(userFamily(result), equalTo(List.of("user.id")));
        assertThat(result.perFileInfo().get(a).fileSchema().attributes(), equalTo(objectFile1));
        assertThat(result.perFileInfo().get(c).fileSchema().attributes(), equalTo(objectFile2));
        assertThat(
            result.perFileInfo().get(b).fileSchema().attributes().stream().map(Attribute::name).toList(),
            equalTo(List.of("event", "user.id"))
        );
        drainWarningMessages();
    }

    /**
     * A file that carries <em>both</em> the bare name and an unrelated dotted child for the same
     * root in one file (e.g. a literal flat {@code "user.tag"} key coexisting with scalar
     * {@code "user"}) has its dotted column excluded from the family — that column is already
     * disambiguated per-file as a flat key, not a nested child (see
     * {@code NdJsonPageDecoder#hasDottedPrefixConflict}) — but its bare {@code user} leaf still
     * fully participates in the cross-file vote like any other file's. Here it happens to *agree*
     * with the (scalar) winner, so both its columns stay untouched. See
     * {@link #testUnionByNameShapeConflictFileWithBothShapesDisagreeingIsAlsoOverridden} for the
     * disagreeing case.
     */
    public void testUnionByNameShapeConflictFileWithBothShapesAgreeingIsUnaffected() {
        List<Attribute> scalarFile = List.of(attr("event", DataType.INTEGER), attr("user", DataType.KEYWORD));
        List<Attribute> objectFile = List.of(
            attr("event", DataType.INTEGER),
            attr("user.id", DataType.KEYWORD),
            attr("user.tier", DataType.KEYWORD)
        );
        List<Attribute> bothShapesFile = List.of(
            attr("event", DataType.INTEGER),
            attr("user", DataType.KEYWORD),
            attr("user.tag", DataType.KEYWORD)
        );

        StoragePath scalarPath = path("s3://b/a.ndjson");
        StoragePath objectPath = path("s3://b/b.ndjson");
        StoragePath bothPath = path("s3://b/c.ndjson");
        Map<StoragePath, SourceMetadata> metadata = new LinkedHashMap<>();
        metadata.put(scalarPath, meta(scalarFile, "ndjson"));
        metadata.put(objectPath, meta(objectFile, "ndjson"));
        metadata.put(bothPath, meta(bothShapesFile, "ndjson"));

        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);

        assertThat(userFamily(result), equalTo(List.of("user", "user.tag")));
        assertThat(result.perFileInfo().get(bothPath).fileSchema().attributes(), equalTo(bothShapesFile));
        assertThat(
            result.perFileInfo().get(objectPath).fileSchema().attributes().stream().map(Attribute::name).toList(),
            equalTo(List.of("event", "user"))
        );
        drainWarningMessages();
    }

    /**
     * Mirror of {@link #testUnionByNameShapeConflictFileWithBothShapesAgreeingIsUnaffected}: when
     * the both-shapes file's own bare {@code user} *disagrees* with the winning shape (here the
     * winner is the nested object, contributed by a different file), that leaf column is
     * overridden exactly like any other losing file's — only the file's unrelated dotted column
     * ({@code user.tag}, a literal flat key per {@code NdJsonPageDecoder#hasDottedPrefixConflict})
     * stays untouched. Guards the fix to {@code resolveFamily}: an earlier version exempted a
     * both-shapes file from the win/loss vote entirely, which let its scalar {@code user} value
     * silently keep coexisting with the winning nested shape in the unified schema — reopening
     * the exact scalar/object ambiguity this pass exists to close.
     */
    public void testUnionByNameShapeConflictFileWithBothShapesDisagreeingIsAlsoOverridden() {
        List<Attribute> objectFile = List.of(
            attr("event", DataType.INTEGER),
            attr("user.id", DataType.KEYWORD),
            attr("user.tier", DataType.KEYWORD)
        );
        List<Attribute> scalarFile = List.of(attr("event", DataType.INTEGER), attr("user", DataType.KEYWORD));
        List<Attribute> bothShapesFile = List.of(
            attr("event", DataType.INTEGER),
            attr("user", DataType.KEYWORD),
            attr("user.tag", DataType.KEYWORD)
        );

        StoragePath objectPath = path("s3://b/a.ndjson");
        StoragePath scalarPath = path("s3://b/b.ndjson");
        StoragePath bothPath = path("s3://b/c.ndjson");
        Map<StoragePath, SourceMetadata> metadata = new LinkedHashMap<>();
        metadata.put(objectPath, meta(objectFile, "ndjson"));
        metadata.put(scalarPath, meta(scalarFile, "ndjson"));
        metadata.put(bothPath, meta(bothShapesFile, "ndjson"));

        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);

        // The scalar [user] shape is now fully gone from the unified schema -- both of its
        // contributors (scalarFile and bothShapesFile) lost the vote -- while the unrelated
        // [user.tag] flat key survives untouched.
        assertThat(userFamily(result), equalTo(List.of("user.id", "user.tier", "user.tag")));

        List<String> bothOverrideNames = result.perFileInfo()
            .get(bothPath)
            .fileSchema()
            .attributes()
            .stream()
            .map(Attribute::name)
            .toList();
        assertThat("own unrelated [user.tag] column must survive untouched", bothOverrideNames, hasItem("user.tag"));
        assertThat("own [user] leaf must be pinned to the winning nested shape", bothOverrideNames, hasItem("user.id"));
        assertThat("own [user] leaf must no longer appear on its own", bothOverrideNames, not(hasItem("user")));

        assertThat(
            result.perFileInfo().get(scalarPath).fileSchema().attributes().stream().map(Attribute::name).toList(),
            equalTo(List.of("event", "user.id", "user.tier"))
        );
        // The winning file's own read schema is untouched.
        assertThat(result.perFileInfo().get(objectPath).fileSchema().attributes(), equalTo(objectFile));
        drainWarningMessages();
    }

    /**
     * Pins that {@code STRICT} still rejects the exact esql-planning#1050 repro shape outright
     * (differing column counts) rather than ever attempting the UNION_BY_NAME-style resolution —
     * the issue calls this out as already-correct behavior to guard, not change.
     */
    public void testStrictRejectsScalarVsObjectShapeConflict() {
        List<Attribute> scalarFile = List.of(attr("event", DataType.INTEGER), attr("user", DataType.KEYWORD));
        List<Attribute> objectFile = List.of(
            attr("event", DataType.INTEGER),
            attr("user.id", DataType.KEYWORD),
            attr("user.tier", DataType.KEYWORD)
        );

        StoragePath a = path("s3://b/a.ndjson");
        StoragePath b = path("s3://b/b.ndjson");
        Map<StoragePath, SourceMetadata> metadata = orderedMap(a, meta(scalarFile), b, meta(objectFile));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> SchemaReconciliation.reconcileStrict(a, metadata));
        assertThat(e.getMessage(), containsString("Schema mismatch"));
    }

    /**
     * Regression for the review feedback on #152775: {@link SchemaReconciliation#resolveFamily}
     * only has flattened column names to work with, so a {@code root}/{@code root.*} pair can be a
     * genuine cross-file scalar/object conflict (NDJSON) or two entirely unrelated, independent
     * columns that merely share a naming prefix — e.g. a CSV file whose header is literally
     * {@code user.tag} next to another CSV file's ordinary {@code user} column. CSV headers are
     * never nested, so these must both survive in the unified schema, NULL-filled in whichever
     * file lacks them, exactly like any other unrelated pair of column names — not be treated as a
     * shape conflict that silently drops one of them.
     */
    public void testUnionByNameScalarAndDottedLiteralCoexistForNonNdjsonFormat() {
        List<Attribute> scalarFile = List.of(attr("event", DataType.INTEGER), attr("user", DataType.KEYWORD));
        List<Attribute> literalDottedFile = List.of(attr("event", DataType.INTEGER), attr("user.tag", DataType.KEYWORD));

        StoragePath a = path("s3://b/a.csv");
        StoragePath b = path("s3://b/b.csv");
        Map<StoragePath, SourceMetadata> metadata = orderedMap(a, meta(scalarFile, "csv"), b, meta(literalDottedFile, "csv"));
        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);

        assertThat(
            "both the literal [user] and [user.tag] columns must survive independently, not collapse to one shape",
            userFamily(result),
            equalTo(List.of("user", "user.tag"))
        );
        List<Attribute> unifiedAttributes = result.unifiedSchema().attributes();
        assertThat(
            unifiedAttributes.stream().filter(at -> at.name().equals("user")).findFirst().orElseThrow().nullable(),
            equalTo(Nullability.TRUE)
        );
        assertThat(
            unifiedAttributes.stream().filter(at -> at.name().equals("user.tag")).findFirst().orElseThrow().nullable(),
            equalTo(Nullability.TRUE)
        );
        assertThat(result.perFileInfo().get(a).fileSchema().attributes(), equalTo(scalarFile));
        assertThat(result.perFileInfo().get(b).fileSchema().attributes(), equalTo(literalDottedFile));
        assertNoResponseWarnings();
    }

    /**
     * Same regression as {@link #testUnionByNameScalarAndDottedLiteralCoexistForNonNdjsonFormat}
     * but for Parquet: unlike CSV, Parquet's reader genuinely flattens nested structs into dotted
     * names, but it has no equivalent of NDJSON's {@code shapeConflict} read-time fallback for a
     * column pinned to a shape that disagrees with the file's own footer-declared type — so this
     * pass must not touch Parquet files either. See {@code supportsShapeConflictResolution}.
     */
    public void testUnionByNameScalarVsObjectConflictIgnoredForParquetFormat() {
        List<Attribute> scalarFile = List.of(attr("event", DataType.INTEGER), attr("user", DataType.KEYWORD));
        List<Attribute> objectFile = List.of(
            attr("event", DataType.INTEGER),
            attr("user.id", DataType.KEYWORD),
            attr("user.tier", DataType.KEYWORD)
        );

        StoragePath a = path("s3://b/a.parquet");
        StoragePath b = path("s3://b/b.parquet");
        Map<StoragePath, SourceMetadata> metadata = orderedMap(a, meta(scalarFile, "parquet"), b, meta(objectFile, "parquet"));
        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);

        assertThat(userFamily(result), equalTo(List.of("user", "user.id", "user.tier")));
        assertThat(result.perFileInfo().get(a).fileSchema().attributes(), equalTo(scalarFile));
        assertThat(result.perFileInfo().get(b).fileSchema().attributes(), equalTo(objectFile));
        assertNoResponseWarnings();
    }

    /**
     * A single NDJSON file's nested {@code user.*} shape must not be treated as conflicting with a
     * literal {@code user.tag} column from an unrelated CSV file in the same query — only files
     * whose format actually supports shape-conflict resolution ever enter the family vote, and one
     * ndjson contributor alone (agreeing with itself) is not a conflict.
     */
    public void testUnionByNameMixedFormatsOnlyNdjsonParticipatesInFamily() {
        List<Attribute> ndjsonFile = List.of(
            attr("event", DataType.INTEGER),
            attr("user.id", DataType.KEYWORD),
            attr("user.tier", DataType.KEYWORD)
        );
        List<Attribute> csvFile = List.of(attr("event", DataType.INTEGER), attr("user.tag", DataType.KEYWORD));

        StoragePath a = path("s3://b/a.ndjson");
        StoragePath b = path("s3://b/b.csv");
        Map<StoragePath, SourceMetadata> metadata = orderedMap(a, meta(ndjsonFile, "ndjson"), b, meta(csvFile, "csv"));
        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);

        assertThat(userFamily(result), equalTo(List.of("user.id", "user.tier", "user.tag")));
        assertThat(result.perFileInfo().get(a).fileSchema().attributes(), equalTo(ndjsonFile));
        assertThat(result.perFileInfo().get(b).fileSchema().attributes(), equalTo(csvFile));
        assertNoResponseWarnings();
    }

    /** The {@code [user]}-family column names present in the unified schema, in schema order. */
    private static List<String> userFamily(SchemaReconciliation.Result result) {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < result.unifiedSchema().size(); i++) {
            String n = result.unifiedSchema().get(i).name();
            if (n.equals("user") || n.startsWith("user.")) {
                names.add(n);
            }
        }
        return names;
    }

    // === Config parsing tests ===

    public void testParseSchemaResolutionNull() {
        assertThat(ExternalSourceResolver.parseSchemaResolution(null), equalTo(FormatReader.DEFAULT_SCHEMA_RESOLUTION));
    }

    public void testParseSchemaResolutionEmpty() {
        assertThat(ExternalSourceResolver.parseSchemaResolution(Map.of()), equalTo(FormatReader.DEFAULT_SCHEMA_RESOLUTION));
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

    public void testUnionByNameIntegerVsKeywordWidensToKeyword() {
        // The motivating case for this PR: text-format sampler in file A guessed INTEGER, file B
        // guessed KEYWORD. Pre-fix this threw; we now widen to KEYWORD with a warning that names
        // the contributing files and inferred types so the user can act on the disagreement.
        List<Attribute> schema1 = List.of(attr("val", DataType.INTEGER));
        List<Attribute> schema2 = List.of(attr("val", DataType.KEYWORD));

        StoragePath f1 = path("s3://b/f1.parquet");
        StoragePath f2 = path("s3://b/f2.parquet");

        Map<StoragePath, SourceMetadata> metadata = orderedMap(f1, meta(schema1), f2, meta(schema2));
        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);

        assertThat(result.unifiedSchema().get(0).dataType(), equalTo(DataType.KEYWORD));
        // The INT file carries a stringify cast; the KEYWORD file is a no-op identity.
        assertThat(
            result.perFileInfo().get(f1).mapping(),
            equalTo(new ColumnMapping(new int[] { 0 }, new DataType[] { DataType.KEYWORD }))
        );
        assertThat(result.perFileInfo().get(f2).mapping(), equalTo(new ColumnMapping(new int[] { 0 }, null)));

        List<String> warnings = drainWarningMessages();
        assertWarningMentionsAll(warnings, "val", "integer", "keyword", "f1.parquet", "f2.parquet");
    }

    // === ColumnMapping tests ===

    public void testColumnMappingIdentity() {
        assertTrue(new ColumnMapping(new int[] { 0, 1, 2 }, null).isIdentity());
    }

    public void testColumnMappingWithMissingIsNotIdentity() {
        assertFalse(new ColumnMapping(new int[] { 0, -1, 1 }, null).isIdentity());
    }

    public void testColumnMappingPermutationIsNotIdentity() {
        assertFalse(new ColumnMapping(new int[] { 1, 0, 2 }, null).isIdentity());
    }

    public void testColumnMappingWithCastsIsNotIdentity() {
        assertFalse(new ColumnMapping(new int[] { 0, 1 }, new DataType[] { DataType.LONG, null }).isIdentity());
    }

    // === ColumnMapping serialization round-trip tests ===

    public void testColumnMappingRoundTripNoCasts() throws IOException {
        ColumnMapping original = new ColumnMapping(new int[] { 0, 1, 2 }, null);
        assertThat(roundTrip(original), equalTo(original));
    }

    public void testColumnMappingRoundTripWithCasts() throws IOException {
        ColumnMapping original = new ColumnMapping(new int[] { 0, 1, -1 }, new DataType[] { DataType.LONG, null, null });
        assertThat(roundTrip(original), equalTo(original));
    }

    public void testColumnMappingRoundTripAllCastTypes() throws IOException {
        ColumnMapping original = new ColumnMapping(
            new int[] { 0, 1, 2 },
            new DataType[] { DataType.LONG, DataType.DOUBLE, DataType.DATE_NANOS }
        );
        assertThat(roundTrip(original), equalTo(original));
    }

    public void testColumnMappingRoundTripEmpty() throws IOException {
        ColumnMapping original = new ColumnMapping(new int[] {}, null);
        assertThat(roundTrip(original), equalTo(original));
    }

    public void testColumnMappingLengthMismatchRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new ColumnMapping(new int[] { 0, 1 }, new DataType[] { DataType.LONG })
        );
        assertThat(e.getMessage(), containsString("cast array length [1] must match index array length [2]"));
    }

    public void testColumnMappingRoundTripWithMissingColumnsAndCasts() throws IOException {
        ColumnMapping original = new ColumnMapping(new int[] { 1, -1, 0, -1 }, new DataType[] { null, null, DataType.DOUBLE, null });
        assertThat(roundTrip(original), equalTo(original));
    }

    private static ColumnMapping roundTrip(ColumnMapping mapping) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        mapping.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        return new ColumnMapping(in);
    }

    public void testCastTypeEnumSerialization() {
        EnumSerializationTestUtils.assertEnumSerialization(
            ColumnMapping.CastType.class,
            ColumnMapping.CastType.NONE,
            ColumnMapping.CastType.LONG,
            ColumnMapping.CastType.DOUBLE,
            ColumnMapping.CastType.DATE_NANOS,
            ColumnMapping.CastType.KEYWORD
        );
    }

    // === UBN KEYWORD fallback tests ===

    public void testUnionByNameWidenBooleanIntToKeyword() {
        List<Attribute> schema1 = List.of(attr("val", DataType.BOOLEAN));
        List<Attribute> schema2 = List.of(attr("val", DataType.INTEGER));

        StoragePath f1 = path("s3://b/f1.csv");
        StoragePath f2 = path("s3://b/f2.csv");

        Map<StoragePath, SourceMetadata> metadata = orderedMap(f1, meta(schema1), f2, meta(schema2));
        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);

        assertThat(result.unifiedSchema().get(0).dataType(), equalTo(DataType.KEYWORD));
        assertThat(
            result.perFileInfo().get(f1).mapping(),
            equalTo(new ColumnMapping(new int[] { 0 }, new DataType[] { DataType.KEYWORD }))
        );
        assertThat(
            result.perFileInfo().get(f2).mapping(),
            equalTo(new ColumnMapping(new int[] { 0 }, new DataType[] { DataType.KEYWORD }))
        );

        assertWarningMentionsAll(drainWarningMessages(), "val", "boolean", "integer");
    }

    public void testUnionByNameWidenDatetimeKeywordToKeyword() {
        List<Attribute> schema1 = List.of(attr("ts", DataType.DATETIME));
        List<Attribute> schema2 = List.of(attr("ts", DataType.KEYWORD));

        StoragePath f1 = path("s3://b/f1.csv");
        StoragePath f2 = path("s3://b/f2.csv");

        Map<StoragePath, SourceMetadata> metadata = orderedMap(f1, meta(schema1), f2, meta(schema2));
        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);

        assertThat(result.unifiedSchema().get(0).dataType(), equalTo(DataType.KEYWORD));
        // The DATETIME file needs the cast (so castBlock can pick the date formatter); the
        // already-keyword file does not.
        assertThat(
            result.perFileInfo().get(f1).mapping(),
            equalTo(new ColumnMapping(new int[] { 0 }, new DataType[] { DataType.KEYWORD }))
        );
        assertThat(result.perFileInfo().get(f2).mapping(), equalTo(new ColumnMapping(new int[] { 0 }, null)));

        // At least one non-string contributor → warning fires.
        assertWarningMentionsAll(drainWarningMessages(), "ts", "datetime", "keyword");
    }

    public void testUnionByNameThreeFilesWithTriDisagreement() {
        List<Attribute> schema1 = List.of(attr("c", DataType.INTEGER));
        List<Attribute> schema2 = List.of(attr("c", DataType.KEYWORD));
        List<Attribute> schema3 = List.of(attr("c", DataType.DOUBLE));

        StoragePath f1 = path("s3://b/f1.csv");
        StoragePath f2 = path("s3://b/f2.csv");
        StoragePath f3 = path("s3://b/f3.csv");

        Map<StoragePath, SourceMetadata> metadata = new LinkedHashMap<>();
        metadata.put(f1, meta(schema1));
        metadata.put(f2, meta(schema2));
        metadata.put(f3, meta(schema3));

        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);
        assertThat(result.unifiedSchema().get(0).dataType(), equalTo(DataType.KEYWORD));

        List<String> warnings = drainWarningMessages();
        assertWarningMentionsAll(warnings, "c", "integer", "keyword", "double", "f1.csv", "f2.csv", "f3.csv");
    }

    public void testUnionByNameAllKeywordEmitsNoWarning() {
        // A column that's KEYWORD in every file is steady state, not degradation.
        List<Attribute> schema = List.of(attr("name", DataType.KEYWORD));
        StoragePath f1 = path("s3://b/f1.csv");
        StoragePath f2 = path("s3://b/f2.csv");

        Map<StoragePath, SourceMetadata> metadata = orderedMap(f1, meta(schema), f2, meta(schema));
        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);

        assertThat(result.unifiedSchema().get(0).dataType(), equalTo(DataType.KEYWORD));
        assertNoResponseWarnings();
    }

    public void testUnionByNameLosslessWideningEmitsNoWarning() {
        // Lossless widening (INT + LONG → LONG, INT + DOUBLE → DOUBLE, DATETIME + DATE_NANOS →
        // DATE_NANOS) is unchanged behavior and must not emit a stringification warning.
        List<Attribute> schema1 = List.of(attr("a", DataType.INTEGER), attr("b", DataType.INTEGER), attr("c", DataType.DATETIME));
        List<Attribute> schema2 = List.of(attr("a", DataType.LONG), attr("b", DataType.DOUBLE), attr("c", DataType.DATE_NANOS));

        StoragePath f1 = path("s3://b/f1.parquet");
        StoragePath f2 = path("s3://b/f2.parquet");

        Map<StoragePath, SourceMetadata> metadata = orderedMap(f1, meta(schema1), f2, meta(schema2));
        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);

        assertThat(result.unifiedSchema().get(0).dataType(), equalTo(DataType.LONG));
        assertThat(result.unifiedSchema().get(1).dataType(), equalTo(DataType.DOUBLE));
        assertThat(result.unifiedSchema().get(2).dataType(), equalTo(DataType.DATE_NANOS));
        assertNoResponseWarnings();
    }

    public void testSchemaWidenLongDoubleStaysNullForStrictCallers() {
        // The strict-only entry point keeps returning null so future non-UBN callers that want the
        // lossless-only semantic still have it. UBN's widenToCommonOrKeyword is the additive layer
        // — verified indirectly via testUnionByNameLongToDoubleWidensToKeyword above.
        assertThat(SchemaReconciliation.schemaWiden(DataType.LONG, DataType.DOUBLE), nullValue());
        assertThat(SchemaReconciliation.schemaWiden(DataType.DOUBLE, DataType.LONG), nullValue());
    }

    public void testUnionByNameDenseVectorWithIntegerFallsBackToKeyword() {
        // Defensive: we do not delegate wholesale to EsqlDataTypeConverter.commonType (which would
        // pick DENSE_VECTOR here). The UBN path uses its own widenToCommonOrKeyword and falls back
        // to KEYWORD for any pair the lossless table cannot widen.
        List<Attribute> schema1 = List.of(attr("v", DataType.DENSE_VECTOR));
        List<Attribute> schema2 = List.of(attr("v", DataType.INTEGER));

        StoragePath f1 = path("s3://b/f1.parquet");
        StoragePath f2 = path("s3://b/f2.parquet");

        Map<StoragePath, SourceMetadata> metadata = orderedMap(f1, meta(schema1), f2, meta(schema2));
        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);

        assertThat(result.unifiedSchema().get(0).dataType(), equalTo(DataType.KEYWORD));
        assertWarningMentionsAll(drainWarningMessages(), "v", "dense_vector", "integer");
    }

    public void testColumnMappingCastIncludesKeyword() {
        // Asserts that a file whose local type isn't already KEYWORD/TEXT carries a KEYWORD cast
        // after the UBN reconciler picks KEYWORD as the unified type — i.e. the per-file mapping
        // wired up correctly so {@link ColumnMapping#castBlock} fires at read time.
        List<Attribute> schema1 = List.of(attr("c", DataType.INTEGER));
        List<Attribute> schema2 = List.of(attr("c", DataType.KEYWORD));

        StoragePath f1 = path("s3://b/f1.csv");
        StoragePath f2 = path("s3://b/f2.csv");

        Map<StoragePath, SourceMetadata> metadata = orderedMap(f1, meta(schema1), f2, meta(schema2));
        SchemaReconciliation.Result result = SchemaReconciliation.reconcileUnionByName(metadata);

        DataType cast1 = result.perFileInfo().get(f1).mapping().cast(0);
        DataType cast2 = result.perFileInfo().get(f2).mapping().cast(0);
        assertThat(cast1, equalTo(DataType.KEYWORD));
        assertThat(cast2, nullValue());

        // Drain warnings emitted by the reconciler so subsequent tests see a clean context.
        drainWarningMessages();
    }

    // === Warning-header helpers ===
    //
    // ESTestCase sets up a fresh ThreadContext per test (auto-stashed in {@code @After}); the
    // SchemaReconciliation emits warnings via SkipWarnings → HeaderWarning, which deposits them
    // into that thread context. Drain reads + stashes (so a single test can verify multiple
    // emit-events without warnings leaking across asserts), assertWarningMentions checks
    // substring presence in the emitted summary + details.

    private List<String> drainWarningMessages() {
        List<String> raw = threadContext.getResponseHeaders().getOrDefault("Warning", List.of());
        List<String> messages = raw.stream().map(s -> HeaderWarning.extractWarningValueFromWarningHeader(s, false)).toList();
        threadContext.stashContext();
        return messages;
    }

    private void assertNoResponseWarnings() {
        assertNull(
            "expected no Warning headers, found: " + threadContext.getResponseHeaders().get("Warning"),
            threadContext.getResponseHeaders().get("Warning")
        );
    }

    /**
     * Asserts every needle appears somewhere in the emitted warnings (summary + details
     * concatenated). Conjunctive: all needles must be present.
     */
    private void assertWarningMentionsAll(List<String> warnings, String... needles) {
        assertFalse("expected at least one warning, got none", warnings.isEmpty());
        String joined = String.join(" || ", warnings);
        for (String needle : needles) {
            assertTrue("warning [" + joined + "] should mention [" + needle + "]", joined.contains(needle));
        }
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
        return new SimpleMetadata(schema, "parquet");
    }

    /** Like {@link #meta(List)} but with an explicit {@code sourceType}, e.g. {@code "ndjson"}. */
    private static SourceMetadata meta(List<Attribute> schema, String sourceType) {
        return new SimpleMetadata(schema, sourceType);
    }

    private static Map<StoragePath, SourceMetadata> orderedMap(StoragePath k1, SourceMetadata v1, StoragePath k2, SourceMetadata v2) {
        Map<StoragePath, SourceMetadata> map = new LinkedHashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        return map;
    }

    private static class SimpleMetadata implements SourceMetadata {
        private final List<Attribute> schema;
        private final String sourceType;

        SimpleMetadata(List<Attribute> schema, String sourceType) {
            this.schema = schema;
            this.sourceType = sourceType;
        }

        @Override
        public List<Attribute> schema() {
            return schema;
        }

        @Override
        public String sourceType() {
            return sourceType;
        }

        @Override
        public String location() {
            return "test";
        }
    }
}
