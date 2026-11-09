/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.mount;

import org.apache.lucene.index.DocValuesType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.parquet.docvalues.BytesParquetObject;
import org.elasticsearch.xpack.parquet.docvalues.Conversion;
import org.elasticsearch.xpack.parquet.docvalues.ParquetFileInfo;
import org.elasticsearch.xpack.parquet.docvalues.ParquetFooterReader;
import org.elasticsearch.xpack.parquet.docvalues.ParquetTestFiles;
import org.elasticsearch.xpack.parquet.mount.ParquetMappingDeriver.ColumnOverride;
import org.elasticsearch.xpack.parquet.mount.ParquetMappingDeriver.DerivedField;
import org.elasticsearch.xpack.parquet.mount.ParquetMappingDeriver.DerivedMapping;
import org.elasticsearch.xpack.parquet.mount.ParquetMappingDeriver.Overrides;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;

public class ParquetMappingDeriverTests extends ESTestCase {

    private static ParquetFileInfo read(byte[] file) throws IOException {
        return ParquetFooterReader.read(new BytesParquetObject("repository://lake/f.parquet", file));
    }

    private static DerivedField field(DerivedMapping mapping, String name) {
        return mapping.fields()
            .stream()
            .filter(f -> f.name().equals(name))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no field " + name));
    }

    public void testTheTypeTableForAFlatFile() throws IOException {
        DerivedMapping mapping = ParquetMappingDeriver.derive(read(ParquetTestFiles.writeFlat(10, 10)), Overrides.NONE);

        assertEquals(
            List.of("id", "ts", "low", "bucket", "maybe", "name", "kind", "note"),
            mapping.fields().stream().map(DerivedField::name).toList()
        );
        assertTrue(mapping.unmapped().isEmpty());
        assertEquals("integer", field(mapping, "id").type());
        assertEquals("long", field(mapping, "ts").type());
        assertEquals("keyword", field(mapping, "name").type());
        assertTrue(field(mapping, "id").required());
        assertFalse(field(mapping, "maybe").required());
        assertEquals(DocValuesType.NUMERIC, field(mapping, "id").docValuesType());
        assertEquals(DocValuesType.BINARY, field(mapping, "name").docValuesType());

        // every field has no index, and a keyword keeps a single value
        Map<String, Object> properties = mapping.properties();
        assertEquals(Map.of("type", "integer", "index", false), properties.get("id"));
        assertEquals(Map.of("type", "keyword", "index", false, "multi_value", false), properties.get("name"));
        assertEquals("id:id,ts:ts,low:low,bucket:bucket,maybe:maybe,name:name,kind:kind,note:note", mapping.fieldsSetting());
    }

    public void testDatesTimestampsUnsignedIntegersAndUnsupportedTypes() throws IOException {
        DerivedMapping mapping = ParquetMappingDeriver.derive(read(ParquetTestFiles.writeTypes(10)), Overrides.NONE);

        assertEquals("date", field(mapping, "day").type());
        assertEquals(Conversion.DAYS_TO_MILLIS, field(mapping, "day").conversion());
        assertEquals("date", field(mapping, "ts_millis").type());
        assertEquals(Conversion.NONE, field(mapping, "ts_millis").conversion());
        assertEquals("date_nanos", field(mapping, "ts_micros").type());
        assertEquals(Conversion.MICROS_TO_NANOS, field(mapping, "ts_micros").conversion());
        assertEquals("integer", field(mapping, "small").type());
        // a byte array without an annotation holds bytes, and is served as a keyword
        assertEquals("keyword", field(mapping, "raw").type());
        assertThat(mapping.fieldsSetting(), containsString("day:day:days_to_millis"));
        assertThat(mapping.fieldsSetting(), containsString("ts_micros:ts_micros:micros_to_nanos"));

        // double, float, boolean and decimal are listed with their reason, and nothing is dropped silently
        assertEquals(
            Set.of("d", "f", "flag", "dec"),
            mapping.unmapped().stream().map(ParquetMappingDeriver.UnmappedColumn::column).collect(java.util.stream.Collectors.toSet())
        );
        for (ParquetMappingDeriver.UnmappedColumn column : mapping.unmapped()) {
            assertThat(column.reason(), containsString("not supported yet"));
        }

        DerivedMapping unsigned = ParquetMappingDeriver.derive(read(ParquetTestFiles.writeUnsigned(10)), Overrides.NONE);
        assertEquals("long", field(unsigned, "u").type());
        assertEquals("integer", field(unsigned, "small").type());
    }

    public void testNestedColumnsAreReportedAsUnmapped() throws IOException {
        DerivedMapping mapping = ParquetMappingDeriver.derive(read(ParquetTestFiles.writeNested(5)), Overrides.NONE);
        assertEquals(List.of("id"), mapping.fields().stream().map(DerivedField::name).toList());
        assertEquals("inner.value", mapping.unmapped().get(0).column());
        assertThat(mapping.unmapped().get(0).reason(), containsString("inside a struct"));
    }

    public void testOverridesRenameExcludeRetypeAndConvert() throws IOException {
        ParquetFileInfo file = read(ParquetTestFiles.writeFlat(10, 10));
        // ts holds epoch seconds in this file: the timestamp of the data stream, as a date with a conversion
        Overrides overrides = new Overrides(
            Map.of(
                "ts",
                new ColumnOverride(null, "date", Conversion.SECONDS_TO_MILLIS),
                "id",
                new ColumnOverride("row", "long", null),
                "kind",
                new ColumnOverride("category", null, null)
            ),
            Set.of("note", "maybe"),
            "ts"
        );
        DerivedMapping mapping = ParquetMappingDeriver.derive(file, overrides);

        assertEquals(
            List.of("row", "@timestamp", "low", "bucket", "name", "category"),
            mapping.fields().stream().map(DerivedField::name).toList()
        );
        assertEquals("date", field(mapping, "@timestamp").type());
        assertEquals(Conversion.SECONDS_TO_MILLIS, field(mapping, "@timestamp").conversion());
        assertEquals("long", field(mapping, "row").type());
        assertEquals("ts", field(mapping, "@timestamp").column());
        assertEquals("row:id,@timestamp:ts:seconds_to_millis,low:low,bucket:bucket,name:name,category:kind", mapping.fieldsSetting());
        assertTrue("excluded columns are not reported", mapping.unmapped().isEmpty());
    }

    public void testBadOverridesAreRejectedWithAReason() throws IOException {
        ParquetFileInfo file = read(ParquetTestFiles.writeFlat(10, 10));
        for (var entry : List.of(
            Map.entry(
                new Overrides(Map.of("nope", ColumnOverride.NONE), Set.of(), null),
                "which [repository://lake/f.parquet] does not have"
            ),
            Map.entry(new Overrides(Map.of(), Set.of("nope"), null), "does not have"),
            Map.entry(new Overrides(Map.of(), Set.of(), "nope"), "timestamp_field"),
            Map.entry(
                new Overrides(Map.of("id", new ColumnOverride(null, "keyword", null)), Set.of(), null),
                "cannot be mapped as [keyword]"
            ),
            Map.entry(new Overrides(Map.of("name", new ColumnOverride(null, "long", null)), Set.of(), null), "cannot be mapped as [long]"),
            Map.entry(
                new Overrides(Map.of("name", new ColumnOverride(null, null, Conversion.SECONDS_TO_MILLIS)), Set.of(), null),
                "needs an integer column"
            ),
            Map.entry(
                new Overrides(Map.of("id", new ColumnOverride(null, "long", Conversion.SECONDS_TO_MILLIS)), Set.of(), null),
                "does not give the values of a [long] field"
            ),
            Map.entry(
                new Overrides(Map.of("ts", new ColumnOverride(null, "date", Conversion.MICROS_TO_NANOS)), Set.of(), null),
                "does not give the values of a [date] field"
            ),
            // ts maps as long without an override, so it cannot be the timestamp
            Map.entry(new Overrides(Map.of(), Set.of(), "ts"), "needs a type override of date or date_nanos"),
            Map.entry(new Overrides(Map.of("id", new ColumnOverride("ts", null, null)), Set.of(), null), "both map to the field [ts]")
        )) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> ParquetMappingDeriver.derive(file, entry.getKey())
            );
            assertThat(e.getMessage(), containsString(entry.getValue()));
        }
        // a column cannot be named like a metadata field, and is reported unmapped so that the request can rename it
        DerivedMapping reserved = ParquetMappingDeriver.derive(
            file,
            new Overrides(Map.of("id", new ColumnOverride("_id", null, null)), Set.of(), null)
        );
        assertEquals("id", reserved.unmapped().get(0).column());
        assertThat(reserved.unmapped().get(0).reason(), containsString("cannot be named [_id]"));
    }
}
