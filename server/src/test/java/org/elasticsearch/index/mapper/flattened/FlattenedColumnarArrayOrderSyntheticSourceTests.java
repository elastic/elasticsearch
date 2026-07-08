/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.index.DirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.test.WildcardFieldMaskingReader;

import java.io.IOException;
import java.util.Set;

/**
 * Round-trip synthetic {@code _source} for flattened fields in strictly columnar mode.
 *
 * <p>In columnar mode, flattened values are stored via
 * {@link org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull} — one binary doc-values column that
 * encodes {@code key\0value} slots in document order (including null slots) without a separate {@code .offsets} sidecar. These tests verify
 * that the synthetic-source writer correctly reconstructs the original object, including array ordering, duplicates, interleaved nulls, and
 * multiple keys.
 */
public class FlattenedColumnarArrayOrderSyntheticSourceTests extends MapperServiceTestCase {

    private DocumentMapper columnarMapper() throws IOException {
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        return createMapperService(
            settings,
            mapping(b -> b.startObject("field").field("type", "flattened").field("preserve_leaf_arrays", "exact").endObject())
        ).documentMapper();
    }

    /**
     * The round-trip re-indexes the synthetic source, which has keys in sorted order. This produces a different
     * slot ordering in the keyed binary doc-values column than the original (whose slots reflect document order).
     * Recovery-source fields also differ when the original source is not identical to the synthetic output (e.g.
     * a lone-null array collapses to a scalar null). Mask those internal columns so the assertion focuses on
     * content equality, not encoding order or source representation.
     */
    @Override
    protected void validateRoundTripReader(String syntheticSource, DirectoryReader reader, DirectoryReader roundTripReader)
        throws IOException {
        Set<String> masked = Set.of(SourceFieldMapper.RECOVERY_SOURCE_NAME, SourceFieldMapper.RECOVERY_SOURCE_SIZE_NAME, "field._keyed*");
        assertReaderEquals(
            "round trip " + syntheticSource,
            new WildcardFieldMaskingReader(masked, reader),
            new WildcardFieldMaskingReader(masked, roundTripReader)
        );
    }

    // --- metadata ---

    public void testStoresArrayValuesInOrder() throws IOException {
        var mapper = columnarMapper();
        var fieldMapper = mapper.mappers().getMapper("field");
        assertTrue("flattened columnar path must store array values in order", fieldMapper.storesArrayValuesInOrder());
        assertNull("flattened columnar path must not use an offsets sidecar field", fieldMapper.getOffsetFieldName());
    }

    // --- single key, single value ---

    public void testSingleValueCollapsesToScalar() throws IOException {
        var mapper = columnarMapper();
        assertEquals("""
            {"field":{"key":"a"}}""", syntheticSource(mapper, b -> b.startObject("field").field("key", "a").endObject()));
    }

    // --- single key, array ---

    public void testOrderAndDuplicatesPreservedWithinKey() throws IOException {
        var mapper = columnarMapper();
        assertEquals(
            """
                {"field":{"key":["b","a","a","c"]}}""",
            syntheticSource(
                mapper,
                b -> b.startObject("field").startArray("key").value("b").value("a").value("a").value("c").endArray().endObject()
            )
        );
    }

    public void testInterleavedNullsPreservedWithinKey() throws IOException {
        var mapper = columnarMapper();
        assertEquals(
            """
                {"field":{"key":["a",null,"b"]}}""",
            syntheticSource(mapper, b -> b.startObject("field").startArray("key").value("a").nullValue().value("b").endArray().endObject())
        );
    }

    public void testAllNullArray() throws IOException {
        var mapper = columnarMapper();
        assertEquals(
            """
                {"field":{"key":[null,null]}}""",
            syntheticSource(mapper, b -> b.startObject("field").startArray("key").nullValue().nullValue().endArray().endObject())
        );
    }

    public void testLoneNull() throws IOException {
        // A single-element null array is indistinguishable from a scalar null in flattened fields
        // (no type mapping exists to distinguish them), so it round-trips as a scalar null.
        var mapper = columnarMapper();
        assertEquals(
            """
                {"field":{"key":null}}""",
            syntheticSource(mapper, b -> b.startObject("field").startArray("key").nullValue().endArray().endObject())
        );
    }

    // --- multiple keys ---

    public void testMultipleKeysSortedInOutput() throws IOException {
        // Keys appear in sorted order in synthetic _source regardless of indexing order.
        var mapper = columnarMapper();
        assertEquals(
            """
                {"field":{"a":"x","z":"y"}}""",
            syntheticSource(mapper, b -> b.startObject("field").field("z", "y").field("a", "x").endObject())
        );
    }

    public void testMultipleKeysEachWithArrayOrder() throws IOException {
        // Each key independently preserves its array ordering.
        var mapper = columnarMapper();
        assertEquals(
            """
                {"field":{"a":["c","b"],"z":["x","y"]}}""",
            syntheticSource(
                mapper,
                b -> b.startObject("field")
                    .startArray("a")
                    .value("c")
                    .value("b")
                    .endArray()
                    .startArray("z")
                    .value("x")
                    .value("y")
                    .endArray()
                    .endObject()
            )
        );
    }

    public void testMultipleKeysWithArraysAndNulls() throws IOException {
        var mapper = columnarMapper();
        assertEquals(
            """
                {"field":{"a":["v2",null,"v1"],"b":"w"}}""",
            syntheticSource(
                mapper,
                b -> b.startObject("field").startArray("a").value("v2").nullValue().value("v1").endArray().field("b", "w").endObject()
            )
        );
    }

    // --- ignore_above ---

    public void testIgnoreAboveValuesTailAppended() throws IOException {
        // Values within ignore_above are stored as ordered slots; values exceeding it are stored separately and
        // tail-appended by ArrayOrderKeyedValueProducer. This test verifies that slot ordering is preserved
        // (cc before bb, not alphabetically sorted) and that the ignored value is appended after all slot values.
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        DocumentMapper mapper = createMapperService(
            settings,
            mapping(
                b -> b.startObject("field")
                    .field("type", "flattened")
                    .field("preserve_leaf_arrays", "exact")
                    .field("ignore_above", 3)
                    .endObject()
            )
        ).documentMapper();
        assertEquals(
            """
                {"field":{"k":["cc","bb","aaaa"]}}""",
            syntheticSource(
                mapper,
                // "cc" and "bb" are within ignore_above (3); "aaaa" (4 chars) exceeds it.
                // Slot values [cc, bb] preserved in document order; ignored value [aaaa] tail-appended.
                b -> b.startObject("field").startArray("k").value("cc").value("aaaa").value("bb").endArray().endObject()
            )
        );
    }

    public void testIgnoreAboveOnlyIgnoredValues() throws IOException {
        // When every value for a key exceeds ignore_above, the key does not appear in slotsByKey at all.
        // The ArrayOrderKeyedValueProducer union of slotsByKey and ignoredByKey must still emit the key.
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        DocumentMapper mapper = createMapperService(
            settings,
            mapping(
                b -> b.startObject("field")
                    .field("type", "flattened")
                    .field("preserve_leaf_arrays", "exact")
                    .field("ignore_above", 3)
                    .endObject()
            )
        ).documentMapper();
        assertEquals("""
            {"field":{"k":"toolong"}}""", syntheticSource(mapper, b -> b.startObject("field").field("k", "toolong").endObject()));
    }
}
