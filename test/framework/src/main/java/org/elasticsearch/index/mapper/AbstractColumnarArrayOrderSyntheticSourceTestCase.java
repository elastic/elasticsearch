/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;

import java.io.IOException;

/**
 * Shared round-trip coverage for high-cardinality fields in strictly columnar mode that store their values in document order with inline
 * nulls ({@link MultiValuedBinaryDocValuesField.ArrayOrderInlineNull}) instead of a sidecar {@code .offsets} field. Concrete subclasses
 * supply the field type (keyword, text, match_only_text). Exercises order preservation, duplicates, interleaved nulls, lone/all nulls,
 * empty arrays, and the empty-string-vs-null distinction.
 */
public abstract class AbstractColumnarArrayOrderSyntheticSourceTestCase extends MapperServiceTestCase {

    /**
     * The {@code type} of the field under test; the field is always named {@code field} and is mapped with its columnar-mode defaults.
     */
    protected abstract String fieldTypeName();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        assumeTrue("columnar index mode requires a snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        assumeTrue(
            "in-order binary doc values require the extended doc values feature flag",
            FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled()
        );
    }

    protected DocumentMapper columnarMapper() throws IOException {
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        return createMapperService(settings, mapping(b -> b.startObject("field").field("type", fieldTypeName()).endObject()))
            .documentMapper();
    }

    public void testOffsetsFieldNotUsed() throws IOException {
        var mapper = columnarMapper();
        // The high-cardinality columnar path stores values in order and must not allocate a sidecar offsets field.
        assertNull(mapper.mappers().getMapper("field").getOffsetFieldName());
        assertTrue(mapper.mappers().getMapper("field").storesArrayValuesInOrder());
    }

    public void testOrderAndDuplicatesPreserved() throws IOException {
        var mapper = columnarMapper();
        assertEquals("""
            {"field":["b","a","a","c"]}""", syntheticSource(mapper, b -> b.array("field", "b", "a", "a", "c")));
    }

    public void testSingleValueCollapsesToScalar() throws IOException {
        var mapper = columnarMapper();
        assertEquals("""
            {"field":"a"}""", syntheticSource(mapper, b -> b.array("field", "a")));
        assertEquals("""
            {"field":"a"}""", syntheticSource(mapper, b -> b.field("field", "a")));
    }

    public void testInterleavedNullsPreserved() throws IOException {
        var mapper = columnarMapper();
        assertEquals(
            """
                {"field":["a",null,"b"]}""",
            syntheticSource(mapper, b -> { b.startArray("field").value("a").nullValue().value("b").endArray(); })
        );
    }

    public void testAllNullArray() throws IOException {
        var mapper = columnarMapper();
        assertEquals("""
            {"field":[null,null]}""", syntheticSource(mapper, b -> b.startArray("field").nullValue().nullValue().endArray()));
    }

    public void testLoneNull() throws IOException {
        var mapper = columnarMapper();
        assertEquals("""
            {"field":[null]}""", syntheticSource(mapper, b -> b.startArray("field").nullValue().endArray()));
    }

    public void testEmptyArray() throws IOException {
        var mapper = columnarMapper();
        assertEquals("""
            {"field":[]}""", syntheticSource(mapper, b -> b.startArray("field").endArray()));
    }

    public void testEmptyStringDistinctFromNull() throws IOException {
        var mapper = columnarMapper();
        assertEquals("""
            {"field":""}""", syntheticSource(mapper, b -> b.array("field", "")));
        assertEquals("""
            {"field":["a","",null,"b"]}""", syntheticSource(mapper, b -> {
            b.startArray("field").value("a").value("").nullValue().value("b").endArray();
        }));
    }
}
