/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.lookup;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.List;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LeafStoredFieldsLookupTests extends ESTestCase {
    private LeafStoredFieldsLookup fieldsLookup;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(fieldType.name()).thenReturn("field");
        // Add 10 when valueForDisplay is called so it is easy to be sure it *was* called
        when(fieldType.valueForDisplay(anyObject())).then(invocation ->
                (Double) invocation.getArguments()[0] + 10);

        FieldInfo mockFieldInfo = new FieldInfo("field", 1, false, false, true,
            IndexOptions.NONE, DocValuesType.NONE, -1, Collections.emptyMap(), 0, 0, 0, 0, VectorSimilarityFunction.EUCLIDEAN, false);

        fieldsLookup = new LeafStoredFieldsLookup(field -> field.equals("field") || field.equals("alias") ? fieldType : null,
            (doc, visitor) -> visitor.doubleField(mockFieldInfo, 2.718));
    }

    public void testBasicLookup() {
        FieldLookup fieldLookup = (FieldLookup) fieldsLookup.get("field");
        assertEquals("field", fieldLookup.fieldType().name());

        List<Object> values = fieldLookup.getValues();
        assertNotNull(values);
        assertEquals(1, values.size());
        assertEquals(12.718, values.get(0));
    }

    public void testLookupWithFieldAlias() {
        FieldLookup fieldLookup = (FieldLookup) fieldsLookup.get("alias");
        assertEquals("field", fieldLookup.fieldType().name());

        List<Object> values = fieldLookup.getValues();
        assertNotNull(values);
        assertEquals(1, values.size());
        assertEquals(12.718, values.get(0));
    }
}
