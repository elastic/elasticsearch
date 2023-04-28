/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.lookup;

import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LeafStoredFieldsLookupTests extends ESTestCase {

    private LeafStoredFieldsLookup buildFieldsLookup() {
        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(fieldType.name()).thenReturn("field");
        // Add 10 when valueForDisplay is called so it is easy to be sure it *was* called
        when(fieldType.valueForDisplay(any())).then(invocation -> (Double) invocation.getArguments()[0] + 10);

        return new LeafStoredFieldsLookup(
            field -> field.equals("field") || field.equals("alias") ? fieldType : null,
            (fieldLookup, doc) -> fieldLookup.setValues(List.of(2.718))
        );
    }

    public void testBasicLookup() {
        LeafStoredFieldsLookup fieldsLookup = buildFieldsLookup();
        FieldLookup fieldLookup = fieldsLookup.get("field");
        assertEquals("field", fieldLookup.fieldType().name());

        List<Object> values = fieldLookup.getValues();
        assertNotNull(values);
        assertEquals(1, values.size());
        assertEquals(12.718, values.get(0));
    }

    public void testLookupWithFieldAlias() {
        LeafStoredFieldsLookup fieldsLookup = buildFieldsLookup();
        FieldLookup fieldLookup = fieldsLookup.get("alias");
        assertEquals("field", fieldLookup.fieldType().name());

        List<Object> values = fieldLookup.getValues();
        assertNotNull(values);
        assertEquals(1, values.size());
        assertEquals(12.718, values.get(0));
    }
}
