/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.lookup;

import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
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

    /** The read and enumeration methods reject the call with a client error; the mutators are unsupported. */
    public void testEnumerationThrowsIllegalArgument() {
        LeafStoredFieldsLookup fieldsLookup = buildFieldsLookup();
        FieldLookup fieldLookup = fieldsLookup.get("field");
        assertNotEnumerable(fieldsLookup::size, "size");
        assertNotEnumerable(fieldsLookup::isEmpty, "isEmpty");
        assertNotEnumerable(() -> fieldsLookup.containsValue(fieldLookup), "containsValue");
        assertNotEnumerable(fieldsLookup::keySet, "keySet");
        assertNotEnumerable(fieldsLookup::values, "values");
        assertNotEnumerable(fieldsLookup::entrySet, "entrySet");

        expectThrows(UnsupportedOperationException.class, () -> fieldsLookup.put("field", fieldLookup));
        expectThrows(UnsupportedOperationException.class, () -> fieldsLookup.remove("field"));
        expectThrows(UnsupportedOperationException.class, () -> fieldsLookup.putAll(Map.of()));
        expectThrows(UnsupportedOperationException.class, fieldsLookup::clear);
    }

    /** A map holding the lookup, such as a script's {@code params}, is rejected by the self-reference check. */
    public void testEnsureNoSelfReferencesRejectsStoredFieldsLookup() {
        Map<String, Object> params = new HashMap<>();
        params.put("_fields", buildFieldsLookup());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> CollectionUtils.ensureNoSelfReferences(params, "test")
        );
        assertThat(e.getMessage(), containsString("[_fields] does not support [isEmpty]"));
    }

    private static void assertNotEnumerable(ThrowingRunnable call, String method) {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, call);
        assertThat(e.getMessage(), containsString("[_fields] does not support [" + method + "]"));
        assertThat(e.getMessage(), containsString("params._fields['field']"));
    }
}
