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
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.fieldvisitor.StoredFieldsLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class LeafStoredFieldsLookupTests extends ESTestCase {

    private static FieldInfo fieldInfo(String field) {
        return new FieldInfo(field, 1, false, false, true, IndexOptions.NONE, DocValuesType.NONE,
            -1, Collections.emptyMap(), 0, 0, 0, false);
    }

    private static class MockFieldType extends MappedFieldType {

        MockFieldType(String name, boolean stored) {
            super(name, false, stored, false, TextSearchInfo.NONE, null);
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return null;
        }

        @Override
        public String typeName() {
            return null;
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            return null;
        }

        @Override
        public Object valueForDisplay(Object value) {
            return (double) value + 10;
        }

    }

    public void testBasicLookup() {

        LeafStoredFieldsLookup fieldsLookup = new LeafStoredFieldsLookup(
            f -> {
                switch (f) {
                    case "stored":
                        return new MockFieldType("stored", true);
                    case "unstored":
                        return new MockFieldType("unstored", false);
                    default:
                        return null;
                }
            },
            (doc, visitor) -> visitor.doubleField(fieldInfo("stored"), 2.718)
        );

        assertTrue(fieldsLookup.containsKey("stored"));
        assertFalse(fieldsLookup.containsKey("unstored"));
        assertFalse(fieldsLookup.containsKey("undefined"));

        FieldLookup fieldLookup = (FieldLookup) fieldsLookup.get("stored");
        assertEquals("stored", fieldLookup.fieldType().name());

        List<Object> values = fieldLookup.getValues();
        assertNotNull(values);
        assertEquals(1, values.size());
        assertEquals(12.718, values.get(0));

        assertNull(fieldsLookup.get("unstored"));
        assertNull(fieldsLookup.get("undefined"));
    }

    public void testLaziness() {
        AtomicInteger loadCount = new AtomicInteger();
        StoredFieldsLoader fieldsLoader = (doc, fieldsVisitor) -> {
            assertEquals(StoredFieldVisitor.Status.YES, fieldsVisitor.needsField(fieldInfo("field1")));
            assertEquals(StoredFieldVisitor.Status.YES, fieldsVisitor.needsField(fieldInfo("field2")));
            assertEquals(StoredFieldVisitor.Status.NO, fieldsVisitor.needsField(fieldInfo("field3")));
            assertEquals(StoredFieldVisitor.Status.NO, fieldsVisitor.needsField(fieldInfo("undefined")));
            fieldsVisitor.doubleField(fieldInfo("field1"), 1);
            fieldsVisitor.doubleField(fieldInfo("field2"), 2);
            loadCount.incrementAndGet();
        };

        Map<String, MappedFieldType> fields = new HashMap<>();
        fields.put("field1", new MockFieldType("field1", true));
        fields.put("field2", new MockFieldType("field2", true));
        fields.put("field3", new MockFieldType("field3", false));

        LeafStoredFieldsLookup lookup = new LeafStoredFieldsLookup(fields::get, fieldsLoader);
        fields.forEach((k, v) -> {
            if (v.isStored()) {
                lookup.registerFieldToLoad(v);
            }
        });

        // With all fields pre-registered, then we should do at most one
        // load of data per document.  Undefined fields or fields that
        // are marked as unstored will shortcut and not trigger a stored
        // fields read
        lookup.setDocument(1);
        assertEquals(0, loadCount.get());
        assertFalse(lookup.containsKey("field3"));  // non-stored field, no read required
        assertNull(lookup.get("field3"));
        assertEquals(0, loadCount.get());
        assertTrue(lookup.containsKey("field1"));   // stored field, we read data
        assertEquals(12.0, ((FieldLookup)lookup.get("field2")).getValue());
        assertEquals(1, loadCount.get());
        assertTrue(lookup.containsKey("field2"));   // data is already read, no need to do it again
        assertEquals(1, loadCount.get());

        lookup.setDocument(2);
        assertEquals(1, loadCount.get());   // we don't read data until we're asked
        assertFalse(lookup.containsKey("undefined"));
        assertEquals(1, loadCount.get());   // we don't need to read data for an undefined field
        assertTrue(lookup.containsKey("field1"));
        assertEquals(2, loadCount.get());   // stored field requires a data read
        assertTrue(lookup.containsKey("field2"));
        assertEquals(2, loadCount.get());   // but once we've read once, we don't need to do it again

        assertFalse(lookup.containsKey("undefined"));   // undefined field doesn't change anything
        assertEquals(2, loadCount.get());

        // now we ask for a field that was not pre-registered
        // this will trigger another read
        fields.put("field4", new MockFieldType("field4", true));
        assertFalse(lookup.containsKey("field4"));      // no data present for this field, so returns false
        assertEquals(3, loadCount.get());      // but we needed to hit the disk to confirm

        // if it's a non-stored field though, we can detect that up front
        // and ignore
        fields.put("field5", new MockFieldType("field5", false));
        assertFalse(lookup.containsKey("field5"));
        assertEquals(3, loadCount.get());

        // on the next document, field4 will get loaded up-front
        lookup.setDocument(3);
        assertEquals(3, loadCount.get());
        assertTrue(lookup.containsKey("field1"));
        assertEquals(4, loadCount.get());
        assertFalse(lookup.containsKey("field4"));
        assertEquals(4, loadCount.get());
    }


}
