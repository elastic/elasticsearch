/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.DoubleStream;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class IngestDocumentTests extends ESTestCase {

    private static final ZonedDateTime BOGUS_TIMESTAMP = ZonedDateTime.of(2016, 10, 23, 0, 0, 0, 0, ZoneOffset.UTC);
    private IngestDocument ingestDocument;
    private static final String DOUBLE_ARRAY_FIELD = "double_array_field";
    private static final String DOUBLE_DOUBLE_ARRAY_FIELD = "double_double_array";

    @Before
    public void setTestIngestDocument() {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> ingestMap = new HashMap<>();
        ingestMap.put("timestamp", BOGUS_TIMESTAMP);
        document.put("_ingest", ingestMap);
        document.put("foo", "bar");
        document.put("int", 123);
        Map<String, Object> innerObject = new HashMap<>();
        innerObject.put("buzz", "hello world");
        innerObject.put("foo_null", null);
        innerObject.put("1", "bar");
        List<String> innerInnerList = new ArrayList<>();
        innerInnerList.add("item1");
        List<Object> innerList = new ArrayList<>();
        innerList.add(innerInnerList);
        innerObject.put("list", innerList);
        document.put("fizz", innerObject);
        List<Map<String, Object>> list = new ArrayList<>();
        Map<String, Object> value = new HashMap<>();
        value.put("field", "value");
        list.add(value);
        list.add(null);
        document.put("list", list);

        List<String> list2 = new ArrayList<>();
        list2.add("foo");
        list2.add("bar");
        list2.add("baz");
        document.put("list2", list2);
        document.put(DOUBLE_ARRAY_FIELD, DoubleStream.generate(ESTestCase::randomDouble).limit(randomInt(1000)).toArray());
        document.put(
            DOUBLE_DOUBLE_ARRAY_FIELD,
            new double[][] {
                DoubleStream.generate(ESTestCase::randomDouble).limit(randomInt(1000)).toArray(),
                DoubleStream.generate(ESTestCase::randomDouble).limit(randomInt(1000)).toArray(),
                DoubleStream.generate(ESTestCase::randomDouble).limit(randomInt(1000)).toArray() }
        );

        ingestDocument = new IngestDocument("index", "id", 1, null, null, document);
    }

    public void testSimpleGetFieldValue() {
        assertThat(ingestDocument.getFieldValue("foo", String.class), equalTo("bar"));
        assertThat(ingestDocument.getFieldValue("int", Integer.class), equalTo(123));
        assertThat(ingestDocument.getFieldValue("_source.foo", String.class), equalTo("bar"));
        assertThat(ingestDocument.getFieldValue("_source.int", Integer.class), equalTo(123));
        assertThat(ingestDocument.getFieldValue("_index", String.class), equalTo("index"));
        assertThat(ingestDocument.getFieldValue("_id", String.class), equalTo("id"));
        assertThat(
            ingestDocument.getFieldValue("_ingest.timestamp", ZonedDateTime.class),
            both(notNullValue()).and(not(equalTo(BOGUS_TIMESTAMP)))
        );
        assertThat(ingestDocument.getFieldValue("_source._ingest.timestamp", ZonedDateTime.class), equalTo(BOGUS_TIMESTAMP));
    }

    public void testGetDottedField() {
        ingestDocument.getCtxMap().put("a.b.c", "foo");
        assertThat(ingestDocument.getFieldValue("a.b.c", String.class), equalTo("foo"));
    }

    public void testGetSourceObject() {
        try {
            ingestDocument.getFieldValue("_source", Object.class);
            fail("get field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [_source] not present as part of path [_source]"));
        }
    }

    public void testGetIngestObject() {
        assertThat(ingestDocument.getFieldValue("_ingest", Map.class), notNullValue());
    }

    public void testGetEmptyPathAfterStrippingOutPrefix() {
        try {
            ingestDocument.getFieldValue("_source.", Object.class);
            fail("get field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [_source.] is not valid"));
        }

        try {
            ingestDocument.getFieldValue("_ingest.", Object.class);
            fail("get field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [_ingest.] is not valid"));
        }
    }

    public void testGetFieldValueNullValue() {
        assertThat(ingestDocument.getFieldValue("fizz.foo_null", Object.class), nullValue());
    }

    public void testSimpleGetFieldValueTypeMismatch() {
        try {
            ingestDocument.getFieldValue("int", String.class);
            fail("getFieldValue should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [int] of type [java.lang.Integer] cannot be cast to [java.lang.String]"));
        }

        try {
            ingestDocument.getFieldValue("foo", Integer.class);
            fail("getFieldValue should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [foo] of type [java.lang.String] cannot be cast to [java.lang.Integer]"));
        }
    }

    public void testNestedGetFieldValue() {
        assertThat(ingestDocument.getFieldValue("fizz.buzz", String.class), equalTo("hello world"));
        assertThat(ingestDocument.getFieldValue("fizz.1", String.class), equalTo("bar"));
    }

    public void testNestedGetFieldValueTypeMismatch() {
        try {
            ingestDocument.getFieldValue("foo.foo.bar", String.class);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [foo.foo] not present as part of path [foo.foo.bar]"));
        }
    }

    public void testListGetFieldValue() {
        assertThat(ingestDocument.getFieldValue("list.0.field", String.class), equalTo("value"));
    }

    public void testListGetFieldValueNull() {
        assertThat(ingestDocument.getFieldValue("list.1", String.class), nullValue());
    }

    public void testListGetFieldValueIndexNotNumeric() {
        try {
            ingestDocument.getFieldValue("list.test.field", String.class);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[test] is not an integer, cannot be used as an index as part of path [list.test.field]"));
        }
    }

    public void testListGetFieldValueIndexOutOfBounds() {
        try {
            ingestDocument.getFieldValue("list.10.field", String.class);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[10] is out of bounds for array with length [2] as part of path [list.10.field]"));
        }
    }

    public void testGetFieldValueNotFound() {
        try {
            ingestDocument.getFieldValue("not.here", String.class);
            fail("get field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [not] not present as part of path [not.here]"));
        }
    }

    public void testGetFieldValueNotFoundNullParent() {
        try {
            ingestDocument.getFieldValue("fizz.foo_null.not_there", String.class);
            fail("get field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [not_there] not present as part of path [fizz.foo_null.not_there]"));
        }
    }

    public void testGetFieldValueNull() {
        try {
            ingestDocument.getFieldValue((String) null, String.class);
            fail("get field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
        }
    }

    public void testGetFieldValueEmpty() {
        try {
            ingestDocument.getFieldValue("", String.class);
            fail("get field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
        }
    }

    public void testHasField() {
        assertTrue(ingestDocument.hasField("fizz"));
        assertTrue(ingestDocument.hasField("_index"));
        assertTrue(ingestDocument.hasField("_id"));
        assertTrue(ingestDocument.hasField("_source.fizz"));
        assertTrue(ingestDocument.hasField("_ingest.timestamp"));
    }

    public void testHasFieldNested() {
        assertTrue(ingestDocument.hasField("fizz.buzz"));
        assertTrue(ingestDocument.hasField("_source._ingest.timestamp"));
    }

    public void testListHasField() {
        assertTrue(ingestDocument.hasField("list.0.field"));
    }

    public void testListHasFieldNull() {
        assertTrue(ingestDocument.hasField("list.1"));
    }

    public void testListHasFieldIndexOutOfBounds() {
        assertFalse(ingestDocument.hasField("list.10"));
    }

    public void testListHasFieldIndexOutOfBounds_fail() {
        assertTrue(ingestDocument.hasField("list.0", true));
        assertTrue(ingestDocument.hasField("list.1", true));
        Exception e = expectThrows(IllegalArgumentException.class, () -> ingestDocument.hasField("list.2", true));
        assertThat(e.getMessage(), equalTo("[2] is out of bounds for array with length [2] as part of path [list.2]"));
        e = expectThrows(IllegalArgumentException.class, () -> ingestDocument.hasField("list.10", true));
        assertThat(e.getMessage(), equalTo("[10] is out of bounds for array with length [2] as part of path [list.10]"));
    }

    public void testListHasFieldIndexNotNumeric() {
        assertFalse(ingestDocument.hasField("list.test"));
    }

    public void testNestedHasFieldTypeMismatch() {
        assertFalse(ingestDocument.hasField("foo.foo.bar"));
    }

    public void testHasFieldNotFound() {
        assertFalse(ingestDocument.hasField("not.here"));
    }

    public void testHasFieldNotFoundNullParent() {
        assertFalse(ingestDocument.hasField("fizz.foo_null.not_there"));
    }

    public void testHasFieldNestedNotFound() {
        assertFalse(ingestDocument.hasField("fizz.doesnotexist"));
    }

    public void testHasFieldNull() {
        try {
            ingestDocument.hasField((String) null);
            fail("has field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
        }
    }

    public void testHasFieldNullValue() {
        assertTrue(ingestDocument.hasField("fizz.foo_null"));
    }

    public void testHasFieldEmpty() {
        try {
            ingestDocument.hasField("");
            fail("has field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
        }
    }

    public void testHasFieldSourceObject() {
        assertThat(ingestDocument.hasField("_source"), equalTo(false));
    }

    public void testHasFieldIngestObject() {
        assertThat(ingestDocument.hasField("_ingest"), equalTo(true));
    }

    public void testHasFieldEmptyPathAfterStrippingOutPrefix() {
        try {
            ingestDocument.hasField("_source.");
            fail("has field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [_source.] is not valid"));
        }

        try {
            ingestDocument.hasField("_ingest.");
            fail("has field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [_ingest.] is not valid"));
        }
    }

    public void testSimpleSetFieldValue() {
        ingestDocument.setFieldValue("new_field", "foo");
        assertThat(ingestDocument.getSourceAndMetadata().get("new_field"), equalTo("foo"));
        ingestDocument.setFieldValue("_ttl", "ttl");
        assertThat(ingestDocument.getSourceAndMetadata().get("_ttl"), equalTo("ttl"));
        ingestDocument.setFieldValue("_source.another_field", "bar");
        assertThat(ingestDocument.getSourceAndMetadata().get("another_field"), equalTo("bar"));
        ingestDocument.setFieldValue("_ingest.new_field", "new_value");
        assertThat(ingestDocument.getIngestMetadata().size(), equalTo(2));
        assertThat(ingestDocument.getIngestMetadata().get("new_field"), equalTo("new_value"));
        ingestDocument.setFieldValue("_ingest.timestamp", "timestamp");
        assertThat(ingestDocument.getIngestMetadata().get("timestamp"), equalTo("timestamp"));
    }

    public void testSetFieldValueNullValue() {
        ingestDocument.setFieldValue("new_field", null);
        assertThat(ingestDocument.getSourceAndMetadata().containsKey("new_field"), equalTo(true));
        assertThat(ingestDocument.getSourceAndMetadata().get("new_field"), nullValue());
    }

    @SuppressWarnings("unchecked")
    public void testNestedSetFieldValue() {
        ingestDocument.setFieldValue("a.b.c.d", "foo");
        assertThat(ingestDocument.getSourceAndMetadata().get("a"), instanceOf(Map.class));
        Map<String, Object> a = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("a");
        assertThat(a.get("b"), instanceOf(Map.class));
        Map<String, Object> b = (Map<String, Object>) a.get("b");
        assertThat(b.get("c"), instanceOf(Map.class));
        Map<String, Object> c = (Map<String, Object>) b.get("c");
        assertThat(c.get("d"), instanceOf(String.class));
        String d = (String) c.get("d");
        assertThat(d, equalTo("foo"));
    }

    public void testSetFieldValueOnExistingField() {
        ingestDocument.setFieldValue("foo", "newbar");
        assertThat(ingestDocument.getSourceAndMetadata().get("foo"), equalTo("newbar"));
    }

    @SuppressWarnings("unchecked")
    public void testSetFieldValueOnExistingParent() {
        ingestDocument.setFieldValue("fizz.new", "bar");
        assertThat(ingestDocument.getSourceAndMetadata().get("fizz"), instanceOf(Map.class));
        Map<String, Object> innerMap = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("fizz");
        assertThat(innerMap.get("new"), instanceOf(String.class));
        String value = (String) innerMap.get("new");
        assertThat(value, equalTo("bar"));
    }

    public void testSetFieldValueOnExistingParentTypeMismatch() {
        try {
            ingestDocument.setFieldValue("fizz.buzz.new", "bar");
            fail("add field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(
                e.getMessage(),
                equalTo(
                    "cannot create child of [1:'buzz'] with value [hello world] of type [java.lang.String] as part of path [fizz.buzz.new]"
                )
            );
        }
    }

    public void testSetFieldValueOnExistingNullParent() {
        try {
            ingestDocument.setFieldValue("fizz.foo_null.test", "bar");
            fail("add field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(
                e.getMessage(),
                equalTo("cannot create child of [1:'foo_null'] with value [null] of type [null] as part of path [fizz.foo_null.test]")
            );
        }
    }

    public void testSetFieldValueNullName() {
        try {
            ingestDocument.setFieldValue(null, "bar");
            fail("add field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
        }
    }

    public void testSetSourceObject() {
        ingestDocument.setFieldValue("_source", "value");
        assertThat(ingestDocument.getSourceAndMetadata().get("_source"), equalTo("value"));
    }

    public void testSetIngestObject() {
        ingestDocument.setFieldValue("_ingest", "value");
        assertThat(ingestDocument.getSourceAndMetadata().get("_ingest"), equalTo("value"));
    }

    public void testSetIngestSourceObject() {
        // test that we don't strip out the _source prefix when _ingest is used
        ingestDocument.setFieldValue("_ingest._source", "value");
        assertThat(ingestDocument.getIngestMetadata().get("_source"), equalTo("value"));
    }

    public void testSetEmptyPathAfterStrippingOutPrefix() {
        try {
            ingestDocument.setFieldValue("_source.", "value");
            fail("set field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [_source.] is not valid"));
        }

        try {
            ingestDocument.setFieldValue("_ingest.", "_value");
            fail("set field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [_ingest.] is not valid"));
        }
    }

    public void testListSetFieldValueNoIndexProvided() {
        ingestDocument.setFieldValue("list", "value");
        Object object = ingestDocument.getSourceAndMetadata().get("list");
        assertThat(object, instanceOf(String.class));
        assertThat(object, equalTo("value"));
    }

    public void testListAppendFieldValue() {
        ingestDocument.appendFieldValue("list", "new_value");
        Object object = ingestDocument.getSourceAndMetadata().get("list");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(3));
        assertThat(list.get(0), equalTo(Map.of("field", "value")));
        assertThat(list.get(1), nullValue());
        assertThat(list.get(2), equalTo("new_value"));
    }

    public void testListAppendFieldValueWithDuplicate() {
        ingestDocument.appendFieldValue("list2", "foo", false);
        Object object = ingestDocument.getSourceAndMetadata().get("list2");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(3));
        assertThat(list, equalTo(List.of("foo", "bar", "baz")));
    }

    public void testListAppendFieldValueWithoutDuplicate() {
        ingestDocument.appendFieldValue("list2", "foo2", false);
        Object object = ingestDocument.getSourceAndMetadata().get("list2");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(4));
        assertThat(list, equalTo(List.of("foo", "bar", "baz", "foo2")));
    }

    public void testListAppendFieldValues() {
        ingestDocument.appendFieldValue("list", List.of("item1", "item2", "item3"));
        Object object = ingestDocument.getSourceAndMetadata().get("list");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(5));
        assertThat(list.get(0), equalTo(Map.of("field", "value")));
        assertThat(list.get(1), nullValue());
        assertThat(list.get(2), equalTo("item1"));
        assertThat(list.get(3), equalTo("item2"));
        assertThat(list.get(4), equalTo("item3"));
    }

    public void testListAppendFieldValuesWithoutDuplicates() {
        ingestDocument.appendFieldValue("list2", List.of("foo", "bar", "baz", "foo2"), false);
        Object object = ingestDocument.getSourceAndMetadata().get("list2");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(4));
        assertThat(list.get(0), equalTo("foo"));
        assertThat(list.get(1), equalTo("bar"));
        assertThat(list.get(2), equalTo("baz"));
        assertThat(list.get(3), equalTo("foo2"));
    }

    public void testAppendFieldValueToNonExistingList() {
        ingestDocument.appendFieldValue("non_existing_list", "new_value");
        Object object = ingestDocument.getSourceAndMetadata().get("non_existing_list");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(1));
        assertThat(list.get(0), equalTo("new_value"));
    }

    public void testAppendFieldValuesToNonExistingList() {
        ingestDocument.appendFieldValue("non_existing_list", List.of("item1", "item2", "item3"));
        Object object = ingestDocument.getSourceAndMetadata().get("non_existing_list");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(3));
        assertThat(list.get(0), equalTo("item1"));
        assertThat(list.get(1), equalTo("item2"));
        assertThat(list.get(2), equalTo("item3"));
    }

    public void testAppendFieldValueConvertStringToList() {
        ingestDocument.appendFieldValue("fizz.buzz", "new_value");
        Object object = ingestDocument.getSourceAndMetadata().get("fizz");
        assertThat(object, instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) object;
        object = map.get("buzz");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(2));
        assertThat(list.get(0), equalTo("hello world"));
        assertThat(list.get(1), equalTo("new_value"));
    }

    public void testAppendFieldValuesConvertStringToList() {
        ingestDocument.appendFieldValue("fizz.buzz", List.of("item1", "item2", "item3"));
        Object object = ingestDocument.getSourceAndMetadata().get("fizz");
        assertThat(object, instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) object;
        object = map.get("buzz");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(4));
        assertThat(list.get(0), equalTo("hello world"));
        assertThat(list.get(1), equalTo("item1"));
        assertThat(list.get(2), equalTo("item2"));
        assertThat(list.get(3), equalTo("item3"));
    }

    public void testAppendFieldValueConvertIntegerToList() {
        ingestDocument.appendFieldValue("int", 456);
        Object object = ingestDocument.getSourceAndMetadata().get("int");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(2));
        assertThat(list.get(0), equalTo(123));
        assertThat(list.get(1), equalTo(456));
    }

    public void testAppendFieldValuesConvertIntegerToList() {
        ingestDocument.appendFieldValue("int", List.of(456, 789));
        Object object = ingestDocument.getSourceAndMetadata().get("int");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(3));
        assertThat(list.get(0), equalTo(123));
        assertThat(list.get(1), equalTo(456));
        assertThat(list.get(2), equalTo(789));
    }

    public void testAppendFieldValueConvertMapToList() {
        ingestDocument.appendFieldValue("fizz", Map.of("field", "value"));
        Object object = ingestDocument.getSourceAndMetadata().get("fizz");
        assertThat(object, instanceOf(List.class));
        List<?> list = (List<?>) object;
        assertThat(list.size(), equalTo(2));
        assertThat(list.get(0), instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) list.get(0);
        assertThat(map.size(), equalTo(4));
        assertThat(list.get(1), equalTo(Map.of("field", "value")));
    }

    public void testAppendFieldValueToNull() {
        ingestDocument.appendFieldValue("fizz.foo_null", "new_value");
        Object object = ingestDocument.getSourceAndMetadata().get("fizz");
        assertThat(object, instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) object;
        object = map.get("foo_null");
        assertThat(object, instanceOf(List.class));
        List<?> list = (List<?>) object;
        assertThat(list.size(), equalTo(2));
        assertThat(list.get(0), nullValue());
        assertThat(list.get(1), equalTo("new_value"));
    }

    public void testAppendFieldValueToListElement() {
        ingestDocument.appendFieldValue("fizz.list.0", "item2");
        Object object = ingestDocument.getSourceAndMetadata().get("fizz");
        assertThat(object, instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) object;
        object = map.get("list");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(1));
        object = list.get(0);
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<String> innerList = (List<String>) object;
        assertThat(innerList.size(), equalTo(2));
        assertThat(innerList.get(0), equalTo("item1"));
        assertThat(innerList.get(1), equalTo("item2"));
    }

    public void testAppendFieldValuesToListElement() {
        ingestDocument.appendFieldValue("fizz.list.0", List.of("item2", "item3", "item4"));
        Object object = ingestDocument.getSourceAndMetadata().get("fizz");
        assertThat(object, instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) object;
        object = map.get("list");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(1));
        object = list.get(0);
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<String> innerList = (List<String>) object;
        assertThat(innerList.size(), equalTo(4));
        assertThat(innerList.get(0), equalTo("item1"));
        assertThat(innerList.get(1), equalTo("item2"));
        assertThat(innerList.get(2), equalTo("item3"));
        assertThat(innerList.get(3), equalTo("item4"));
    }

    public void testAppendFieldValueConvertStringListElementToList() {
        ingestDocument.appendFieldValue("fizz.list.0.0", "new_value");
        Object object = ingestDocument.getSourceAndMetadata().get("fizz");
        assertThat(object, instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) object;
        object = map.get("list");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(1));
        object = list.get(0);
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> innerList = (List<Object>) object;
        object = innerList.get(0);
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<String> innerInnerList = (List<String>) object;
        assertThat(innerInnerList.size(), equalTo(2));
        assertThat(innerInnerList.get(0), equalTo("item1"));
        assertThat(innerInnerList.get(1), equalTo("new_value"));
    }

    public void testAppendFieldValuesConvertStringListElementToList() {
        ingestDocument.appendFieldValue("fizz.list.0.0", List.of("item2", "item3", "item4"));
        Object object = ingestDocument.getSourceAndMetadata().get("fizz");
        assertThat(object, instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) object;
        object = map.get("list");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(1));
        object = list.get(0);
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> innerList = (List<Object>) object;
        object = innerList.get(0);
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<String> innerInnerList = (List<String>) object;
        assertThat(innerInnerList.size(), equalTo(4));
        assertThat(innerInnerList.get(0), equalTo("item1"));
        assertThat(innerInnerList.get(1), equalTo("item2"));
        assertThat(innerInnerList.get(2), equalTo("item3"));
        assertThat(innerInnerList.get(3), equalTo("item4"));
    }

    public void testAppendFieldValueListElementConvertMapToList() {
        ingestDocument.appendFieldValue("list.0", Map.of("item2", "value2"));
        Object object = ingestDocument.getSourceAndMetadata().get("list");
        assertThat(object, instanceOf(List.class));
        List<?> list = (List<?>) object;
        assertThat(list.size(), equalTo(2));
        assertThat(list.get(0), instanceOf(List.class));
        assertThat(list.get(1), nullValue());
        list = (List<?>) list.get(0);
        assertThat(list.size(), equalTo(2));
        assertThat(list.get(0), equalTo(Map.of("field", "value")));
        assertThat(list.get(1), equalTo(Map.of("item2", "value2")));
    }

    public void testAppendFieldValueToNullListElement() {
        ingestDocument.appendFieldValue("list.1", "new_value");
        Object object = ingestDocument.getSourceAndMetadata().get("list");
        assertThat(object, instanceOf(List.class));
        List<?> list = (List<?>) object;
        assertThat(list.get(1), instanceOf(List.class));
        list = (List<?>) list.get(1);
        assertThat(list.size(), equalTo(2));
        assertThat(list.get(0), nullValue());
        assertThat(list.get(1), equalTo("new_value"));
    }

    public void testAppendFieldValueToListOfMaps() {
        ingestDocument.appendFieldValue("list", Map.of("item2", "value2"));
        Object object = ingestDocument.getSourceAndMetadata().get("list");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(3));
        assertThat(list.get(0), equalTo(Map.of("field", "value")));
        assertThat(list.get(1), nullValue());
        assertThat(list.get(2), equalTo(Map.of("item2", "value2")));
    }

    public void testListSetFieldValueIndexProvided() {
        ingestDocument.setFieldValue("list.1", "value");
        Object object = ingestDocument.getSourceAndMetadata().get("list");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(2));
        assertThat(list.get(0), equalTo(Map.of("field", "value")));
        assertThat(list.get(1), equalTo("value"));
    }

    public void testSetFieldValueListAsPartOfPath() {
        ingestDocument.setFieldValue("list.0.field", "new_value");
        Object object = ingestDocument.getSourceAndMetadata().get("list");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(2));
        assertThat(list.get(0), equalTo(Map.of("field", "new_value")));
        assertThat(list.get(1), nullValue());
    }

    public void testListSetFieldValueIndexNotNumeric() {
        try {
            ingestDocument.setFieldValue("list.test", "value");
            fail("set field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[test] is not an integer, cannot be used as an index as part of path [list.test]"));
        }

        try {
            ingestDocument.setFieldValue("list.test.field", "new_value");
            fail("set field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(
                e.getMessage(),
                equalTo(
                    "cannot create child of [0:'list'] with value [[{field=value}, null]] of type [java.util.ArrayList] as part of path " +
                    "[list.test.field]"
                )
            );
        }
    }

    public void testListSetFieldValueIndexOutOfBounds() {
        try {
            ingestDocument.setFieldValue("list.10", "value");
            fail("set field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[10] is out of bounds for array with length [2] as part of path [list.10]"));
        }

        try {
            ingestDocument.setFieldValue("list.10.field", "value");
            fail("set field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(
                e.getMessage(),
                equalTo(
                    "cannot create child of [0:'list'] with value [[{field=value}, null]] of type [java.util.ArrayList] as part of path "
                        + "[list.10.field]"
                )
            );
        }
    }

    public void testSetFieldValueEmptyName() {
        try {
            ingestDocument.setFieldValue("", "bar");
            fail("add field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
        }
    }

    public void testRemoveField() {
        ingestDocument.removeField("foo");
        assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(10));
        assertThat(ingestDocument.getSourceAndMetadata().containsKey("foo"), equalTo(false));
        ingestDocument.removeField("_index");
        assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(9));
        assertThat(ingestDocument.getSourceAndMetadata().containsKey("_index"), equalTo(false));
        ingestDocument.removeField("_source.fizz");
        assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(8));
        assertThat(ingestDocument.getSourceAndMetadata().containsKey("fizz"), equalTo(false));
        assertThat(ingestDocument.getIngestMetadata().size(), equalTo(1));
        ingestDocument.removeField("_ingest.timestamp");
        assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(8));
        assertThat(ingestDocument.getIngestMetadata().size(), equalTo(0));
    }

    public void testRemoveInnerField() {
        ingestDocument.removeField("fizz.buzz");
        assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(11));
        assertThat(ingestDocument.getSourceAndMetadata().get("fizz"), instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("fizz");
        assertThat(map.size(), equalTo(3));
        assertThat(map.containsKey("buzz"), equalTo(false));

        ingestDocument.removeField("fizz.foo_null");
        assertThat(map.size(), equalTo(2));
        assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(11));
        assertThat(ingestDocument.getSourceAndMetadata().containsKey("fizz"), equalTo(true));

        ingestDocument.removeField("fizz.1");
        assertThat(map.size(), equalTo(1));
        assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(11));
        assertThat(ingestDocument.getSourceAndMetadata().containsKey("fizz"), equalTo(true));

        ingestDocument.removeField("fizz.list");
        assertThat(map.size(), equalTo(0));
        assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(11));
        assertThat(ingestDocument.getSourceAndMetadata().containsKey("fizz"), equalTo(true));
    }

    public void testRemoveNonExistingField() {
        try {
            ingestDocument.removeField("does_not_exist");
            fail("remove field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [does_not_exist] not present as part of path [does_not_exist]"));
        }
    }

    public void testRemoveExistingParentTypeMismatch() {
        try {
            ingestDocument.removeField("foo.foo.bar");
            fail("remove field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(
                e.getMessage(),
                equalTo("field [foo.foo] not present as part of path [foo.foo.bar]")
            );
        }
    }

    public void testRemoveSourceObject() {
        try {
            ingestDocument.removeField("_source");
            fail("remove field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [_source] not present as part of path [_source]"));
        }
    }

    public void testRemoveIngestObject() {
        ingestDocument.removeField("_ingest");
        assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(10));
        assertThat(ingestDocument.getSourceAndMetadata().containsKey("_ingest"), equalTo(false));
    }

    public void testRemoveEmptyPathAfterStrippingOutPrefix() {
        try {
            ingestDocument.removeField("_source.");
            fail("set field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [_source.] is not valid"));
        }

        try {
            ingestDocument.removeField("_ingest.");
            fail("set field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [_ingest.] is not valid"));
        }
    }

    public void testListRemoveField() {
        ingestDocument.removeField("list.0.field");
        assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(11));
        assertThat(ingestDocument.getSourceAndMetadata().containsKey("list"), equalTo(true));
        Object object = ingestDocument.getSourceAndMetadata().get("list");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(2));
        object = list.get(0);
        assertThat(object, instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) object;
        assertThat(map.size(), equalTo(0));
        ingestDocument.removeField("list.0");
        assertThat(list.size(), equalTo(1));
        assertThat(list.get(0), nullValue());
    }

    public void testRemoveFieldValueNotFoundNullParent() {
        try {
            ingestDocument.removeField("fizz.foo_null.not_there");
            fail("get field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [not_there] not present as part of path [fizz.foo_null.not_there]"));
        }
    }

    public void testNestedRemoveFieldTypeMismatch() {
        try {
            ingestDocument.removeField("fizz.1.bar");
            fail("remove field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [bar] not present as part of path [fizz.1.bar]"));
        }
    }

    public void testListRemoveFieldIndexNotNumeric() {
        try {
            ingestDocument.removeField("list.test");
            fail("remove field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[test] is not an integer, cannot be used as an index as part of path [list.test]"));
        }
    }

    public void testListRemoveFieldIndexOutOfBounds() {
        try {
            ingestDocument.removeField("list.10");
            fail("remove field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[10] is out of bounds for array with length [2] as part of path [list.10]"));
        }
    }

    public void testRemoveNullField() {
        try {
            ingestDocument.removeField((String) null);
            fail("remove field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
        }
    }

    public void testRemoveEmptyField() {
        try {
            ingestDocument.removeField("");
            fail("remove field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
        }
    }

    public void testEqualsAndHashcode() throws Exception {
        Map<String, Object> sourceAndMetadata = RandomDocumentPicks.randomSource(random());
        int numFields = randomIntBetween(1, IngestDocument.Metadata.values().length);
        for (int i = 0; i < numFields; i++) {
            Tuple<String, Object> metadata = TestIngestDocument.randomMetadata();
            sourceAndMetadata.put(metadata.v1(), metadata.v2());
        }
        sourceAndMetadata.putIfAbsent("_version", TestIngestDocument.randomVersion());
        Map<String, Object> ingestMetadata = new HashMap<>();
        numFields = randomIntBetween(1, 5);
        for (int i = 0; i < numFields; i++) {
            ingestMetadata.put(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10));
        }
        // this is testing equality so use the wire constructor
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, ingestMetadata);

        boolean changed = false;
        Map<String, Object> otherSourceAndMetadata;
        if (randomBoolean()) {
            otherSourceAndMetadata = RandomDocumentPicks.randomSource(random());
            otherSourceAndMetadata.putIfAbsent("_version", TestIngestDocument.randomVersion());
            changed = true;
        } else {
            otherSourceAndMetadata = new HashMap<>(sourceAndMetadata);
        }
        if (randomBoolean()) {
            numFields = randomIntBetween(1, IngestDocument.Metadata.values().length);
            for (int i = 0; i < numFields; i++) {
                Tuple<String, Object> metadata;
                do {
                    metadata = TestIngestDocument.randomMetadata();
                } while (metadata.v2().equals(sourceAndMetadata.get(metadata.v1()))); // must actually be a change
                otherSourceAndMetadata.put(metadata.v1(), metadata.v2());
            }
            changed = true;
        }

        Map<String, Object> otherIngestMetadata;
        if (randomBoolean()) {
            otherIngestMetadata = new HashMap<>();
            numFields = randomIntBetween(1, 5);
            for (int i = 0; i < numFields; i++) {
                otherIngestMetadata.put(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10));
            }
            changed = true;
        } else {
            otherIngestMetadata = Map.copyOf(ingestMetadata);
        }

        IngestDocument otherIngestDocument = new IngestDocument(otherSourceAndMetadata, otherIngestMetadata);
        if (changed) {
            assertThat(ingestDocument, not(equalTo(otherIngestDocument)));
            assertThat(otherIngestDocument, not(equalTo(ingestDocument)));
        } else {
            assertThat(ingestDocument, equalTo(otherIngestDocument));
            assertThat(otherIngestDocument, equalTo(ingestDocument));
            assertThat(ingestDocument.hashCode(), equalTo(otherIngestDocument.hashCode()));
            IngestDocument thirdIngestDocument = new IngestDocument(Map.copyOf(sourceAndMetadata), Map.copyOf(ingestMetadata));
            assertThat(thirdIngestDocument, equalTo(ingestDocument));
            assertThat(ingestDocument, equalTo(thirdIngestDocument));
            assertThat(ingestDocument.hashCode(), equalTo(thirdIngestDocument.hashCode()));
        }
    }

    public void testIngestMetadataTimestamp() throws Exception {
        long before = System.currentTimeMillis();
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        long after = System.currentTimeMillis();
        ZonedDateTime timestamp = (ZonedDateTime) ingestDocument.getIngestMetadata().get(IngestDocument.TIMESTAMP);
        long actualMillis = timestamp.toInstant().toEpochMilli();
        assertThat(timestamp, notNullValue());
        assertThat(actualMillis, greaterThanOrEqualTo(before));
        assertThat(actualMillis, lessThanOrEqualTo(after));
    }

    public void testCopyConstructor() {
        {
            // generic test with a random document and copy
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
            IngestDocument copy = new IngestDocument(ingestDocument);

            // these fields should not be the same instance
            assertThat(ingestDocument.getSourceAndMetadata(), not(sameInstance(copy.getSourceAndMetadata())));
            assertThat(ingestDocument.getCtxMap(), not(sameInstance(copy.getCtxMap())));
            assertThat(ingestDocument.getCtxMap().getMetadata(), not(sameInstance(copy.getCtxMap().getMetadata())));

            // but the two objects should be very much equal to each other
            assertIngestDocument(ingestDocument, copy);
        }

        {
            // manually punch in a few values
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
            ingestDocument.setFieldValue("_index", "foo1");
            ingestDocument.setFieldValue("_id", "bar1");
            ingestDocument.setFieldValue("hello", "world1");
            IngestDocument copy = new IngestDocument(ingestDocument);

            // make sure the copy matches
            assertIngestDocument(ingestDocument, copy);

            // change the copy
            copy.setFieldValue("_index", "foo2");
            copy.setFieldValue("_id", "bar2");
            copy.setFieldValue("hello", "world2");

            // the original shouldn't have changed
            assertThat(ingestDocument.getFieldValue("_index", String.class), equalTo("foo1"));
            assertThat(ingestDocument.getFieldValue("_id", String.class), equalTo("bar1"));
            assertThat(ingestDocument.getFieldValue("hello", String.class), equalTo("world1"));
        }
    }

    public void testCopyConstructorWithZonedDateTime() {
        ZoneId timezone = ZoneId.of("Europe/London");

        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("beforeClockChange", ZonedDateTime.ofInstant(Instant.ofEpochSecond(1509237000), timezone));
        sourceAndMetadata.put("afterClockChange", ZonedDateTime.ofInstant(Instant.ofEpochSecond(1509240600), timezone));

        IngestDocument original = TestIngestDocument.withDefaultVersion(sourceAndMetadata);
        IngestDocument copy = new IngestDocument(original);

        assertThat(copy.getSourceAndMetadata().get("beforeClockChange"), equalTo(original.getSourceAndMetadata().get("beforeClockChange")));
        assertThat(copy.getSourceAndMetadata().get("afterClockChange"), equalTo(original.getSourceAndMetadata().get("afterClockChange")));
    }

    public void testSetInvalidSourceField() throws Exception {
        Map<String, Object> document = new HashMap<>();
        Object randomObject = randomFrom(new ArrayList<>(), new HashMap<>(), 12, 12.34);
        document.put("source_field", randomObject);

        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        try {
            ingestDocument.getFieldValueAsBytes("source_field");
            fail("Expected an exception due to invalid source field, but did not happen");
        } catch (IllegalArgumentException e) {
            String expectedClassName = randomObject.getClass().getName();

            assertThat(
                e.getMessage(),
                containsString("field [source_field] of unknown type [" + expectedClassName + "], must be string or byte array")
            );
        }
    }

    public void testDeepCopy() {
        IngestDocument copiedDoc = new IngestDocument(
            IngestDocument.deepCopyMap(ingestDocument.getSourceAndMetadata()),
            IngestDocument.deepCopyMap(ingestDocument.getIngestMetadata())
        );
        assertArrayEquals(
            copiedDoc.getFieldValue(DOUBLE_ARRAY_FIELD, double[].class),
            ingestDocument.getFieldValue(DOUBLE_ARRAY_FIELD, double[].class),
            1e-10
        );
        assertArrayEquals(
            copiedDoc.getFieldValue(DOUBLE_DOUBLE_ARRAY_FIELD, double[][].class),
            ingestDocument.getFieldValue(DOUBLE_DOUBLE_ARRAY_FIELD, double[][].class)
        );
    }

    public void testGetAllFields() {
        Map<String, Object> address = new HashMap<>();
        address.put("street", "Ipiranga Street");
        address.put("number", 123);

        Map<String, Object> source = new HashMap<>();
        source.put("_id", "a123");
        source.put("name", "eric clapton");
        source.put("address", address);

        Set<String> result = IngestDocument.getAllFields(source);

        assertThat(result, containsInAnyOrder("_id", "name", "address", "address.street", "address.number"));
    }

    public void testIsMetadata() {
        assertTrue(IngestDocument.Metadata.isMetadata("_type"));
        assertTrue(IngestDocument.Metadata.isMetadata("_index"));
        assertTrue(IngestDocument.Metadata.isMetadata("_version"));
        assertFalse(IngestDocument.Metadata.isMetadata("name"));
        assertFalse(IngestDocument.Metadata.isMetadata("address"));
    }

    public void testIndexHistory() {
        // the index history contains the original index
        String index1 = ingestDocument.getFieldValue("_index", String.class);
        assertThat(index1, equalTo("index"));
        assertThat(ingestDocument.getIndexHistory(), Matchers.contains(index1));

        // it can be updated to include another index
        String index2 = "another_index";
        assertTrue(ingestDocument.updateIndexHistory(index2));
        assertThat(ingestDocument.getIndexHistory(), Matchers.contains(index1, index2));

        // an index cycle cannot be introduced, however
        assertFalse(ingestDocument.updateIndexHistory(index1));
        assertThat(ingestDocument.getIndexHistory(), Matchers.contains(index1, index2));
    }
}
