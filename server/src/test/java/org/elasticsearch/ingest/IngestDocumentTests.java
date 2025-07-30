/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class IngestDocumentTests extends ESTestCase {

    private static final ZonedDateTime BOGUS_TIMESTAMP = ZonedDateTime.of(2016, 10, 23, 0, 0, 0, 0, ZoneOffset.UTC);
    private IngestDocument document;
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

        this.document = new IngestDocument("index", "id", 1, null, null, document);
    }

    public void testSimpleGetFieldValue() {
        assertThat(document.getFieldValue("foo", String.class), equalTo("bar"));
        assertThat(document.getFieldValue("int", Integer.class), equalTo(123));
        assertThat(document.getFieldValue("_source.foo", String.class), equalTo("bar"));
        assertThat(document.getFieldValue("_source.int", Integer.class), equalTo(123));
        assertThat(document.getFieldValue("_index", String.class), equalTo("index"));
        assertThat(document.getFieldValue("_id", String.class), equalTo("id"));
        assertThat(
            document.getFieldValue("_ingest.timestamp", ZonedDateTime.class),
            both(notNullValue()).and(not(equalTo(BOGUS_TIMESTAMP)))
        );
        assertThat(document.getFieldValue("_source._ingest.timestamp", ZonedDateTime.class), equalTo(BOGUS_TIMESTAMP));
    }

    public void testGetFieldValueIgnoreMissing() {
        assertThat(document.getFieldValue("foo", String.class, randomBoolean()), equalTo("bar"));
        assertThat(document.getFieldValue("int", Integer.class, randomBoolean()), equalTo(123));

        // if ignoreMissing is true, we just return nulls for values that aren't found
        assertThat(document.getFieldValue("nonsense", Integer.class, true), nullValue());
        assertThat(document.getFieldValue("some.nonsense", Integer.class, true), nullValue());
        assertThat(document.getFieldValue("fizz.some.nonsense", Integer.class, true), nullValue());

        // if ignoreMissing is false, we throw an exception for values that aren't found
        IllegalArgumentException e;
        e = expectThrows(IllegalArgumentException.class, () -> document.getFieldValue("fizz.some.nonsense", Integer.class, false));
        assertThat(e.getMessage(), is("field [some] not present as part of path [fizz.some.nonsense]"));

        // if ignoreMissing is true, and the object is present-and-of-the-wrong-type, then we also throw an exception
        e = expectThrows(IllegalArgumentException.class, () -> document.getFieldValue("int", Boolean.class, true));
        assertThat(e.getMessage(), is("field [int] of type [java.lang.Integer] cannot be cast to [java.lang.Boolean]"));
    }

    public void testGetSourceObject() {
        try {
            document.getFieldValue("_source", Object.class);
            fail("get field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [_source] not present as part of path [_source]"));
        }
    }

    public void testGetIngestObject() {
        assertThat(document.getFieldValue("_ingest", Map.class), notNullValue());
    }

    public void testGetEmptyPathAfterStrippingOutPrefix() {
        try {
            document.getFieldValue("_source.", Object.class);
            fail("get field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [_source.] is not valid"));
        }

        try {
            document.getFieldValue("_ingest.", Object.class);
            fail("get field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [_ingest.] is not valid"));
        }
    }

    public void testGetFieldValueNullValue() {
        assertThat(document.getFieldValue("fizz.foo_null", Object.class), nullValue());
    }

    public void testSimpleGetFieldValueTypeMismatch() {
        try {
            document.getFieldValue("int", String.class);
            fail("getFieldValue should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [int] of type [java.lang.Integer] cannot be cast to [java.lang.String]"));
        }

        try {
            document.getFieldValue("foo", Integer.class);
            fail("getFieldValue should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [foo] of type [java.lang.String] cannot be cast to [java.lang.Integer]"));
        }
    }

    public void testSimpleGetFieldValueIgnoreMissingAndTypeMismatch() {
        try {
            document.getFieldValue("int", String.class, randomBoolean());
            fail("getFieldValue should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [int] of type [java.lang.Integer] cannot be cast to [java.lang.String]"));
        }

        try {
            document.getFieldValue("foo", Integer.class, randomBoolean());
            fail("getFieldValue should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [foo] of type [java.lang.String] cannot be cast to [java.lang.Integer]"));
        }
    }

    public void testNestedGetFieldValue() {
        assertThat(document.getFieldValue("fizz.buzz", String.class), equalTo("hello world"));
        assertThat(document.getFieldValue("fizz.1", String.class), equalTo("bar"));
    }

    public void testNestedGetFieldValueTypeMismatch() {
        try {
            document.getFieldValue("foo.foo.bar", String.class);
        } catch (IllegalArgumentException e) {
            assertThat(
                e.getMessage(),
                equalTo("cannot resolve [foo] from object of type [java.lang.String] as part of path [foo.foo.bar]")
            );
        }
    }

    public void testListGetFieldValue() {
        assertThat(document.getFieldValue("list.0.field", String.class), equalTo("value"));
    }

    public void testListGetFieldValueNull() {
        assertThat(document.getFieldValue("list.1", String.class), nullValue());
    }

    public void testListGetFieldValueIndexNotNumeric() {
        try {
            document.getFieldValue("list.test.field", String.class);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[test] is not an integer, cannot be used as an index as part of path [list.test.field]"));
        }
    }

    public void testListGetFieldValueIndexOutOfBounds() {
        try {
            document.getFieldValue("list.10.field", String.class);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[10] is out of bounds for array with length [2] as part of path [list.10.field]"));
        }
    }

    public void testGetFieldValueNotFound() {
        try {
            document.getFieldValue("not.here", String.class);
            fail("get field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [not] not present as part of path [not.here]"));
        }
    }

    public void testGetFieldValueNotFoundNullParent() {
        try {
            document.getFieldValue("fizz.foo_null.not_there", String.class);
            fail("get field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("cannot resolve [not_there] from null as part of path [fizz.foo_null.not_there]"));
        }
    }

    public void testGetFieldValueNull() {
        try {
            document.getFieldValue(null, String.class);
            fail("get field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
        }
    }

    public void testGetFieldValueEmpty() {
        try {
            document.getFieldValue("", String.class);
            fail("get field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
        }
    }

    public void testHasField() {
        assertTrue(document.hasField("fizz"));
        assertTrue(document.hasField("_index"));
        assertTrue(document.hasField("_id"));
        assertTrue(document.hasField("_source.fizz"));
        assertTrue(document.hasField("_ingest.timestamp"));
    }

    public void testHasFieldNested() {
        assertTrue(document.hasField("fizz.buzz"));
        assertTrue(document.hasField("_source._ingest.timestamp"));
    }

    public void testListHasField() {
        assertTrue(document.hasField("list.0.field"));
    }

    public void testListHasFieldNull() {
        assertTrue(document.hasField("list.1"));
    }

    public void testListHasFieldIndexOutOfBounds() {
        assertFalse(document.hasField("list.10"));
    }

    public void testListHasFieldIndexOutOfBounds_fail() {
        assertTrue(document.hasField("list.0", true));
        assertTrue(document.hasField("list.1", true));
        Exception e = expectThrows(IllegalArgumentException.class, () -> document.hasField("list.2", true));
        assertThat(e.getMessage(), equalTo("[2] is out of bounds for array with length [2] as part of path [list.2]"));
        e = expectThrows(IllegalArgumentException.class, () -> document.hasField("list.10", true));
        assertThat(e.getMessage(), equalTo("[10] is out of bounds for array with length [2] as part of path [list.10]"));
    }

    public void testListHasFieldIndexNotNumeric() {
        assertFalse(document.hasField("list.test"));
    }

    public void testNestedHasFieldTypeMismatch() {
        assertFalse(document.hasField("foo.foo.bar"));
    }

    public void testHasFieldNotFound() {
        assertFalse(document.hasField("not.here"));
    }

    public void testHasFieldNotFoundNullParent() {
        assertFalse(document.hasField("fizz.foo_null.not_there"));
    }

    public void testHasFieldNestedNotFound() {
        assertFalse(document.hasField("fizz.doesnotexist"));
    }

    public void testHasFieldNull() {
        try {
            document.hasField(null);
            fail("has field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
        }
    }

    public void testHasFieldNullValue() {
        assertTrue(document.hasField("fizz.foo_null"));
    }

    public void testHasFieldEmpty() {
        try {
            document.hasField("");
            fail("has field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
        }
    }

    public void testHasFieldSourceObject() {
        assertThat(document.hasField("_source"), equalTo(false));
    }

    public void testHasFieldIngestObject() {
        assertThat(document.hasField("_ingest"), equalTo(true));
    }

    public void testHasFieldEmptyPathAfterStrippingOutPrefix() {
        try {
            document.hasField("_source.");
            fail("has field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [_source.] is not valid"));
        }

        try {
            document.hasField("_ingest.");
            fail("has field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [_ingest.] is not valid"));
        }
    }

    public void testSimpleSetFieldValue() {
        document.setFieldValue("new_field", "foo");
        assertThat(document.getSourceAndMetadata().get("new_field"), equalTo("foo"));
        document.setFieldValue("_ttl", "ttl");
        assertThat(document.getSourceAndMetadata().get("_ttl"), equalTo("ttl"));
        document.setFieldValue("_source.another_field", "bar");
        assertThat(document.getSourceAndMetadata().get("another_field"), equalTo("bar"));
        document.setFieldValue("_ingest.new_field", "new_value");
        assertThat(document.getIngestMetadata().size(), equalTo(2));
        assertThat(document.getIngestMetadata().get("new_field"), equalTo("new_value"));
        document.setFieldValue("_ingest.timestamp", "timestamp");
        assertThat(document.getIngestMetadata().get("timestamp"), equalTo("timestamp"));
    }

    public void testSetFieldValueNullValue() {
        document.setFieldValue("new_field", (Object) null);
        assertThat(document.getSourceAndMetadata().containsKey("new_field"), equalTo(true));
        assertThat(document.getSourceAndMetadata().get("new_field"), nullValue());
    }

    @SuppressWarnings("unchecked")
    public void testNestedSetFieldValue() {
        document.setFieldValue("a.b.c.d", "foo");
        assertThat(document.getSourceAndMetadata().get("a"), instanceOf(Map.class));
        Map<String, Object> a = (Map<String, Object>) document.getSourceAndMetadata().get("a");
        assertThat(a.get("b"), instanceOf(Map.class));
        Map<String, Object> b = (Map<String, Object>) a.get("b");
        assertThat(b.get("c"), instanceOf(Map.class));
        Map<String, Object> c = (Map<String, Object>) b.get("c");
        assertThat(c.get("d"), instanceOf(String.class));
        String d = (String) c.get("d");
        assertThat(d, equalTo("foo"));
    }

    public void testSetFieldValueOnExistingField() {
        document.setFieldValue("foo", "newbar");
        assertThat(document.getSourceAndMetadata().get("foo"), equalTo("newbar"));
    }

    @SuppressWarnings("unchecked")
    public void testSetFieldValueOnExistingParent() {
        document.setFieldValue("fizz.new", "bar");
        assertThat(document.getSourceAndMetadata().get("fizz"), instanceOf(Map.class));
        Map<String, Object> innerMap = (Map<String, Object>) document.getSourceAndMetadata().get("fizz");
        assertThat(innerMap.get("new"), instanceOf(String.class));
        String value = (String) innerMap.get("new");
        assertThat(value, equalTo("bar"));
    }

    public void testSetFieldValueOnExistingParentTypeMismatch() {
        try {
            document.setFieldValue("fizz.buzz.new", "bar");
            fail("add field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(
                e.getMessage(),
                equalTo("cannot set [new] with parent object of type [java.lang.String] as part of path [fizz.buzz.new]")
            );
        }
    }

    public void testSetFieldValueOnExistingNullParent() {
        try {
            document.setFieldValue("fizz.foo_null.test", "bar");
            fail("add field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("cannot set [test] with null parent as part of path [fizz.foo_null.test]"));
        }
    }

    public void testSetFieldValueNullName() {
        try {
            document.setFieldValue(null, "bar");
            fail("add field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
        }
    }

    public void testSetSourceObject() {
        document.setFieldValue("_source", "value");
        assertThat(document.getSourceAndMetadata().get("_source"), equalTo("value"));
    }

    public void testSetIngestObject() {
        document.setFieldValue("_ingest", "value");
        assertThat(document.getSourceAndMetadata().get("_ingest"), equalTo("value"));
    }

    public void testSetIngestSourceObject() {
        // test that we don't strip out the _source prefix when _ingest is used
        document.setFieldValue("_ingest._source", "value");
        assertThat(document.getIngestMetadata().get("_source"), equalTo("value"));
    }

    public void testSetEmptyPathAfterStrippingOutPrefix() {
        try {
            document.setFieldValue("_source.", "value");
            fail("set field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [_source.] is not valid"));
        }

        try {
            document.setFieldValue("_ingest.", "_value");
            fail("set field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [_ingest.] is not valid"));
        }
    }

    public void testListSetFieldValueNoIndexProvided() {
        document.setFieldValue("list", "value");
        Object object = document.getSourceAndMetadata().get("list");
        assertThat(object, instanceOf(String.class));
        assertThat(object, equalTo("value"));
    }

    public void testListAppendFieldValue() {
        document.appendFieldValue("list", "new_value");
        Object object = document.getSourceAndMetadata().get("list");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(3));
        assertThat(list.get(0), equalTo(Map.of("field", "value")));
        assertThat(list.get(1), nullValue());
        assertThat(list.get(2), equalTo("new_value"));
    }

    public void testListAppendFieldValueWithDuplicate() {
        document.appendFieldValue("list2", "foo", false);
        Object object = document.getSourceAndMetadata().get("list2");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(3));
        assertThat(list, equalTo(List.of("foo", "bar", "baz")));
    }

    public void testListAppendFieldValueWithoutDuplicate() {
        document.appendFieldValue("list2", "foo2", false);
        Object object = document.getSourceAndMetadata().get("list2");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(4));
        assertThat(list, equalTo(List.of("foo", "bar", "baz", "foo2")));
    }

    public void testListAppendFieldValues() {
        document.appendFieldValue("list", List.of("item1", "item2", "item3"));
        Object object = document.getSourceAndMetadata().get("list");
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
        document.appendFieldValue("list2", List.of("foo", "bar", "baz", "foo2"), false);
        Object object = document.getSourceAndMetadata().get("list2");
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
        document.appendFieldValue("non_existing_list", "new_value");
        Object object = document.getSourceAndMetadata().get("non_existing_list");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(1));
        assertThat(list.get(0), equalTo("new_value"));
    }

    public void testAppendFieldValuesToNonExistingList() {
        document.appendFieldValue("non_existing_list", List.of("item1", "item2", "item3"));
        Object object = document.getSourceAndMetadata().get("non_existing_list");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(3));
        assertThat(list.get(0), equalTo("item1"));
        assertThat(list.get(1), equalTo("item2"));
        assertThat(list.get(2), equalTo("item3"));
    }

    public void testAppendFieldValueConvertStringToList() {
        document.appendFieldValue("fizz.buzz", "new_value");
        Object object = document.getSourceAndMetadata().get("fizz");
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
        document.appendFieldValue("fizz.buzz", List.of("item1", "item2", "item3"));
        Object object = document.getSourceAndMetadata().get("fizz");
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
        document.appendFieldValue("int", 456);
        Object object = document.getSourceAndMetadata().get("int");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(2));
        assertThat(list.get(0), equalTo(123));
        assertThat(list.get(1), equalTo(456));
    }

    public void testAppendFieldValuesConvertIntegerToList() {
        document.appendFieldValue("int", List.of(456, 789));
        Object object = document.getSourceAndMetadata().get("int");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(3));
        assertThat(list.get(0), equalTo(123));
        assertThat(list.get(1), equalTo(456));
        assertThat(list.get(2), equalTo(789));
    }

    public void testAppendFieldValueConvertMapToList() {
        document.appendFieldValue("fizz", Map.of("field", "value"));
        Object object = document.getSourceAndMetadata().get("fizz");
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
        document.appendFieldValue("fizz.foo_null", "new_value");
        Object object = document.getSourceAndMetadata().get("fizz");
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
        document.appendFieldValue("fizz.list.0", "item2");
        Object object = document.getSourceAndMetadata().get("fizz");
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
        document.appendFieldValue("fizz.list.0", List.of("item2", "item3", "item4"));
        Object object = document.getSourceAndMetadata().get("fizz");
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
        document.appendFieldValue("fizz.list.0.0", "new_value");
        Object object = document.getSourceAndMetadata().get("fizz");
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
        document.appendFieldValue("fizz.list.0.0", List.of("item2", "item3", "item4"));
        Object object = document.getSourceAndMetadata().get("fizz");
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
        document.appendFieldValue("list.0", Map.of("item2", "value2"));
        Object object = document.getSourceAndMetadata().get("list");
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
        document.appendFieldValue("list.1", "new_value");
        Object object = document.getSourceAndMetadata().get("list");
        assertThat(object, instanceOf(List.class));
        List<?> list = (List<?>) object;
        assertThat(list.get(1), instanceOf(List.class));
        list = (List<?>) list.get(1);
        assertThat(list.size(), equalTo(2));
        assertThat(list.get(0), nullValue());
        assertThat(list.get(1), equalTo("new_value"));
    }

    public void testAppendFieldValueToListOfMaps() {
        document.appendFieldValue("list", Map.of("item2", "value2"));
        Object object = document.getSourceAndMetadata().get("list");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(3));
        assertThat(list.get(0), equalTo(Map.of("field", "value")));
        assertThat(list.get(1), nullValue());
        assertThat(list.get(2), equalTo(Map.of("item2", "value2")));
    }

    public void testListSetFieldValueIndexProvided() {
        document.setFieldValue("list.1", "value");
        Object object = document.getSourceAndMetadata().get("list");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(2));
        assertThat(list.get(0), equalTo(Map.of("field", "value")));
        assertThat(list.get(1), equalTo("value"));
    }

    public void testSetFieldValueListAsPartOfPath() {
        document.setFieldValue("list.0.field", "new_value");
        Object object = document.getSourceAndMetadata().get("list");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(2));
        assertThat(list.get(0), equalTo(Map.of("field", "new_value")));
        assertThat(list.get(1), nullValue());
    }

    public void testListSetFieldValueIndexNotNumeric() {
        try {
            document.setFieldValue("list.test", "value");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[test] is not an integer, cannot be used as an index as part of path [list.test]"));
        }

        try {
            document.setFieldValue("list.test.field", "new_value");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[test] is not an integer, cannot be used as an index as part of path [list.test.field]"));
        }
    }

    public void testListSetFieldValueIndexOutOfBounds() {
        try {
            document.setFieldValue("list.10", "value");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[10] is out of bounds for array with length [2] as part of path [list.10]"));
        }

        try {
            document.setFieldValue("list.10.field", "value");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[10] is out of bounds for array with length [2] as part of path [list.10.field]"));
        }
    }

    public void testSetFieldValueEmptyName() {
        try {
            document.setFieldValue("", "bar");
            fail("add field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
        }
    }

    public void testRemoveField() {
        document.removeField("foo");
        assertThat(document.getSourceAndMetadata().size(), equalTo(10));
        assertThat(document.getSourceAndMetadata().containsKey("foo"), equalTo(false));
        document.removeField("_index");
        assertThat(document.getSourceAndMetadata().size(), equalTo(9));
        assertThat(document.getSourceAndMetadata().containsKey("_index"), equalTo(false));
        document.removeField("_source.fizz");
        assertThat(document.getSourceAndMetadata().size(), equalTo(8));
        assertThat(document.getSourceAndMetadata().containsKey("fizz"), equalTo(false));
        assertThat(document.getIngestMetadata().size(), equalTo(1));
        document.removeField("_ingest.timestamp");
        assertThat(document.getSourceAndMetadata().size(), equalTo(8));
        assertThat(document.getIngestMetadata().size(), equalTo(0));
    }

    public void testRemoveFieldIgnoreMissing() {
        document.removeField("foo", randomBoolean());
        assertThat(document.getSourceAndMetadata().size(), equalTo(10));
        assertThat(document.getSourceAndMetadata().containsKey("foo"), equalTo(false));
        document.removeField("_index", randomBoolean());
        assertThat(document.getSourceAndMetadata().size(), equalTo(9));
        assertThat(document.getSourceAndMetadata().containsKey("_index"), equalTo(false));

        // if ignoreMissing is false, we throw an exception for values that aren't found
        IllegalArgumentException e;
        switch (randomIntBetween(0, 2)) {
            case 0 -> {
                document.setFieldValue("fizz.some", (Object) null);
                e = expectThrows(IllegalArgumentException.class, () -> document.removeField("fizz.some.nonsense", false));
                assertThat(e.getMessage(), is("cannot remove [nonsense] from null as part of path [fizz.some.nonsense]"));
            }
            case 1 -> {
                document.setFieldValue("fizz.some", List.of("foo", "bar"));
                e = expectThrows(IllegalArgumentException.class, () -> document.removeField("fizz.some.nonsense", false));
                assertThat(
                    e.getMessage(),
                    is("[nonsense] is not an integer, cannot be used as an index as part of path [fizz.some.nonsense]")
                );
            }
            case 2 -> {
                e = expectThrows(IllegalArgumentException.class, () -> document.removeField("fizz.some.nonsense", false));
                assertThat(e.getMessage(), is("field [some] not present as part of path [fizz.some.nonsense]"));
            }
            default -> throw new AssertionError("failure, got illegal switch case");
        }

        // but no exception is thrown if ignoreMissing is true
        document.removeField("fizz.some.nonsense", true);
    }

    public void testRemoveInnerField() {
        document.removeField("fizz.buzz");
        assertThat(document.getSourceAndMetadata().size(), equalTo(11));
        assertThat(document.getSourceAndMetadata().get("fizz"), instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) document.getSourceAndMetadata().get("fizz");
        assertThat(map.size(), equalTo(3));
        assertThat(map.containsKey("buzz"), equalTo(false));

        document.removeField("fizz.foo_null");
        assertThat(map.size(), equalTo(2));
        assertThat(document.getSourceAndMetadata().size(), equalTo(11));
        assertThat(document.getSourceAndMetadata().containsKey("fizz"), equalTo(true));

        document.removeField("fizz.1");
        assertThat(map.size(), equalTo(1));
        assertThat(document.getSourceAndMetadata().size(), equalTo(11));
        assertThat(document.getSourceAndMetadata().containsKey("fizz"), equalTo(true));

        document.removeField("fizz.list");
        assertThat(map.size(), equalTo(0));
        assertThat(document.getSourceAndMetadata().size(), equalTo(11));
        assertThat(document.getSourceAndMetadata().containsKey("fizz"), equalTo(true));
    }

    public void testRemoveNonExistingField() {
        try {
            document.removeField("does_not_exist");
            fail("remove field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [does_not_exist] not present as part of path [does_not_exist]"));
        }
    }

    public void testRemoveExistingParentTypeMismatch() {
        try {
            document.removeField("foo.foo.bar");
            fail("remove field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(
                e.getMessage(),
                equalTo("cannot resolve [foo] from object of type [java.lang.String] as part of path [foo.foo.bar]")
            );
        }
    }

    public void testRemoveSourceObject() {
        try {
            document.removeField("_source");
            fail("remove field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [_source] not present as part of path [_source]"));
        }
    }

    public void testRemoveIngestObject() {
        document.removeField("_ingest");
        assertThat(document.getSourceAndMetadata().size(), equalTo(10));
        assertThat(document.getSourceAndMetadata().containsKey("_ingest"), equalTo(false));
    }

    public void testRemoveEmptyPathAfterStrippingOutPrefix() {
        try {
            document.removeField("_source.");
            fail("set field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [_source.] is not valid"));
        }

        try {
            document.removeField("_ingest.");
            fail("set field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path [_ingest.] is not valid"));
        }
    }

    public void testListRemoveField() {
        document.removeField("list.0.field");
        assertThat(document.getSourceAndMetadata().size(), equalTo(11));
        assertThat(document.getSourceAndMetadata().containsKey("list"), equalTo(true));
        Object object = document.getSourceAndMetadata().get("list");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(2));
        object = list.get(0);
        assertThat(object, instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) object;
        assertThat(map.size(), equalTo(0));
        document.removeField("list.0");
        assertThat(list.size(), equalTo(1));
        assertThat(list.get(0), nullValue());
    }

    public void testRemoveFieldValueNotFoundNullParent() {
        try {
            document.removeField("fizz.foo_null.not_there");
            fail("get field value should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("cannot remove [not_there] from null as part of path [fizz.foo_null.not_there]"));
        }
    }

    public void testNestedRemoveFieldTypeMismatch() {
        try {
            document.removeField("fizz.1.bar");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("cannot remove [bar] from object of type [java.lang.String] as part of path [fizz.1.bar]"));
        }
    }

    public void testListRemoveFieldIndexNotNumeric() {
        try {
            document.removeField("list.test");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[test] is not an integer, cannot be used as an index as part of path [list.test]"));
        }
    }

    public void testListRemoveFieldIndexOutOfBounds() {
        try {
            document.removeField("list.10");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[10] is out of bounds for array with length [2] as part of path [list.10]"));
        }
    }

    public void testRemoveNullField() {
        try {
            document.removeField(null);
            fail("remove field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
        }
    }

    public void testRemoveEmptyField() {
        try {
            document.removeField("");
            fail("remove field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
        }
    }

    public void testIngestMetadataTimestamp() {
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

        {
            // the copy constructor rejects self-references
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
            List<Object> someList = new ArrayList<>();
            someList.add("some string");
            someList.add(someList); // the list contains itself
            ingestDocument.setFieldValue("someList", someList);
            Exception e = expectThrows(IllegalArgumentException.class, () -> new IngestDocument(ingestDocument));
            assertThat(e.getMessage(), equalTo("Iterable object is self-referencing itself"));
        }
    }

    public void testCopyConstructorWithExecutedPipelines() {
        /*
         * This is similar to the first part of testCopyConstructor, except that we're executing a pipeilne, and running the
         * assertions inside the processor so that we can test that executedPipelines is correct.
         */
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        TestProcessor processor = new TestProcessor(ingestDocument1 -> {
            assertThat(ingestDocument1.getPipelineStack().size(), equalTo(1));
            IngestDocument copy = new IngestDocument(ingestDocument1);
            assertThat(ingestDocument1.getSourceAndMetadata(), not(sameInstance(copy.getSourceAndMetadata())));
            assertThat(ingestDocument1.getCtxMap(), not(sameInstance(copy.getCtxMap())));
            assertThat(ingestDocument1.getCtxMap().getMetadata(), not(sameInstance(copy.getCtxMap().getMetadata())));
            assertIngestDocument(ingestDocument1, copy);
            assertThat(copy.getPipelineStack(), equalTo(ingestDocument1.getPipelineStack()));
        });
        Pipeline pipeline = new Pipeline("pipeline1", "test pipeline", 1, Map.of(), new CompoundProcessor(processor));
        ingestDocument.executePipeline(pipeline, (ingestDocument1, exception) -> {
            assertNotNull(ingestDocument1);
            assertNull(exception);
        });
        assertThat(processor.getInvokedCounter(), equalTo(1));
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

    public void testSetInvalidSourceField() {
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
            IngestDocument.deepCopyMap(document.getSourceAndMetadata()),
            IngestDocument.deepCopyMap(document.getIngestMetadata())
        );
        assertArrayEquals(
            copiedDoc.getFieldValue(DOUBLE_ARRAY_FIELD, double[].class),
            document.getFieldValue(DOUBLE_ARRAY_FIELD, double[].class),
            1e-10
        );
        assertArrayEquals(
            copiedDoc.getFieldValue(DOUBLE_DOUBLE_ARRAY_FIELD, double[][].class),
            document.getFieldValue(DOUBLE_DOUBLE_ARRAY_FIELD, double[][].class)
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
        String index1 = document.getFieldValue("_index", String.class);
        assertThat(index1, equalTo("index"));
        assertThat(document.getIndexHistory(), Matchers.contains(index1));

        // it can be updated to include another index
        String index2 = "another_index";
        assertTrue(document.updateIndexHistory(index2));
        assertThat(document.getIndexHistory(), Matchers.contains(index1, index2));

        // an index cycle cannot be introduced, however
        assertFalse(document.updateIndexHistory(index1));
        assertThat(document.getIndexHistory(), Matchers.contains(index1, index2));
    }

    public void testSourceHashMapIsNotCopied() {
        // an ingest document's ctxMap will, as an optimization, just use the passed-in map reference
        {
            Map<String, Object> source = new HashMap<>(Map.of("foo", 1));
            IngestDocument document = new IngestDocument("index", "id", 1, null, null, source);
            assertThat(document.getSource(), sameInstance(source));
            assertThat(document.getCtxMap().getSource(), sameInstance(source));
        }

        {
            Map<String, Object> source = XContentHelper.convertToMap(new BytesArray("{ \"foo\": 1 }"), false, XContentType.JSON).v2();
            IngestDocument document = new IngestDocument("index", "id", 1, null, null, source);
            assertThat(document.getSource(), sameInstance(source));
            assertThat(document.getCtxMap().getSource(), sameInstance(source));
        }

        {
            Map<String, Object> source = Map.of("foo", 1);
            IngestDocument document = new IngestDocument("index", "id", 1, null, null, source);
            assertThat(document.getSource(), sameInstance(source));
            assertThat(document.getCtxMap().getSource(), sameInstance(source));
        }

        // a cloned ingest document will copy the map, though
        {
            Map<String, Object> source = Map.of("foo", 1);
            IngestDocument document1 = new IngestDocument("index", "id", 1, null, null, source);
            document1.getIngestMetadata().put("bar", 2);
            IngestDocument document2 = new IngestDocument(document1);
            assertThat(document2.getCtxMap().getMetadata(), equalTo(document1.getCtxMap().getMetadata()));
            assertThat(document2.getSource(), not(sameInstance(source)));
            assertThat(document2.getCtxMap().getMetadata(), equalTo(document1.getCtxMap().getMetadata()));
            assertThat(document2.getCtxMap().getSource(), not(sameInstance(source)));

            // it also copies these other nearby maps
            assertThat(document2.getIngestMetadata(), equalTo(document1.getIngestMetadata()));
            assertThat(document2.getIngestMetadata(), not(sameInstance(document1.getIngestMetadata())));

            assertThat(document2.getCtxMap().getMetadata(), not(sameInstance(document1.getCtxMap().getMetadata())));
            assertThat(document2.getCtxMap().getMetadata(), not(sameInstance(document1.getCtxMap().getMetadata())));
        }
    }
}
