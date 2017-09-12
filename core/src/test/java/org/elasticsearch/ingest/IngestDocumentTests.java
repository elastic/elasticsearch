/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.Matchers.both;
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
        ingestDocument = new IngestDocument("index", "type", "id", null, null, document);
    }

    public void testSimpleGetFieldValue() {
        assertThat(ingestDocument.getFieldValue("foo", String.class), equalTo("bar"));
        assertThat(ingestDocument.getFieldValue("int", Integer.class), equalTo(123));
        assertThat(ingestDocument.getFieldValue("_source.foo", String.class), equalTo("bar"));
        assertThat(ingestDocument.getFieldValue("_source.int", Integer.class), equalTo(123));
        assertThat(ingestDocument.getFieldValue("_index", String.class), equalTo("index"));
        assertThat(ingestDocument.getFieldValue("_type", String.class), equalTo("type"));
        assertThat(ingestDocument.getFieldValue("_id", String.class), equalTo("id"));
        assertThat(ingestDocument.getFieldValue("_ingest.timestamp", ZonedDateTime.class),
            both(notNullValue()).and(not(equalTo(BOGUS_TIMESTAMP))));
        assertThat(ingestDocument.getFieldValue("_source._ingest.timestamp", ZonedDateTime.class),
            equalTo(BOGUS_TIMESTAMP));
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
            assertThat(e.getMessage(),
                    equalTo("cannot resolve [foo] from object of type [java.lang.String] as part of path [foo.foo.bar]"));
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
            assertThat(e.getMessage(), equalTo("cannot resolve [not_there] from null as part of path [fizz.foo_null.not_there]"));
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
        assertTrue(ingestDocument.hasField("_type"));
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
            assertThat(e.getMessage(),
                    equalTo("cannot set [new] with parent object of type [java.lang.String] as part of path [fizz.buzz.new]"));
        }
    }

    public void testSetFieldValueOnExistingNullParent() {
        try {
            ingestDocument.setFieldValue("fizz.foo_null.test", "bar");
            fail("add field should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("cannot set [test] with null parent as part of path [fizz.foo_null.test]"));
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
        //test that we don't strip out the _source prefix when _ingest is used
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
        assertThat(list.get(0), equalTo(Collections.singletonMap("field", "value")));
        assertThat(list.get(1), nullValue());
        assertThat(list.get(2), equalTo("new_value"));
    }

    public void testListAppendFieldValues() {
        ingestDocument.appendFieldValue("list", Arrays.asList("item1", "item2", "item3"));
        Object object = ingestDocument.getSourceAndMetadata().get("list");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(5));
        assertThat(list.get(0), equalTo(Collections.singletonMap("field", "value")));
        assertThat(list.get(1), nullValue());
        assertThat(list.get(2), equalTo("item1"));
        assertThat(list.get(3), equalTo("item2"));
        assertThat(list.get(4), equalTo("item3"));
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
        ingestDocument.appendFieldValue("non_existing_list", Arrays.asList("item1", "item2", "item3"));
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
        ingestDocument.appendFieldValue("fizz.buzz", Arrays.asList("item1", "item2", "item3"));
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
        ingestDocument.appendFieldValue("int", Arrays.asList(456, 789));
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
        ingestDocument.appendFieldValue("fizz", Collections.singletonMap("field", "value"));
        Object object = ingestDocument.getSourceAndMetadata().get("fizz");
        assertThat(object, instanceOf(List.class));
        List<?> list = (List<?>) object;
        assertThat(list.size(), equalTo(2));
        assertThat(list.get(0), instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) list.get(0);
        assertThat(map.size(), equalTo(4));
        assertThat(list.get(1), equalTo(Collections.singletonMap("field", "value")));
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
        ingestDocument.appendFieldValue("fizz.list.0", Arrays.asList("item2", "item3", "item4"));
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
        ingestDocument.appendFieldValue("fizz.list.0.0", Arrays.asList("item2", "item3", "item4"));
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
        ingestDocument.appendFieldValue("list.0", Collections.singletonMap("item2", "value2"));
        Object object = ingestDocument.getSourceAndMetadata().get("list");
        assertThat(object, instanceOf(List.class));
        List<?> list = (List<?>) object;
        assertThat(list.size(), equalTo(2));
        assertThat(list.get(0), instanceOf(List.class));
        assertThat(list.get(1), nullValue());
        list = (List<?>) list.get(0);
        assertThat(list.size(), equalTo(2));
        assertThat(list.get(0), equalTo(Collections.singletonMap("field", "value")));
        assertThat(list.get(1), equalTo(Collections.singletonMap("item2", "value2")));
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
        ingestDocument.appendFieldValue("list", Collections.singletonMap("item2", "value2"));
        Object object = ingestDocument.getSourceAndMetadata().get("list");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(3));
        assertThat(list.get(0), equalTo(Collections.singletonMap("field", "value")));
        assertThat(list.get(1), nullValue());
        assertThat(list.get(2), equalTo(Collections.singletonMap("item2", "value2")));
    }

    public void testListSetFieldValueIndexProvided() {
        ingestDocument.setFieldValue("list.1", "value");
        Object object = ingestDocument.getSourceAndMetadata().get("list");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(2));
        assertThat(list.get(0), equalTo(Collections.singletonMap("field", "value")));
        assertThat(list.get(1), equalTo("value"));
    }

    public void testSetFieldValueListAsPartOfPath() {
        ingestDocument.setFieldValue("list.0.field", "new_value");
        Object object = ingestDocument.getSourceAndMetadata().get("list");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(2));
        assertThat(list.get(0), equalTo(Collections.singletonMap("field", "new_value")));
        assertThat(list.get(1), nullValue());
    }

    public void testListSetFieldValueIndexNotNumeric() {
        try {
            ingestDocument.setFieldValue("list.test", "value");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[test] is not an integer, cannot be used as an index as part of path [list.test]"));
        }

        try {
            ingestDocument.setFieldValue("list.test.field", "new_value");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[test] is not an integer, cannot be used as an index as part of path [list.test.field]"));
        }
    }

    public void testListSetFieldValueIndexOutOfBounds() {
        try {
            ingestDocument.setFieldValue("list.10", "value");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[10] is out of bounds for array with length [2] as part of path [list.10]"));
        }

        try {
            ingestDocument.setFieldValue("list.10.field", "value");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[10] is out of bounds for array with length [2] as part of path [list.10.field]"));
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
        assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(7));
        assertThat(ingestDocument.getSourceAndMetadata().containsKey("foo"), equalTo(false));
        ingestDocument.removeField("_index");
        assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(6));
        assertThat(ingestDocument.getSourceAndMetadata().containsKey("_index"), equalTo(false));
        ingestDocument.removeField("_source.fizz");
        assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(5));
        assertThat(ingestDocument.getSourceAndMetadata().containsKey("fizz"), equalTo(false));
        assertThat(ingestDocument.getIngestMetadata().size(), equalTo(1));
        ingestDocument.removeField("_ingest.timestamp");
        assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(5));
        assertThat(ingestDocument.getIngestMetadata().size(), equalTo(0));
    }

    public void testRemoveInnerField() {
        ingestDocument.removeField("fizz.buzz");
        assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(8));
        assertThat(ingestDocument.getSourceAndMetadata().get("fizz"), instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("fizz");
        assertThat(map.size(), equalTo(3));
        assertThat(map.containsKey("buzz"), equalTo(false));

        ingestDocument.removeField("fizz.foo_null");
        assertThat(map.size(), equalTo(2));
        assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(8));
        assertThat(ingestDocument.getSourceAndMetadata().containsKey("fizz"), equalTo(true));

        ingestDocument.removeField("fizz.1");
        assertThat(map.size(), equalTo(1));
        assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(8));
        assertThat(ingestDocument.getSourceAndMetadata().containsKey("fizz"), equalTo(true));

        ingestDocument.removeField("fizz.list");
        assertThat(map.size(), equalTo(0));
        assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(8));
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
            assertThat(e.getMessage(),
                    equalTo("cannot resolve [foo] from object of type [java.lang.String] as part of path [foo.foo.bar]"));
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
        assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(7));
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
        assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(8));
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
            assertThat(e.getMessage(), equalTo("cannot remove [not_there] from null as part of path [fizz.foo_null.not_there]"));
        }
    }

    public void testNestedRemoveFieldTypeMismatch() {
        try {
            ingestDocument.removeField("fizz.1.bar");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("cannot remove [bar] from object of type [java.lang.String] as part of path [fizz.1.bar]"));
        }
    }

    public void testListRemoveFieldIndexNotNumeric() {
        try {
            ingestDocument.removeField("list.test");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[test] is not an integer, cannot be used as an index as part of path [list.test]"));
        }
    }

    public void testListRemoveFieldIndexOutOfBounds() {
        try {
            ingestDocument.removeField("list.10");
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
        int numFields = randomIntBetween(1, IngestDocument.MetaData.values().length);
        for (int i = 0; i < numFields; i++) {
            sourceAndMetadata.put(randomFrom(IngestDocument.MetaData.values()).getFieldName(), randomAlphaOfLengthBetween(5, 10));
        }
        Map<String, Object> ingestMetadata = new HashMap<>();
        numFields = randomIntBetween(1, 5);
        for (int i = 0; i < numFields; i++) {
            ingestMetadata.put(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10));
        }
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, ingestMetadata);

        boolean changed = false;
        Map<String, Object> otherSourceAndMetadata;
        if (randomBoolean()) {
            otherSourceAndMetadata = RandomDocumentPicks.randomSource(random());
            changed = true;
        } else {
            otherSourceAndMetadata = new HashMap<>(sourceAndMetadata);
        }
        if (randomBoolean()) {
            numFields = randomIntBetween(1, IngestDocument.MetaData.values().length);
            for (int i = 0; i < numFields; i++) {
                otherSourceAndMetadata.put(randomFrom(IngestDocument.MetaData.values()).getFieldName(), randomAlphaOfLengthBetween(5, 10));
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
            otherIngestMetadata = Collections.unmodifiableMap(ingestMetadata);
        }

        IngestDocument otherIngestDocument = new IngestDocument(otherSourceAndMetadata, otherIngestMetadata);
        if (changed) {
            assertThat(ingestDocument, not(equalTo(otherIngestDocument)));
            assertThat(otherIngestDocument, not(equalTo(ingestDocument)));
        } else {
            assertThat(ingestDocument, equalTo(otherIngestDocument));
            assertThat(otherIngestDocument, equalTo(ingestDocument));
            assertThat(ingestDocument.hashCode(), equalTo(otherIngestDocument.hashCode()));
            IngestDocument thirdIngestDocument = new IngestDocument(Collections.unmodifiableMap(sourceAndMetadata),
                    Collections.unmodifiableMap(ingestMetadata));
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
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        IngestDocument copy = new IngestDocument(ingestDocument);
        assertThat(ingestDocument.getSourceAndMetadata(), not(sameInstance(copy.getSourceAndMetadata())));
        assertIngestDocument(ingestDocument, copy);
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

            assertThat(e.getMessage(),
                    containsString("field [source_field] of unknown type [" + expectedClassName + "], must be string or byte array"));
        }
    }

}
