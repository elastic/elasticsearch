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

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.*;

import static org.hamcrest.Matchers.*;

public class IngestDocumentTests extends ESTestCase {

    private IngestDocument ingestDocument;

    @Before
    public void setIngestDocument() {
        Map<String, Object> document = new HashMap<>();
        document.put("foo", "bar");
        document.put("int", 123);
        Map<String, Object> innerObject = new HashMap<>();
        innerObject.put("buzz", "hello world");
        innerObject.put("foo_null", null);
        innerObject.put("1", "bar");
        document.put("fizz", innerObject);
        List<Map<String, Object>> list = new ArrayList<>();
        Map<String, Object> value = new HashMap<>();
        value.put("field", "value");
        list.add(value);
        list.add(null);
        document.put("list", list);
        ingestDocument = new IngestDocument("index", "type", "id", document);
        assertThat(ingestDocument.isSourceModified(), equalTo(false));
    }

    public void testSimpleGetFieldValue() {
        assertThat(ingestDocument.getFieldValue("foo", String.class), equalTo("bar"));
        assertThat(ingestDocument.getFieldValue("int", Integer.class), equalTo(123));
    }

    public void testGetFieldValueNullValue() {
        assertThat(ingestDocument.getFieldValue("fizz.foo_null", Object.class), nullValue());
    }

    public void testSimpleGetFieldValueTypeMismatch() {
        try {
            ingestDocument.getFieldValue("int", String.class);
            fail("getFieldValue should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [int] of type [java.lang.Integer] cannot be cast to [java.lang.String]"));
        }

        try {
            ingestDocument.getFieldValue("foo", Integer.class);
            fail("getFieldValue should have failed");
        } catch(IllegalArgumentException e) {
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
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("cannot resolve [foo] from object of type [java.lang.String] as part of path [foo.foo.bar]"));
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
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[test] is not an integer, cannot be used as an index as part of path [list.test.field]"));
        }
    }

    public void testListGetFieldValueIndexOutOfBounds() {
        try {
            ingestDocument.getFieldValue("list.10.field", String.class);
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[10] is out of bounds for array with length [2] as part of path [list.10.field]"));
        }
    }

    public void testGetFieldValueNotFound() {
        try {
            ingestDocument.getFieldValue("not.here", String.class);
            fail("get field value should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [not] not present as part of path [not.here]"));
        }
    }

    public void testGetFieldValueNotFoundNullParent() {
        try {
            ingestDocument.getFieldValue("fizz.foo_null.not_there", String.class);
            fail("get field value should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("cannot resolve [not_there] from null as part of path [fizz.foo_null.not_there]"));
        }
    }

    public void testGetFieldValueNull() {
        try {
            ingestDocument.getFieldValue(null, String.class);
            fail("get field value should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
        }
    }

    public void testGetFieldValueEmpty() {
        try {
            ingestDocument.getFieldValue("", String.class);
            fail("get field value should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
        }
    }

    public void testHasField() {
        assertTrue(ingestDocument.hasField("fizz"));
    }

    public void testHasFieldNested() {
        assertTrue(ingestDocument.hasField("fizz.buzz"));
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
        assertFalse(ingestDocument.hasField(null));
    }

    public void testHasFieldNullValue() {
        assertTrue(ingestDocument.hasField("fizz.foo_null"));
    }

    public void testHasFieldEmpty() {
        assertFalse(ingestDocument.hasField(""));
    }

    public void testSimpleSetFieldValue() {
        ingestDocument.setFieldValue("new_field", "foo");
        assertThat(ingestDocument.getSource().get("new_field"), equalTo("foo"));
        assertThat(ingestDocument.isSourceModified(), equalTo(true));
    }

    public void testSetFieldValueNullValue() {
        ingestDocument.setFieldValue("new_field", null);
        assertThat(ingestDocument.getSource().containsKey("new_field"), equalTo(true));
        assertThat(ingestDocument.getSource().get("new_field"), nullValue());
        assertThat(ingestDocument.isSourceModified(), equalTo(true));
    }

    @SuppressWarnings("unchecked")
    public void testNestedSetFieldValue() {
        ingestDocument.setFieldValue("a.b.c.d", "foo");
        assertThat(ingestDocument.getSource().get("a"), instanceOf(Map.class));
        Map<String, Object> a = (Map<String, Object>) ingestDocument.getSource().get("a");
        assertThat(a.get("b"), instanceOf(Map.class));
        Map<String, Object> b = (Map<String, Object>) a.get("b");
        assertThat(b.get("c"), instanceOf(Map.class));
        Map<String, Object> c = (Map<String, Object>) b.get("c");
        assertThat(c.get("d"), instanceOf(String.class));
        String d = (String) c.get("d");
        assertThat(d, equalTo("foo"));
        assertThat(ingestDocument.isSourceModified(), equalTo(true));
    }

    public void testSetFieldValueOnExistingField() {
        ingestDocument.setFieldValue("foo", "newbar");
        assertThat(ingestDocument.getSource().get("foo"), equalTo("newbar"));
    }

    @SuppressWarnings("unchecked")
    public void testSetFieldValueOnExistingParent() {
        ingestDocument.setFieldValue("fizz.new", "bar");
        assertThat(ingestDocument.getSource().get("fizz"), instanceOf(Map.class));
        Map<String, Object> innerMap = (Map<String, Object>) ingestDocument.getSource().get("fizz");
        assertThat(innerMap.get("new"), instanceOf(String.class));
        String value = (String) innerMap.get("new");
        assertThat(value, equalTo("bar"));
        assertThat(ingestDocument.isSourceModified(), equalTo(true));
    }

    public void testSetFieldValueOnExistingParentTypeMismatch() {
        try {
            ingestDocument.setFieldValue("fizz.buzz.new", "bar");
            fail("add field should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("cannot set [new] with parent object of type [java.lang.String] as part of path [fizz.buzz.new]"));
            assertThat(ingestDocument.isSourceModified(), equalTo(false));
        }
    }

    public void testSetFieldValueOnExistingNullParent() {
        try {
            ingestDocument.setFieldValue("fizz.foo_null.test", "bar");
            fail("add field should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("cannot set [test] with null parent as part of path [fizz.foo_null.test]"));
            assertThat(ingestDocument.isSourceModified(), equalTo(false));
        }
    }

    public void testSetFieldValueNullName() {
        try {
            ingestDocument.setFieldValue(null, "bar");
            fail("add field should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
            assertThat(ingestDocument.isSourceModified(), equalTo(false));
        }
    }

    public void testListSetFieldValueNoIndexProvided() {
        ingestDocument.setFieldValue("list", "value");
        assertThat(ingestDocument.isSourceModified(), equalTo(true));
        Object object = ingestDocument.getSource().get("list");
        assertThat(object, instanceOf(String.class));
        assertThat(object, equalTo("value"));
    }

    public void testListAppendFieldValue() {
        ingestDocument.appendFieldValue("list", "new_value");
        assertThat(ingestDocument.isSourceModified(), equalTo(true));
        Object object = ingestDocument.getSource().get("list");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(3));
        assertThat(list.get(0), equalTo(Collections.singletonMap("field", "value")));
        assertThat(list.get(1), nullValue());
        assertThat(list.get(2), equalTo("new_value"));
    }

    public void testListSetFieldValueIndexProvided() {
        ingestDocument.setFieldValue("list.1", "value");
        assertThat(ingestDocument.isSourceModified(), equalTo(true));
        Object object = ingestDocument.getSource().get("list");
        assertThat(object, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) object;
        assertThat(list.size(), equalTo(3));
        assertThat(list.get(0), equalTo(Collections.singletonMap("field", "value")));
        assertThat(list.get(1), equalTo("value"));
        assertThat(list.get(2), nullValue());
    }

    public void testSetFieldValueListAsPartOfPath() {
        ingestDocument.setFieldValue("list.0.field", "new_value");
        assertThat(ingestDocument.isSourceModified(), equalTo(true));
        Object object = ingestDocument.getSource().get("list");
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
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[test] is not an integer, cannot be used as an index as part of path [list.test]"));
            assertThat(ingestDocument.isSourceModified(), equalTo(false));
        }

        try {
            ingestDocument.setFieldValue("list.test.field", "new_value");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[test] is not an integer, cannot be used as an index as part of path [list.test.field]"));
            assertThat(ingestDocument.isSourceModified(), equalTo(false));
        }
    }

    public void testListSetFieldValueIndexOutOfBounds() {
        try {
            ingestDocument.setFieldValue("list.10", "value");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[10] is out of bounds for array with length [2] as part of path [list.10]"));
            assertThat(ingestDocument.isSourceModified(), equalTo(false));
        }

        try {
            ingestDocument.setFieldValue("list.10.field", "value");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[10] is out of bounds for array with length [2] as part of path [list.10.field]"));
            assertThat(ingestDocument.isSourceModified(), equalTo(false));
        }
    }

    public void testSetFieldValueEmptyName() {
        try {
            ingestDocument.setFieldValue("", "bar");
            fail("add field should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
            assertThat(ingestDocument.isSourceModified(), equalTo(false));
        }
    }

    public void testRemoveField() {
        ingestDocument.removeField("foo");
        assertThat(ingestDocument.isSourceModified(), equalTo(true));
        assertThat(ingestDocument.getSource().size(), equalTo(3));
        assertThat(ingestDocument.getSource().containsKey("foo"), equalTo(false));
    }

    public void testRemoveInnerField() {
        ingestDocument.removeField("fizz.buzz");
        assertThat(ingestDocument.getSource().size(), equalTo(4));
        assertThat(ingestDocument.getSource().get("fizz"), instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) ingestDocument.getSource().get("fizz");
        assertThat(map.size(), equalTo(2));
        assertThat(map.containsKey("buzz"), equalTo(false));

        ingestDocument.removeField("fizz.foo_null");
        assertThat(map.size(), equalTo(1));
        assertThat(ingestDocument.getSource().size(), equalTo(4));
        assertThat(ingestDocument.getSource().containsKey("fizz"), equalTo(true));
        assertThat(ingestDocument.isSourceModified(), equalTo(true));

        ingestDocument.removeField("fizz.1");
        assertThat(map.size(), equalTo(0));
        assertThat(ingestDocument.getSource().size(), equalTo(4));
        assertThat(ingestDocument.getSource().containsKey("fizz"), equalTo(true));
        assertThat(ingestDocument.isSourceModified(), equalTo(true));
    }

    public void testRemoveNonExistingField() {
        try {
            ingestDocument.removeField("does_not_exist");
            fail("remove field should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [does_not_exist] not present as part of path [does_not_exist]"));
            assertThat(ingestDocument.isSourceModified(), equalTo(false));
        }
    }

    public void testRemoveExistingParentTypeMismatch() {
        try {
            ingestDocument.removeField("foo.foo.bar");
            fail("remove field should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("cannot resolve [foo] from object of type [java.lang.String] as part of path [foo.foo.bar]"));
            assertThat(ingestDocument.isSourceModified(), equalTo(false));
        }
    }

    public void testListRemoveField() {
        ingestDocument.removeField("list.0.field");
        assertThat(ingestDocument.isSourceModified(), equalTo(true));
        assertThat(ingestDocument.getSource().size(), equalTo(4));
        assertThat(ingestDocument.getSource().containsKey("list"), equalTo(true));
        Object object = ingestDocument.getSource().get("list");
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
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("cannot remove [not_there] from null as part of path [fizz.foo_null.not_there]"));
            assertThat(ingestDocument.isSourceModified(), equalTo(false));
        }
    }

    public void testNestedRemoveFieldTypeMismatch() {
        try {
            ingestDocument.removeField("fizz.1.bar");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("cannot remove [bar] from object of type [java.lang.String] as part of path [fizz.1.bar]"));
            assertThat(ingestDocument.isSourceModified(), equalTo(false));
        }
    }

    public void testListRemoveFieldIndexNotNumeric() {
        try {
            ingestDocument.removeField("list.test");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[test] is not an integer, cannot be used as an index as part of path [list.test]"));
            assertThat(ingestDocument.isSourceModified(), equalTo(false));
        }
    }

    public void testListRemoveFieldIndexOutOfBounds() {
        try {
            ingestDocument.removeField("list.10");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[10] is out of bounds for array with length [2] as part of path [list.10]"));
            assertThat(ingestDocument.isSourceModified(), equalTo(false));
        }
    }

    public void testRemoveNullField() {
        try {
            ingestDocument.removeField(null);
            fail("remove field should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
            assertThat(ingestDocument.isSourceModified(), equalTo(false));
        }
    }

    public void testRemoveEmptyField() {
        try {
            ingestDocument.removeField("");
            fail("remove field should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("path cannot be null nor empty"));
            assertThat(ingestDocument.isSourceModified(), equalTo(false));
        }
    }

    public void testEqualsAndHashcode() throws Exception {
        String index = randomAsciiOfLengthBetween(1, 10);
        String type = randomAsciiOfLengthBetween(1, 10);
        String id = randomAsciiOfLengthBetween(1, 10);
        String fieldName = randomAsciiOfLengthBetween(1, 10);
        String fieldValue = randomAsciiOfLengthBetween(1, 10);
        IngestDocument ingestDocument = new IngestDocument(index, type, id, Collections.singletonMap(fieldName, fieldValue));

        boolean changed = false;
        String otherIndex;
        if (randomBoolean()) {
            otherIndex = randomAsciiOfLengthBetween(1, 10);
            changed = true;
        } else {
            otherIndex = index;
        }
        String otherType;
        if (randomBoolean()) {
            otherType = randomAsciiOfLengthBetween(1, 10);
            changed = true;
        } else {
            otherType = type;
        }
        String otherId;
        if (randomBoolean()) {
            otherId = randomAsciiOfLengthBetween(1, 10);
            changed = true;
        } else {
            otherId = id;
        }
        Map<String, Object> document;
        if (randomBoolean()) {
            document = Collections.singletonMap(randomAsciiOfLengthBetween(1, 10), randomAsciiOfLengthBetween(1, 10));
            changed = true;
        } else {
            document = Collections.singletonMap(fieldName, fieldValue);
        }

        IngestDocument otherIngestDocument = new IngestDocument(otherIndex, otherType, otherId, document);
        if (changed) {
            assertThat(ingestDocument, not(equalTo(otherIngestDocument)));
            assertThat(otherIngestDocument, not(equalTo(ingestDocument)));
        } else {
            assertThat(ingestDocument, equalTo(otherIngestDocument));
            assertThat(otherIngestDocument, equalTo(ingestDocument));
            assertThat(ingestDocument.hashCode(), equalTo(otherIngestDocument.hashCode()));
            IngestDocument thirdIngestDocument = new IngestDocument(index, type, id, Collections.singletonMap(fieldName, fieldValue));
            assertThat(thirdIngestDocument, equalTo(ingestDocument));
            assertThat(ingestDocument, equalTo(thirdIngestDocument));
            assertThat(ingestDocument.hashCode(), equalTo(thirdIngestDocument.hashCode()));
        }
    }

    public void testDeepCopy() {
        int iterations = scaledRandomIntBetween(8, 64);
        for (int i = 0; i < iterations; i++) {
            Map<String, Object> map = RandomDocumentPicks.randomDocument(random());
            Object copy = IngestDocument.deepCopy(map);
            assertThat("iteration: " + i, copy, equalTo(map));
            assertThat("iteration: " + i, copy, not(sameInstance(map)));
        }
    }
}
