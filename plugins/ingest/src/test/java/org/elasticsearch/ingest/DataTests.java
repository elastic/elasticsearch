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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.*;

public class DataTests extends ESTestCase {

    private Data data;

    @Before
    public void setData() {
        Map<String, Object> document = new HashMap<>();
        document.put("foo", "bar");
        document.put("int", 123);
        Map<String, Object> innerObject = new HashMap<>();
        innerObject.put("buzz", "hello world");
        innerObject.put("foo_null", null);
        document.put("fizz", innerObject);
        data = new Data("index", "type", "id", document);
    }

    public void testSimpleGetPropertyValue() {
        assertThat(data.getPropertyValue("foo", String.class), equalTo("bar"));
        assertThat(data.getPropertyValue("int", Integer.class), equalTo(123));
    }

    public void testGetPropertyValueNullValue() {
        assertThat(data.getPropertyValue("fizz.foo_null", Object.class), nullValue());
    }

    public void testSimpleGetPropertyValueTypeMismatch() {
        try {
            data.getPropertyValue("int", String.class);
            fail("getProperty should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [int] of type [java.lang.Integer] cannot be cast to [java.lang.String]"));
        }

        try {
            data.getPropertyValue("foo", Integer.class);
            fail("getProperty should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [foo] of type [java.lang.String] cannot be cast to [java.lang.Integer]"));
        }
    }

    public void testNestedGetPropertyValue() {
        assertThat(data.getPropertyValue("fizz.buzz", String.class), equalTo("hello world"));
    }

    public void testGetPropertyValueNotFound() {
        assertThat(data.getPropertyValue("not.here", String.class), nullValue());
    }

    public void testGetPropertyValueNull() {
        assertNull(data.getPropertyValue(null, String.class));
    }

    public void testGetPropertyValueEmpty() {
        assertNull(data.getPropertyValue("", String.class));
    }

    public void testHasProperty() {
        assertTrue(data.hasPropertyValue("fizz"));
    }

    public void testHasPropertyValueNested() {
        assertTrue(data.hasPropertyValue("fizz.buzz"));
    }

    public void testHasPropertyValueNotFound() {
        assertFalse(data.hasPropertyValue("doesnotexist"));
    }

    public void testHasPropertyValueNestedNotFound() {
        assertFalse(data.hasPropertyValue("fizz.doesnotexist"));
    }

    public void testHasPropertyValueNull() {
        assertFalse(data.hasPropertyValue(null));
    }

    public void testHasPropertyValueNullValue() {
        assertTrue(data.hasPropertyValue("fizz.foo_null"));
    }

    public void testHasPropertyValueEmpty() {
        assertFalse(data.hasPropertyValue(""));
    }

    public void testSimpleSetPropertyValue() {
        data.setPropertyValue("new_field", "foo");
        assertThat(data.getDocument().get("new_field"), equalTo("foo"));
    }

    public void testSetPropertyValueNullValue() {
        data.setPropertyValue("new_field", null);
        assertThat(data.getDocument().containsKey("new_field"), equalTo(true));
        assertThat(data.getDocument().get("new_field"), nullValue());
    }

    @SuppressWarnings("unchecked")
    public void testNestedSetPropertyValue() {
        data.setPropertyValue("a.b.c.d", "foo");
        assertThat(data.getDocument().get("a"), instanceOf(Map.class));
        Map<String, Object> a = (Map<String, Object>) data.getDocument().get("a");
        assertThat(a.get("b"), instanceOf(Map.class));
        Map<String, Object> b = (Map<String, Object>) a.get("b");
        assertThat(b.get("c"), instanceOf(Map.class));
        Map<String, Object> c = (Map<String, Object>) b.get("c");
        assertThat(c.get("d"), instanceOf(String.class));
        String d = (String) c.get("d");
        assertThat(d, equalTo("foo"));
    }

    public void testSetPropertyValueOnExistingField() {
        data.setPropertyValue("foo", "newbar");
        assertThat(data.getDocument().get("foo"), equalTo("newbar"));
    }

    @SuppressWarnings("unchecked")
    public void testSetPropertyValueOnExistingParent() {
        data.setPropertyValue("fizz.new", "bar");
        assertThat(data.getDocument().get("fizz"), instanceOf(Map.class));
        Map<String, Object> innerMap = (Map<String, Object>) data.getDocument().get("fizz");
        assertThat(innerMap.get("new"), instanceOf(String.class));
        String value = (String) innerMap.get("new");
        assertThat(value, equalTo("bar"));
    }

    public void testSetPropertyValueOnExistingParentTypeMismatch() {
        try {
            data.setPropertyValue("fizz.buzz.new", "bar");
            fail("add field should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("cannot add field to parent [buzz] of type [java.lang.String], [java.util.Map] expected instead."));
        }
    }

    public void testSetPropertyValueOnExistingNullParent() {
        try {
            data.setPropertyValue("fizz.foo_null.test", "bar");
            fail("add field should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("cannot add field to null parent, [java.util.Map] expected instead."));
        }
    }

    public void testSetPropertyValueNullName() {
        try {
            data.setPropertyValue(null, "bar");
            fail("add field should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("cannot add null or empty field"));
        }
    }

    public void testSetPropertyValueEmptyName() {
        try {
            data.setPropertyValue("", "bar");
            fail("add field should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("cannot add null or empty field"));
        }
    }

    public void testRemoveProperty() {
        data.removeProperty("foo");
        assertThat(data.getDocument().size(), equalTo(2));
        assertThat(data.getDocument().containsKey("foo"), equalTo(false));
    }

    public void testRemoveInnerProperty() {
        data.removeProperty("fizz.buzz");
        assertThat(data.getDocument().size(), equalTo(3));
        assertThat(data.getDocument().get("fizz"), instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>)data.getDocument().get("fizz");
        assertThat(map.size(), equalTo(1));
        assertThat(map.containsKey("buzz"), equalTo(false));

        data.removeProperty("fizz.foo_null");
        assertThat(map.size(), equalTo(0));
        assertThat(data.getDocument().size(), equalTo(3));
        assertThat(data.getDocument().containsKey("fizz"), equalTo(true));
    }

    public void testRemoveNonExistingProperty() {
        data.removeProperty("does_not_exist");
        assertThat(data.getDocument().size(), equalTo(3));
    }

    public void testRemoveExistingParentTypeMismatch() {
        data.removeProperty("foo.test");
        assertThat(data.getDocument().size(), equalTo(3));
    }

    public void testRemoveNullProperty() {
        data.removeProperty(null);
        assertThat(data.getDocument().size(), equalTo(3));
    }

    public void testRemoveEmptyProperty() {
        data.removeProperty("");
        assertThat(data.getDocument().size(), equalTo(3));
    }

    public void testEqualsAndHashcode() throws Exception {
        String index = randomAsciiOfLengthBetween(1, 10);
        String type = randomAsciiOfLengthBetween(1, 10);
        String id = randomAsciiOfLengthBetween(1, 10);
        String fieldName = randomAsciiOfLengthBetween(1, 10);
        String fieldValue = randomAsciiOfLengthBetween(1, 10);
        Data data = new Data(index, type, id, Collections.singletonMap(fieldName, fieldValue));

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

        Data otherData = new Data(otherIndex, otherType, otherId, document);
        if (changed) {
            assertThat(data, not(equalTo(otherData)));
            assertThat(otherData, not(equalTo(data)));
        } else {
            assertThat(data, equalTo(otherData));
            assertThat(otherData, equalTo(data));
            Data thirdData = new Data(index, type, id, Collections.singletonMap(fieldName, fieldValue));
            assertThat(thirdData, equalTo(data));
            assertThat(data, equalTo(thirdData));
        }
    }
}
