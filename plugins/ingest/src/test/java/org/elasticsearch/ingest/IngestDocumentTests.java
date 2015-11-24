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
        document.put("fizz", innerObject);
        ingestDocument = new IngestDocument("index", "type", "id", document);
    }

    public void testSimpleGetPropertyValue() {
        assertThat(ingestDocument.getPropertyValue("foo", String.class), equalTo("bar"));
        assertThat(ingestDocument.getPropertyValue("int", Integer.class), equalTo(123));
    }

    public void testGetPropertyValueNullValue() {
        assertThat(ingestDocument.getPropertyValue("fizz.foo_null", Object.class), nullValue());
    }

    public void testSimpleGetPropertyValueTypeMismatch() {
        try {
            ingestDocument.getPropertyValue("int", String.class);
            fail("getProperty should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [int] of type [java.lang.Integer] cannot be cast to [java.lang.String]"));
        }

        try {
            ingestDocument.getPropertyValue("foo", Integer.class);
            fail("getProperty should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [foo] of type [java.lang.String] cannot be cast to [java.lang.Integer]"));
        }
    }

    public void testNestedGetPropertyValue() {
        assertThat(ingestDocument.getPropertyValue("fizz.buzz", String.class), equalTo("hello world"));
    }

    public void testGetPropertyValueNotFound() {
        assertThat(ingestDocument.getPropertyValue("not.here", String.class), nullValue());
    }

    public void testGetPropertyValueNull() {
        assertNull(ingestDocument.getPropertyValue(null, String.class));
    }

    public void testGetPropertyValueEmpty() {
        assertNull(ingestDocument.getPropertyValue("", String.class));
    }

    public void testHasProperty() {
        assertTrue(ingestDocument.hasPropertyValue("fizz"));
    }

    public void testHasPropertyValueNested() {
        assertTrue(ingestDocument.hasPropertyValue("fizz.buzz"));
    }

    public void testHasPropertyValueNotFound() {
        assertFalse(ingestDocument.hasPropertyValue("doesnotexist"));
    }

    public void testHasPropertyValueNestedNotFound() {
        assertFalse(ingestDocument.hasPropertyValue("fizz.doesnotexist"));
    }

    public void testHasPropertyValueNull() {
        assertFalse(ingestDocument.hasPropertyValue(null));
    }

    public void testHasPropertyValueNullValue() {
        assertTrue(ingestDocument.hasPropertyValue("fizz.foo_null"));
    }

    public void testHasPropertyValueEmpty() {
        assertFalse(ingestDocument.hasPropertyValue(""));
    }

    public void testSimpleSetPropertyValue() {
        ingestDocument.setPropertyValue("new_field", "foo");
        assertThat(ingestDocument.getSource().get("new_field"), equalTo("foo"));
        assertThat(ingestDocument.isModified(), equalTo(true));
    }

    public void testSetPropertyValueNullValue() {
        ingestDocument.setPropertyValue("new_field", null);
        assertThat(ingestDocument.getSource().containsKey("new_field"), equalTo(true));
        assertThat(ingestDocument.getSource().get("new_field"), nullValue());
        assertThat(ingestDocument.isModified(), equalTo(true));
    }

    @SuppressWarnings("unchecked")
    public void testNestedSetPropertyValue() {
        ingestDocument.setPropertyValue("a.b.c.d", "foo");
        assertThat(ingestDocument.getSource().get("a"), instanceOf(Map.class));
        Map<String, Object> a = (Map<String, Object>) ingestDocument.getSource().get("a");
        assertThat(a.get("b"), instanceOf(Map.class));
        Map<String, Object> b = (Map<String, Object>) a.get("b");
        assertThat(b.get("c"), instanceOf(Map.class));
        Map<String, Object> c = (Map<String, Object>) b.get("c");
        assertThat(c.get("d"), instanceOf(String.class));
        String d = (String) c.get("d");
        assertThat(d, equalTo("foo"));
        assertThat(ingestDocument.isModified(), equalTo(true));
    }

    public void testSetPropertyValueOnExistingField() {
        ingestDocument.setPropertyValue("foo", "newbar");
        assertThat(ingestDocument.getSource().get("foo"), equalTo("newbar"));
    }

    @SuppressWarnings("unchecked")
    public void testSetPropertyValueOnExistingParent() {
        ingestDocument.setPropertyValue("fizz.new", "bar");
        assertThat(ingestDocument.getSource().get("fizz"), instanceOf(Map.class));
        Map<String, Object> innerMap = (Map<String, Object>) ingestDocument.getSource().get("fizz");
        assertThat(innerMap.get("new"), instanceOf(String.class));
        String value = (String) innerMap.get("new");
        assertThat(value, equalTo("bar"));
        assertThat(ingestDocument.isModified(), equalTo(true));
    }

    public void testSetPropertyValueOnExistingParentTypeMismatch() {
        try {
            ingestDocument.setPropertyValue("fizz.buzz.new", "bar");
            fail("add field should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("cannot add field to parent [buzz] of type [java.lang.String], [java.util.Map] expected instead."));
            assertThat(ingestDocument.isModified(), equalTo(false));
        }
    }

    public void testSetPropertyValueOnExistingNullParent() {
        try {
            ingestDocument.setPropertyValue("fizz.foo_null.test", "bar");
            fail("add field should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("cannot add field to null parent, [java.util.Map] expected instead."));
            assertThat(ingestDocument.isModified(), equalTo(false));
        }
    }

    public void testSetPropertyValueNullName() {
        try {
            ingestDocument.setPropertyValue(null, "bar");
            fail("add field should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("cannot add null or empty field"));
            assertThat(ingestDocument.isModified(), equalTo(false));
        }
    }

    public void testSetPropertyValueEmptyName() {
        try {
            ingestDocument.setPropertyValue("", "bar");
            fail("add field should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("cannot add null or empty field"));
            assertThat(ingestDocument.isModified(), equalTo(false));
        }
    }

    public void testRemoveProperty() {
        ingestDocument.removeProperty("foo");
        assertThat(ingestDocument.isModified(), equalTo(true));
        assertThat(ingestDocument.getSource().size(), equalTo(2));
        assertThat(ingestDocument.getSource().containsKey("foo"), equalTo(false));
    }

    public void testRemoveInnerProperty() {
        ingestDocument.removeProperty("fizz.buzz");
        assertThat(ingestDocument.getSource().size(), equalTo(3));
        assertThat(ingestDocument.getSource().get("fizz"), instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) ingestDocument.getSource().get("fizz");
        assertThat(map.size(), equalTo(1));
        assertThat(map.containsKey("buzz"), equalTo(false));

        ingestDocument.removeProperty("fizz.foo_null");
        assertThat(map.size(), equalTo(0));
        assertThat(ingestDocument.getSource().size(), equalTo(3));
        assertThat(ingestDocument.getSource().containsKey("fizz"), equalTo(true));
        assertThat(ingestDocument.isModified(), equalTo(true));
    }

    public void testRemoveNonExistingProperty() {
        ingestDocument.removeProperty("does_not_exist");
        assertThat(ingestDocument.isModified(), equalTo(false));
        assertThat(ingestDocument.getSource().size(), equalTo(3));
    }

    public void testRemoveExistingParentTypeMismatch() {
        ingestDocument.removeProperty("foo.test");
        assertThat(ingestDocument.isModified(), equalTo(false));
        assertThat(ingestDocument.getSource().size(), equalTo(3));
    }

    public void testRemoveNullProperty() {
        ingestDocument.removeProperty(null);
        assertThat(ingestDocument.isModified(), equalTo(false));
        assertThat(ingestDocument.getSource().size(), equalTo(3));
    }

    public void testRemoveEmptyProperty() {
        ingestDocument.removeProperty("");
        assertThat(ingestDocument.isModified(), equalTo(false));
        assertThat(ingestDocument.getSource().size(), equalTo(3));
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
            IngestDocument thirdIngestDocument = new IngestDocument(index, type, id, Collections.singletonMap(fieldName, fieldValue));
            assertThat(thirdIngestDocument, equalTo(ingestDocument));
            assertThat(ingestDocument, equalTo(thirdIngestDocument));
        }
    }
}
