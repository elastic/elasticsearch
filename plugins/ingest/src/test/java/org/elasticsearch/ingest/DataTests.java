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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class DataTests extends ESTestCase {

    private Data data;

    @Before
    public void setData() {
        Map<String, Object> document = new HashMap<>();
        document.put("foo", "bar");
        Map<String, Object> innerObject = new HashMap<>();
        innerObject.put("buzz", "hello world");
        document.put("fizz", innerObject);
        data = new Data("index", "type", "id", document);
    }

    public void testSimpleGetProperty() {
        assertThat(data.getProperty("foo"), equalTo("bar"));
    }

    public void testNestedGetProperty() {
        assertThat(data.getProperty("fizz.buzz"), equalTo("hello world"));
    }

    public void testGetPropertyNotFound() {
        data.getProperty("not.here");
        assertThat(data.getProperty("not.here"), nullValue());
    }

    public void testContainsProperty() {
        assertTrue(data.containsProperty("fizz"));
    }

    public void testContainsProperty_Nested() {
        assertTrue(data.containsProperty("fizz.buzz"));
    }

    public void testContainsProperty_NotFound() {
        assertFalse(data.containsProperty("doesnotexist"));
    }

    public void testContainsProperty_NestedNotFound() {
        assertFalse(data.containsProperty("fizz.doesnotexist"));
    }

    public void testSimpleAddField() {
        data.addField("new_field", "foo");
        assertThat(data.getDocument().get("new_field"), equalTo("foo"));
    }

    public void testNestedAddField() {
        data.addField("a.b.c.d", "foo");
        assertThat(data.getProperty("a.b.c.d"), equalTo("foo"));
    }

    public void testAddFieldOnExistingField() {
        data.addField("foo", "newbar");
        assertThat(data.getProperty("foo"), equalTo("newbar"));
    }

    public void testAddFieldOnExistingParent() {
        data.addField("fizz.new", "bar");
        assertThat(data.getProperty("fizz.new"), equalTo("bar"));
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
