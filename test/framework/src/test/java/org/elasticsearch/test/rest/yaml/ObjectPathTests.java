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
package org.elasticsearch.test.rest.yaml;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ObjectPathTests extends ESTestCase {

    private static XContentBuilder randomXContentBuilder() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        return XContentBuilder.builder(XContentFactory.xContent(xContentType));
    }

    public void testEvaluateObjectPathEscape() throws Exception {
        XContentBuilder xContentBuilder = randomXContentBuilder();
        xContentBuilder.startObject();
        xContentBuilder.startObject("field1");
        xContentBuilder.field("field2.field3", "value2");
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        ObjectPath objectPath = ObjectPath.createFromXContent(xContentBuilder.contentType().xContent(), xContentBuilder.bytes());
        Object object = objectPath.evaluate("field1.field2\\.field3");
        assertThat(object, instanceOf(String.class));
        assertThat(object, equalTo("value2"));
    }

    public void testEvaluateObjectPathWithDots() throws Exception {
        XContentBuilder xContentBuilder = randomXContentBuilder();
        xContentBuilder.startObject();
        xContentBuilder.startObject("field1");
        xContentBuilder.field("field2", "value2");
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        ObjectPath objectPath = ObjectPath.createFromXContent(xContentBuilder.contentType().xContent(), xContentBuilder.bytes());
        Object object = objectPath.evaluate("field1..field2");
        assertThat(object, instanceOf(String.class));
        assertThat(object, equalTo("value2"));
        object = objectPath.evaluate("field1.field2.");
        assertThat(object, instanceOf(String.class));
        assertThat(object, equalTo("value2"));
        object = objectPath.evaluate("field1.field2");
        assertThat(object, instanceOf(String.class));
        assertThat(object, equalTo("value2"));
    }

    public void testEvaluateInteger() throws Exception {
        XContentBuilder xContentBuilder = randomXContentBuilder();
        xContentBuilder.startObject();
        xContentBuilder.startObject("field1");
        xContentBuilder.field("field2", 333);
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        ObjectPath objectPath = ObjectPath.createFromXContent(xContentBuilder.contentType().xContent(), xContentBuilder.bytes());
        Object object = objectPath.evaluate("field1.field2");
        assertThat(object, instanceOf(Integer.class));
        assertThat(object, equalTo(333));
    }

    public void testEvaluateDouble() throws Exception {
        XContentBuilder xContentBuilder = randomXContentBuilder();
        xContentBuilder.startObject();
        xContentBuilder.startObject("field1");
        xContentBuilder.field("field2", 3.55);
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        ObjectPath objectPath = ObjectPath.createFromXContent(xContentBuilder.contentType().xContent(), xContentBuilder.bytes());
        Object object = objectPath.evaluate("field1.field2");
        assertThat(object, instanceOf(Double.class));
        assertThat(object, equalTo(3.55));
    }

    public void testEvaluateArray() throws Exception {
        XContentBuilder xContentBuilder = randomXContentBuilder();
        xContentBuilder.startObject();
        xContentBuilder.startObject("field1");
        xContentBuilder.array("array1", "value1", "value2");
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        ObjectPath objectPath = ObjectPath.createFromXContent(xContentBuilder.contentType().xContent(), xContentBuilder.bytes());
        Object object = objectPath.evaluate("field1.array1");
        assertThat(object, instanceOf(List.class));
        List list = (List) object;
        assertThat(list.size(), equalTo(2));
        assertThat(list.get(0), instanceOf(String.class));
        assertThat(list.get(0), equalTo("value1"));
        assertThat(list.get(1), instanceOf(String.class));
        assertThat(list.get(1), equalTo("value2"));
        object = objectPath.evaluate("field1.array1.1");
        assertThat(object, instanceOf(String.class));
        assertThat(object, equalTo("value2"));
    }

    @SuppressWarnings("unchecked")
    public void testEvaluateArrayElementObject() throws Exception {
        XContentBuilder xContentBuilder = randomXContentBuilder();
        xContentBuilder.startObject();
        xContentBuilder.startObject("field1");
        xContentBuilder.startArray("array1");
        xContentBuilder.startObject();
        xContentBuilder.field("element", "value1");
        xContentBuilder.endObject();
        xContentBuilder.startObject();
        xContentBuilder.field("element", "value2");
        xContentBuilder.endObject();
        xContentBuilder.endArray();
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        ObjectPath objectPath = ObjectPath.createFromXContent(xContentBuilder.contentType().xContent(), xContentBuilder.bytes());
        Object object = objectPath.evaluate("field1.array1.1.element");
        assertThat(object, instanceOf(String.class));
        assertThat(object, equalTo("value2"));
        object = objectPath.evaluate("");
        assertThat(object, notNullValue());
        assertThat(object, instanceOf(Map.class));
        assertThat(((Map<String, Object>)object).containsKey("field1"), equalTo(true));
        object = objectPath.evaluate("field1.array2.1.element");
        assertThat(object, nullValue());
    }

    @SuppressWarnings("unchecked")
    public void testEvaluateObjectKeys() throws Exception {
        XContentBuilder xContentBuilder = randomXContentBuilder();
        xContentBuilder.startObject();
        xContentBuilder.startObject("metadata");
        xContentBuilder.startObject("templates");
        xContentBuilder.startObject("template_1");
        xContentBuilder.field("field", "value");
        xContentBuilder.endObject();
        xContentBuilder.startObject("template_2");
        xContentBuilder.field("field", "value");
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        ObjectPath objectPath = ObjectPath.createFromXContent(xContentBuilder.contentType().xContent(), xContentBuilder.bytes());
        Object object = objectPath.evaluate("metadata.templates");
        assertThat(object, instanceOf(Map.class));
        Map<String, Object> map = (Map<String, Object>)object;
        assertThat(map.size(), equalTo(2));
        Set<String> strings = map.keySet();
        assertThat(strings, contains("template_1", "template_2"));
    }

    public void testEvaluateStashInPropertyName() throws Exception {
        XContentBuilder xContentBuilder = randomXContentBuilder();
        xContentBuilder.startObject();
        xContentBuilder.startObject("field1");
        xContentBuilder.startObject("elements");
        xContentBuilder.field("element1", "value1");
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        ObjectPath objectPath = ObjectPath.createFromXContent(xContentBuilder.contentType().xContent(), xContentBuilder.bytes());
        try {
            objectPath.evaluate("field1.$placeholder.element1");
            fail("evaluate should have failed due to unresolved placeholder");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("stashed value not found for key [placeholder]"));
        }

        // Stashed value is whole property name
        Stash stash = new Stash();
        stash.stashValue("placeholder", "elements");
        Object object = objectPath.evaluate("field1.$placeholder.element1", stash);
        assertThat(object, notNullValue());
        assertThat(object.toString(), equalTo("value1"));

        // Stash key has dots
        Map<String, Object> stashedObject = new HashMap<>();
        stashedObject.put("subobject", "elements");
        stash.stashValue("object", stashedObject);
        object = objectPath.evaluate("field1.$object\\.subobject.element1", stash);
        assertThat(object, notNullValue());
        assertThat(object.toString(), equalTo("value1"));

        // Stashed value is part of property name
        stash.stashValue("placeholder", "ele");
        object = objectPath.evaluate("field1.${placeholder}ments.element1", stash);
        assertThat(object, notNullValue());
        assertThat(object.toString(), equalTo("value1"));

        // Stashed value is inside of property name
        stash.stashValue("placeholder", "le");
        object = objectPath.evaluate("field1.e${placeholder}ments.element1", stash);
        assertThat(object, notNullValue());
        assertThat(object.toString(), equalTo("value1"));

        // Multiple stashed values in property name
        stash.stashValue("placeholder", "le");
        stash.stashValue("placeholder2", "nts");
        object = objectPath.evaluate("field1.e${placeholder}me${placeholder2}.element1", stash);
        assertThat(object, notNullValue());
        assertThat(object.toString(), equalTo("value1"));

        // Stashed value is part of property name and has dots
        stashedObject.put("subobject", "ele");
        stash.stashValue("object", stashedObject);
        object = objectPath.evaluate("field1.${object\\.subobject}ments.element1", stash);
        assertThat(object, notNullValue());
        assertThat(object.toString(), equalTo("value1"));
    }

    @SuppressWarnings("unchecked")
    public void testEvaluateArrayAsRoot() throws Exception {
        XContentBuilder xContentBuilder = randomXContentBuilder();
        xContentBuilder.startArray();
        xContentBuilder.startObject();
        xContentBuilder.field("alias", "test_alias1");
        xContentBuilder.field("index", "test1");
        xContentBuilder.endObject();
        xContentBuilder.startObject();
        xContentBuilder.field("alias", "test_alias2");
        xContentBuilder.field("index", "test2");
        xContentBuilder.endObject();
        xContentBuilder.endArray();
        ObjectPath objectPath = ObjectPath.createFromXContent(
                XContentFactory.xContent(xContentBuilder.contentType()), xContentBuilder.bytes());
        Object object = objectPath.evaluate("");
        assertThat(object, notNullValue());
        assertThat(object, instanceOf(List.class));
        assertThat(((List<Object>)object).size(), equalTo(2));
        object = objectPath.evaluate("0");
        assertThat(object, notNullValue());
        assertThat(object, instanceOf(Map.class));
        assertThat(((Map<String, Object>)object).get("alias"), equalTo("test_alias1"));
        object = objectPath.evaluate("1.index");
        assertThat(object, notNullValue());
        assertThat(object, instanceOf(String.class));
        assertThat(object, equalTo("test2"));
    }
}
