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
package org.elasticsearch.test.rest.test;

import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.rest.json.JsonPath;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.*;

public class JsonPathTests extends ElasticsearchTestCase {

    @Test
    public void testEvaluateObjectPathEscape() throws Exception {
        String json = "{ \"field1\": { \"field2.field3\" : \"value2\" } }";
        JsonPath jsonPath = new JsonPath(json);
        Object object = jsonPath.evaluate("field1.field2\\.field3");
        assertThat(object, instanceOf(String.class));
        assertThat((String)object, equalTo("value2"));
    }

    @Test
    public void testEvaluateObjectPathWithDoubleDot() throws Exception {
        String json = "{ \"field1\": { \"field2\" : \"value2\" } }";
        JsonPath jsonPath = new JsonPath(json);
        Object object = jsonPath.evaluate("field1..field2");
        assertThat(object, instanceOf(String.class));
        assertThat((String)object, equalTo("value2"));
    }

    @Test
    public void testEvaluateObjectPathEndsWithDot() throws Exception {
        String json = "{ \"field1\": { \"field2\" : \"value2\" } }";
        JsonPath jsonPath = new JsonPath(json);
        Object object = jsonPath.evaluate("field1.field2.");
        assertThat(object, instanceOf(String.class));
        assertThat((String)object, equalTo("value2"));
    }

    @Test
    public void testEvaluateString() throws Exception {
        String json = "{ \"field1\": { \"field2\" : \"value2\" } }";
        JsonPath jsonPath = new JsonPath(json);
        Object object = jsonPath.evaluate("field1.field2");
        assertThat(object, instanceOf(String.class));
        assertThat((String)object, equalTo("value2"));
    }

    @Test
    public void testEvaluateInteger() throws Exception {
        String json = "{ \"field1\": { \"field2\" : 333 } }";
        JsonPath jsonPath = new JsonPath(json);
        Object object = jsonPath.evaluate("field1.field2");
        assertThat(object, instanceOf(Integer.class));
        assertThat((Integer)object, equalTo(333));
    }

    @Test
    public void testEvaluateDouble() throws Exception {
        String json = "{ \"field1\": { \"field2\" : 3.55 } }";
        JsonPath jsonPath = new JsonPath(json);
        Object object = jsonPath.evaluate("field1.field2");
        assertThat(object, instanceOf(Double.class));
        assertThat((Double)object, equalTo(3.55));
    }

    @Test
    public void testEvaluateArray() throws Exception {
        String json = "{ \"field1\": { \"array1\" : [ \"value1\", \"value2\" ] } }";
        JsonPath jsonPath = new JsonPath(json);
        Object object = jsonPath.evaluate("field1.array1");
        assertThat(object, instanceOf(List.class));
        List list = (List) object;
        assertThat(list.size(), equalTo(2));
        assertThat(list.get(0), instanceOf(String.class));
        assertThat((String)list.get(0), equalTo("value1"));
        assertThat(list.get(1), instanceOf(String.class));
        assertThat((String)list.get(1), equalTo("value2"));
    }

    @Test
    public void testEvaluateArrayElement() throws Exception {
        String json = "{ \"field1\": { \"array1\" : [ \"value1\", \"value2\" ] } }";
        JsonPath jsonPath = new JsonPath(json);
        Object object = jsonPath.evaluate("field1.array1.1");
        assertThat(object, instanceOf(String.class));
        assertThat((String)object, equalTo("value2"));
    }

    @Test
    public void testEvaluateArrayElementObject() throws Exception {
        String json = "{ \"field1\": { \"array1\" : [ {\"element\": \"value1\"}, {\"element\":\"value2\"} ] } }";
        JsonPath jsonPath = new JsonPath(json);
        Object object = jsonPath.evaluate("field1.array1.1.element");
        assertThat(object, instanceOf(String.class));
        assertThat((String)object, equalTo("value2"));
    }

    @Test
    public void testEvaluateArrayElementObjectWrongPath() throws Exception {
        String json = "{ \"field1\": { \"array1\" : [ {\"element\": \"value1\"}, {\"element\":\"value2\"} ] } }";
        JsonPath jsonPath = new JsonPath(json);
        Object object = jsonPath.evaluate("field1.array2.1.element");
        assertThat(object, nullValue());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testEvaluateObjectKeys() throws Exception {
        String json = "{ \"metadata\": { \"templates\" : {\"template_1\": { \"field\" : \"value\"}, \"template_2\": { \"field\" : \"value\"} } } }";
        JsonPath jsonPath = new JsonPath(json);
        Object object = jsonPath.evaluate("metadata.templates");
        assertThat(object, instanceOf(Map.class));
        Map<String, Object> map = (Map<String, Object>)object;
        assertThat(map.size(), equalTo(2));
        Set<String> strings = map.keySet();
        assertThat(strings, contains("template_1", "template_2"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testEvaluateEmptyPath() throws Exception {
        String json = "{ \"field1\": { \"array1\" : [ {\"element\": \"value1\"}, {\"element\":\"value2\"} ] } }";
        JsonPath jsonPath = new JsonPath(json);
        Object object = jsonPath.evaluate("");
        assertThat(object, notNullValue());
        assertThat(object, instanceOf(Map.class));
        assertThat(((Map<String, Object>)object).containsKey("field1"), equalTo(true));
    }
}
