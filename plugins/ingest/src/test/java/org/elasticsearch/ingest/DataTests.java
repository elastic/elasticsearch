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

import java.util.HashMap;

import static org.hamcrest.Matchers.*;

public class DataTests extends ESTestCase {

    private Data data;

    @Before
    public void setData() {
        data = new Data("index", "type", "id",
                new HashMap<String, Object>() {{
                    put("foo", "bar");
                    put("fizz", new HashMap<String, Object>() {{
                        put("buzz", "hello world");
                    }});
                }});
    }

    public void testSimpleGetProperty() {
        assertThat(data.getProperty("foo"), equalTo("bar"));
    }

    public void testNestedGetProperty() {
        assertThat(data.getProperty("fizz.buzz"), equalTo("hello world"));
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
}
