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

import org.elasticsearch.common.xcontent.ObjectPath;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class StashableObjectPathTests extends ESTestCase {

    private static XContentBuilder randomXContentBuilder() throws IOException {
        //only string based formats are supported, no cbor nor smile
        XContentType xContentType = randomFrom(XContentType.JSON, XContentType.YAML);
        return XContentBuilder.builder(XContentFactory.xContent(xContentType));
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
        StashableObjectPath objectPath = new StashableObjectPath(
                ObjectPath.createFromXContent(xContentBuilder.contentType().xContent(), xContentBuilder.string()));
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
}
