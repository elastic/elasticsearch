/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.inference;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class NamedXContentObjectHelperTests extends ESTestCase {

    static class NamedTestObject implements NamedXContentObject {

        private String fieldValue;
        public static final ObjectParser<NamedTestObject, Void> PARSER =
            new ObjectParser<>("my_named_object", true, NamedTestObject::new);
        static {
            PARSER.declareString(NamedTestObject::setFieldValue, new ParseField("my_field"));
        }

        NamedTestObject() {

        }

        NamedTestObject(String value) {
            this.fieldValue = value;
        }

        @Override
        public String getName() {
            return "my_named_object";
        }

        public void setFieldValue(String value) {
            this.fieldValue = value;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (fieldValue != null) {
                builder.field("my_field", fieldValue);
            }
            builder.endObject();
            return builder;
        }
    }

    public void testSerializeInOrder() throws IOException {
        String expected =
            "{\"my_objects\":[{\"my_named_object\":{\"my_field\":\"value1\"}},{\"my_named_object\":{\"my_field\":\"value2\"}}]}";
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            List<NamedXContentObject> objects = Arrays.asList(new NamedTestObject("value1"), new NamedTestObject("value2"));
            NamedXContentObjectHelper.writeNamedObjects(builder, ToXContent.EMPTY_PARAMS, true, "my_objects", objects);
            builder.endObject();
            assertThat(BytesReference.bytes(builder).utf8ToString(), equalTo(expected));
        }
    }

    public void testSerialize() throws IOException {
        String expected = "{\"my_objects\":{\"my_named_object\":{\"my_field\":\"value1\"},\"my_named_object\":{\"my_field\":\"value2\"}}}";
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            List<NamedXContentObject> objects = Arrays.asList(new NamedTestObject("value1"), new NamedTestObject("value2"));
            NamedXContentObjectHelper.writeNamedObjects(builder, ToXContent.EMPTY_PARAMS, false, "my_objects", objects);
            builder.endObject();
            assertThat(BytesReference.bytes(builder).utf8ToString(), equalTo(expected));
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(Collections.singletonList(new NamedXContentRegistry.Entry(NamedXContentObject.class,
            new ParseField("my_named_object"),
            (p, c) -> NamedTestObject.PARSER.apply(p, null))));
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);
    }

}
