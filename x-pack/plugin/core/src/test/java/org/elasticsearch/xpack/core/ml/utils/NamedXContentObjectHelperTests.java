/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.client.ml.inference.NamedXContentObject;
import org.elasticsearch.client.ml.inference.NamedXContentObjectHelper;
import org.elasticsearch.common.xcontent.ParseField;
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

        void setFieldValue(String value) {
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
