/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.idp.saml.support;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class SamlInitiateSingleSignOnAttributesTests extends ESTestCase {

    public void testConstructors() {
        // Test default constructor
        final SamlInitiateSingleSignOnAttributes attributes1 = new SamlInitiateSingleSignOnAttributes();
        assertThat(attributes1.getAttributes(), hasSize(0));

        // Test with empty attribute list
        final SamlInitiateSingleSignOnAttributes attributes2 = new SamlInitiateSingleSignOnAttributes();
        attributes2.setAttributes(Collections.emptyList());
        assertThat(attributes2.getAttributes(), hasSize(0));

        // Test with non-empty attribute list
        List<SamlInitiateSingleSignOnAttributes.Attribute> attributeList = new ArrayList<>();
        attributeList.add(new SamlInitiateSingleSignOnAttributes.Attribute("key1", Collections.singletonList("value1")));
        final SamlInitiateSingleSignOnAttributes attributes3 = new SamlInitiateSingleSignOnAttributes();
        attributes3.setAttributes(attributeList);
        assertThat(attributes3.getAttributes(), hasSize(1));
        assertThat(attributes3.getAttributes().get(0).getKey(), equalTo("key1"));
    }

    public void testEmptyAttributes() throws IOException {
        final SamlInitiateSingleSignOnAttributes attributes = new SamlInitiateSingleSignOnAttributes();
        assertThat(attributes.getAttributes(), hasSize(0));

        final XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        attributes.toXContent(builder, ToXContent.EMPTY_PARAMS);
        final String json = Strings.toString(builder);
        assertThat(json, equalTo("{\"attributes\":[]}"));

        final SamlInitiateSingleSignOnAttributes parsedAttributes = parseFromJson(json);
        assertThat(parsedAttributes.getAttributes(), hasSize(0));

        // Test wire serialization
        SamlInitiateSingleSignOnAttributes serialized = copySerialize(attributes);
        assertThat(serialized.getAttributes(), hasSize(0));
    }

    public void testWithAttributes() throws IOException {
        final SamlInitiateSingleSignOnAttributes attributes = new SamlInitiateSingleSignOnAttributes();

        List<SamlInitiateSingleSignOnAttributes.Attribute> attributeList = new ArrayList<>();
        attributeList.add(new SamlInitiateSingleSignOnAttributes.Attribute("key1", Arrays.asList("value1", "value2")));
        attributeList.add(new SamlInitiateSingleSignOnAttributes.Attribute("key2", Collections.singletonList("value3")));

        attributes.setAttributes(attributeList);

        assertThat(attributes.getAttributes(), hasSize(2));
        assertThat(attributes.getAttributes().get(0).getKey(), equalTo("key1"));
        assertThat(attributes.getAttributes().get(0).getValues(), hasSize(2));
        assertThat(attributes.getAttributes().get(0).getValues().get(0), equalTo("value1"));
        assertThat(attributes.getAttributes().get(0).getValues().get(1), equalTo("value2"));

        assertThat(attributes.getAttributes().get(1).getKey(), equalTo("key2"));
        assertThat(attributes.getAttributes().get(1).getValues(), hasSize(1));
        assertThat(attributes.getAttributes().get(1).getValues().get(0), equalTo("value3"));

        final XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        attributes.toXContent(builder, ToXContent.EMPTY_PARAMS);
        final String json = Strings.toString(builder);

        // Check that JSON contains expected values
        assertThat(json, containsString("key1"));
        assertThat(json, containsString("value1"));
        assertThat(json, containsString("value2"));
        assertThat(json, containsString("key2"));
        assertThat(json, containsString("value3"));

        // Check that parsed JSON matches original attributes
        final SamlInitiateSingleSignOnAttributes parsedAttributes = parseFromJson(json);
        assertThat(parsedAttributes.getAttributes(), hasSize(2));
        assertThat(parsedAttributes.getAttributes().get(0).getKey(), equalTo("key1"));
        assertThat(parsedAttributes.getAttributes().get(0).getValues(), hasSize(2));
        assertThat(parsedAttributes.getAttributes().get(1).getKey(), equalTo("key2"));
        assertThat(parsedAttributes.getAttributes().get(1).getValues(), hasSize(1));

        // Test equals/hashCode
        assertThat(attributes, equalTo(parsedAttributes));
        assertThat(attributes.hashCode(), equalTo(parsedAttributes.hashCode()));

        // Test wire serialization
        SamlInitiateSingleSignOnAttributes serialized = copySerialize(attributes);
        assertThat(serialized.getAttributes(), hasSize(2));
        assertThat(serialized.getAttributes().get(0).getKey(), equalTo("key1"));
        assertThat(serialized.getAttributes().get(0).getValues(), hasSize(2));
        assertThat(serialized.getAttributes().get(1).getKey(), equalTo("key2"));
        assertThat(serialized.getAttributes().get(1).getValues(), hasSize(1));
    }

    public void testToString() {
        final SamlInitiateSingleSignOnAttributes attributes = new SamlInitiateSingleSignOnAttributes();
        List<SamlInitiateSingleSignOnAttributes.Attribute> attributeList = new ArrayList<>();
        attributeList.add(new SamlInitiateSingleSignOnAttributes.Attribute("key1", Arrays.asList("value1", "value2")));
        attributes.setAttributes(attributeList);

        String toString = attributes.toString();
        assertThat(toString, containsString("SamlInitiateSingleSignOnAttributes"));
        assertThat(toString, containsString("key1"));
        assertThat(toString, containsString("value1"));
        assertThat(toString, containsString("value2"));

        // Test with multiple attributes
        attributeList.add(new SamlInitiateSingleSignOnAttributes.Attribute("key2", Collections.singletonList("value3")));
        attributes.setAttributes(attributeList);
        toString = attributes.toString();
        assertThat(toString, containsString("key1"));
        assertThat(toString, containsString("key2"));
        assertThat(toString, containsString("value3"));

        // Test with empty attributes
        final SamlInitiateSingleSignOnAttributes emptyAttributes = new SamlInitiateSingleSignOnAttributes();
        toString = emptyAttributes.toString();
        assertThat(toString, containsString("SamlInitiateSingleSignOnAttributes"));
        assertThat(toString, containsString("attributes=[]"));
    }

    public void testAttributeClass() throws IOException {
        SamlInitiateSingleSignOnAttributes.Attribute attribute = new SamlInitiateSingleSignOnAttributes.Attribute(
            "testKey",
            Arrays.asList("val1", "val2")
        );

        assertThat(attribute.getKey(), equalTo("testKey"));
        assertThat(attribute.getValues(), hasSize(2));
        assertThat(attribute.getValues().get(0), equalTo("val1"));
        assertThat(attribute.getValues().get(1), equalTo("val2"));

        // Test equals/hashCode
        SamlInitiateSingleSignOnAttributes.Attribute attribute2 = new SamlInitiateSingleSignOnAttributes.Attribute(
            "testKey",
            Arrays.asList("val1", "val2")
        );
        assertThat(attribute, equalTo(attribute2));
        assertThat(attribute.hashCode(), equalTo(attribute2.hashCode()));

        SamlInitiateSingleSignOnAttributes.Attribute attribute3 = new SamlInitiateSingleSignOnAttributes.Attribute(
            "differentKey",
            Arrays.asList("val1", "val2")
        );
        assertThat(attribute, not(equalTo(attribute3)));

        SamlInitiateSingleSignOnAttributes.Attribute attribute4 = new SamlInitiateSingleSignOnAttributes.Attribute(
            "testKey",
            Arrays.asList("differentValue")
        );
        assertThat(attribute, not(equalTo(attribute4)));

        // Test equals with different object types
        assertThat(attribute.equals("someString"), equalTo(false));
        assertThat(attribute.equals(null), equalTo(false));

        // Test setter methods
        SamlInitiateSingleSignOnAttributes.Attribute mutableAttr = new SamlInitiateSingleSignOnAttributes.Attribute(
            "testKey",
            Collections.emptyList()
        );
        mutableAttr.setKey("newKey");
        mutableAttr.setValues(Arrays.asList("newVal1", "newVal2"));
        assertThat(mutableAttr.getKey(), equalTo("newKey"));
        assertThat(mutableAttr.getValues(), hasSize(2));
        assertThat(mutableAttr.getValues().get(0), equalTo("newVal1"));
        assertThat(mutableAttr.getValues().get(1), equalTo("newVal2"));

        // Test toString
        String toString = attribute.toString();
        assertThat(toString, containsString("Attribute"));
        assertThat(toString, containsString("testKey"));
        assertThat(toString, containsString("val1"));
    }

    public void testEqualsAndHashCode() {
        // Create attributes objects with the same content
        List<SamlInitiateSingleSignOnAttributes.Attribute> attributeList1 = new ArrayList<>();
        attributeList1.add(new SamlInitiateSingleSignOnAttributes.Attribute("key1", Arrays.asList("value1", "value2")));
        attributeList1.add(new SamlInitiateSingleSignOnAttributes.Attribute("key2", Collections.singletonList("value3")));

        SamlInitiateSingleSignOnAttributes attributes1 = new SamlInitiateSingleSignOnAttributes();
        attributes1.setAttributes(attributeList1);

        List<SamlInitiateSingleSignOnAttributes.Attribute> attributeList2 = new ArrayList<>();
        attributeList2.add(new SamlInitiateSingleSignOnAttributes.Attribute("key1", Arrays.asList("value1", "value2")));
        attributeList2.add(new SamlInitiateSingleSignOnAttributes.Attribute("key2", Collections.singletonList("value3")));

        SamlInitiateSingleSignOnAttributes attributes2 = new SamlInitiateSingleSignOnAttributes();
        attributes2.setAttributes(attributeList2);

        // Test equals and hashCode for identical objects
        assertThat(attributes1, equalTo(attributes2));
        assertThat(attributes1.hashCode(), equalTo(attributes2.hashCode()));

        // Test equals with self
        assertThat(attributes1.equals(attributes1), equalTo(true));

        // Test different objects
        List<SamlInitiateSingleSignOnAttributes.Attribute> attributeList3 = new ArrayList<>();
        attributeList3.add(new SamlInitiateSingleSignOnAttributes.Attribute("key1", Arrays.asList("value1", "different")));
        SamlInitiateSingleSignOnAttributes attributes3 = new SamlInitiateSingleSignOnAttributes();
        attributes3.setAttributes(attributeList3);

        assertThat(attributes1.equals(attributes3), equalTo(false));

        // Test equals with null and different object types
        assertThat(attributes1.equals(null), equalTo(false));
        assertThat(attributes1.equals("string"), equalTo(false));
    }

    public void testValidate() {
        // Test with valid attributes - should pass validation
        final SamlInitiateSingleSignOnAttributes validAttributes = new SamlInitiateSingleSignOnAttributes();
        List<SamlInitiateSingleSignOnAttributes.Attribute> attributeList = new ArrayList<>();
        attributeList.add(new SamlInitiateSingleSignOnAttributes.Attribute("key1", Collections.singletonList("value1")));
        attributeList.add(new SamlInitiateSingleSignOnAttributes.Attribute("key2", Arrays.asList("value2A", "value2B")));
        validAttributes.setAttributes(attributeList);

        ActionRequestValidationException validationException = validAttributes.validate();
        assertNull("Valid attributes should pass validation", validationException);

        // Test with null key - should fail validation
        final SamlInitiateSingleSignOnAttributes nullKeyAttributes = new SamlInitiateSingleSignOnAttributes();
        attributeList = new ArrayList<>();
        attributeList.add(new SamlInitiateSingleSignOnAttributes.Attribute(null, Collections.singletonList("value1")));
        nullKeyAttributes.setAttributes(attributeList);

        validationException = nullKeyAttributes.validate();
        assertNotNull("Null key should fail validation", validationException);
        assertThat(validationException.validationErrors().size(), equalTo(1));
        assertThat(validationException.validationErrors().get(0), containsString("attribute key cannot be null or empty"));

        // Test with empty key - should fail validation
        final SamlInitiateSingleSignOnAttributes emptyKeyAttributes = new SamlInitiateSingleSignOnAttributes();
        attributeList = new ArrayList<>();
        attributeList.add(new SamlInitiateSingleSignOnAttributes.Attribute("", Collections.singletonList("value1")));
        emptyKeyAttributes.setAttributes(attributeList);

        validationException = emptyKeyAttributes.validate();
        assertNotNull("Empty key should fail validation", validationException);
        assertThat(validationException.validationErrors().size(), equalTo(1));
        assertThat(validationException.validationErrors().get(0), containsString("attribute key cannot be null or empty"));

        // Test with duplicate keys - should fail validation
        final SamlInitiateSingleSignOnAttributes duplicateKeyAttributes = new SamlInitiateSingleSignOnAttributes();
        attributeList = new ArrayList<>();
        attributeList.add(new SamlInitiateSingleSignOnAttributes.Attribute("duplicate_key", Collections.singletonList("value1")));
        attributeList.add(new SamlInitiateSingleSignOnAttributes.Attribute("unique_key", Collections.singletonList("value2")));
        attributeList.add(new SamlInitiateSingleSignOnAttributes.Attribute("duplicate_key", Arrays.asList("value3", "value4")));
        duplicateKeyAttributes.setAttributes(attributeList);

        validationException = duplicateKeyAttributes.validate();
        assertNotNull("Duplicate keys should fail validation", validationException);
        assertThat(validationException.validationErrors().size(), equalTo(1));
        assertThat(validationException.validationErrors().get(0), containsString("duplicate attribute key [duplicate_key] found"));
    }

    private SamlInitiateSingleSignOnAttributes parseFromJson(String json) throws IOException {
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            parser.nextToken(); // Start object
            return SamlInitiateSingleSignOnAttributes.fromXContent(parser);
        }
    }

    private SamlInitiateSingleSignOnAttributes copySerialize(SamlInitiateSingleSignOnAttributes attributes) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            attributes.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                return new SamlInitiateSingleSignOnAttributes(in);
            }
        }
    }
}
