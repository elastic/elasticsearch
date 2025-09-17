/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.support;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matchers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SamlInitiateSingleSignOnAttributesTests extends ESTestCase {

    public void testConstructors() throws Exception {
        final SamlInitiateSingleSignOnAttributes attributes1 = new SamlInitiateSingleSignOnAttributes(Collections.emptyMap());
        assertThat(attributes1.getAttributes(), Matchers.anEmptyMap());

        // Test with non-empty map
        Map<String, List<String>> attributeMap = new HashMap<>();
        attributeMap.put("key1", Collections.singletonList("value1"));
        final SamlInitiateSingleSignOnAttributes attributes3 = new SamlInitiateSingleSignOnAttributes(attributeMap);
        assertThat(attributes3.getAttributes().size(), equalTo(1));
    }

    public void testEmptyAttributes() throws Exception {
        final SamlInitiateSingleSignOnAttributes attributes = new SamlInitiateSingleSignOnAttributes(Collections.emptyMap());

        // Test toXContent
        XContentBuilder builder = XContentFactory.jsonBuilder();
        attributes.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = BytesReference.bytes(builder).utf8ToString();

        final SamlInitiateSingleSignOnAttributes parsedAttributes = parseFromJson(json);
        assertThat(parsedAttributes.getAttributes(), Matchers.anEmptyMap());

        // Test serialization
        SamlInitiateSingleSignOnAttributes serialized = copySerialize(attributes);
        assertThat(serialized.getAttributes(), Matchers.anEmptyMap());
    }

    public void testWithAttributes() throws Exception {
        Map<String, List<String>> attributeMap = new HashMap<>();
        attributeMap.put("key1", Arrays.asList("value1", "value2"));
        attributeMap.put("key2", Collections.singletonList("value3"));
        final SamlInitiateSingleSignOnAttributes attributes = new SamlInitiateSingleSignOnAttributes(attributeMap);

        // Test getAttributes
        Map<String, List<String>> returnedAttributes = attributes.getAttributes();
        assertThat(returnedAttributes.size(), equalTo(2));
        assertThat(returnedAttributes.get("key1").size(), equalTo(2));
        assertThat(returnedAttributes.get("key1").get(0), equalTo("value1"));
        assertThat(returnedAttributes.get("key1").get(1), equalTo("value2"));
        assertThat(returnedAttributes.get("key2").size(), equalTo(1));
        assertThat(returnedAttributes.get("key2").get(0), equalTo("value3"));

        // Test immutability of returned attributes
        expectThrows(UnsupportedOperationException.class, () -> returnedAttributes.put("newKey", Collections.singletonList("value")));
        expectThrows(UnsupportedOperationException.class, () -> returnedAttributes.get("key1").add("value3"));

        // Test validate
        ActionRequestValidationException validationException = attributes.validate();
        assertNull(validationException);

        // Test toXContent
        XContentBuilder builder = XContentFactory.jsonBuilder();
        attributes.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = BytesReference.bytes(builder).utf8ToString();

        // Test parsing from JSON
        final SamlInitiateSingleSignOnAttributes parsedAttributes = parseFromJson(json);
        assertThat(parsedAttributes.getAttributes().size(), equalTo(2));
        assertThat(parsedAttributes.getAttributes().get("key1").size(), equalTo(2));
        assertThat(parsedAttributes.getAttributes().get("key1").get(0), equalTo("value1"));
        assertThat(parsedAttributes.getAttributes().get("key1").get(1), equalTo("value2"));
        assertThat(parsedAttributes.getAttributes().get("key2").size(), equalTo(1));
        assertThat(parsedAttributes.getAttributes().get("key2").get(0), equalTo("value3"));

        // Test serialization
        SamlInitiateSingleSignOnAttributes serialized = copySerialize(attributes);
        assertThat(serialized.getAttributes().size(), equalTo(2));
        assertThat(serialized.getAttributes().get("key1").size(), equalTo(2));
        assertThat(serialized.getAttributes().get("key1").get(0), equalTo("value1"));
        assertThat(serialized.getAttributes().get("key1").get(1), equalTo("value2"));
        assertThat(serialized.getAttributes().get("key2").size(), equalTo(1));
        assertThat(serialized.getAttributes().get("key2").get(0), equalTo("value3"));
    }

    public void testToString() {
        Map<String, List<String>> attributeMap = new HashMap<>();
        attributeMap.put("key1", Arrays.asList("value1", "value2"));
        final SamlInitiateSingleSignOnAttributes attributes = new SamlInitiateSingleSignOnAttributes(attributeMap);

        String toString = attributes.toString();
        assertThat(toString, containsString("SamlInitiateSingleSignOnAttributes"));
        assertThat(toString, containsString("key1"));
        assertThat(toString, containsString("value1"));
        assertThat(toString, containsString("value2"));

        // Test empty attributes
        final SamlInitiateSingleSignOnAttributes emptyAttributes = new SamlInitiateSingleSignOnAttributes(Collections.emptyMap());
        toString = emptyAttributes.toString();
        assertThat(toString, containsString("SamlInitiateSingleSignOnAttributes"));
        assertThat(toString, containsString("attributes={}"));
    }

    public void testValidation() throws Exception {
        // Test validation with empty key
        Map<String, List<String>> attributeMap = new HashMap<>();
        attributeMap.put("", Arrays.asList("value1", "value2"));
        SamlInitiateSingleSignOnAttributes attributes = new SamlInitiateSingleSignOnAttributes(attributeMap);

        ActionRequestValidationException validationException = attributes.validate();
        assertNotNull(validationException);
        assertThat(validationException.getMessage(), containsString("attribute key cannot be null or empty"));

        // Test validation with null key
        attributeMap = new HashMap<>();
        attributeMap.put(null, Collections.singletonList("value"));
        attributes = new SamlInitiateSingleSignOnAttributes(attributeMap);

        validationException = attributes.validate();
        assertNotNull(validationException);
        assertThat(validationException.getMessage(), containsString("attribute key cannot be null or empty"));
    }

    public void testEqualsAndHashCode() {
        Map<String, List<String>> attributeMap1 = new HashMap<>();
        attributeMap1.put("key1", Arrays.asList("value1", "value2"));
        attributeMap1.put("key2", Collections.singletonList("value3"));

        SamlInitiateSingleSignOnAttributes attributes1 = new SamlInitiateSingleSignOnAttributes(attributeMap1);

        Map<String, List<String>> attributeMap2 = new HashMap<>();
        attributeMap2.put("key1", Arrays.asList("value1", "value2"));
        attributeMap2.put("key2", Collections.singletonList("value3"));

        SamlInitiateSingleSignOnAttributes attributes2 = new SamlInitiateSingleSignOnAttributes(attributeMap2);

        // Test equals
        assertEquals(attributes1, attributes2);
        assertEquals(attributes2, attributes1);

        // Test hashCode
        assertThat(attributes1.hashCode(), equalTo(attributes2.hashCode()));

        // Test with different values
        Map<String, List<String>> attributeMap3 = new HashMap<>();
        attributeMap3.put("key1", Arrays.asList("different", "value2"));
        attributeMap3.put("key2", Collections.singletonList("value3"));

        SamlInitiateSingleSignOnAttributes attributes3 = new SamlInitiateSingleSignOnAttributes(attributeMap3);

        assertNotEquals(attributes1, attributes3);

        // Test with missing key
        Map<String, List<String>> attributeMap4 = new HashMap<>();
        attributeMap4.put("key1", Arrays.asList("value1", "value2"));

        SamlInitiateSingleSignOnAttributes attributes4 = new SamlInitiateSingleSignOnAttributes(attributeMap4);

        assertNotEquals(attributes1, attributes4);
    }

    private SamlInitiateSingleSignOnAttributes parseFromJson(String json) throws IOException {
        try (
            InputStream stream = new ByteArrayInputStream(json.getBytes("UTF-8"));
            XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, stream)
        ) {
            parser.nextToken(); // Start object
            return SamlInitiateSingleSignOnAttributes.fromXContent(parser);
        }
    }

    private SamlInitiateSingleSignOnAttributes copySerialize(SamlInitiateSingleSignOnAttributes original) throws IOException {
        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
        original.writeTo(out);
        out.flush();

        ByteArrayInputStream inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput in = new InputStreamStreamInput(inBuffer);
        return new SamlInitiateSingleSignOnAttributes(in);
    }
}
