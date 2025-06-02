/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.idp.saml.support.SamlInitiateSingleSignOnAttributes;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;

public class SamlInitiateSingleSignOnRequestTests extends ESTestCase {

    public void testSerialization() throws Exception {
        final SamlInitiateSingleSignOnRequest request = new SamlInitiateSingleSignOnRequest();
        request.setSpEntityId("https://kibana_url");
        request.setAssertionConsumerService("https://kibana_url/acs");
        assertThat("An invalid request is not guaranteed to serialize correctly", request.validate(), nullValue());
        final BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        final SamlInitiateSingleSignOnRequest request1 = new SamlInitiateSingleSignOnRequest(out.bytes().streamInput());
        assertThat(request1.getSpEntityId(), equalTo(request.getSpEntityId()));
        assertThat(request1.getAssertionConsumerService(), equalTo(request.getAssertionConsumerService()));
        final ActionRequestValidationException validationException = request1.validate();
        assertNull(validationException);
    }

    public void testValidation() {
        final SamlInitiateSingleSignOnRequest request1 = new SamlInitiateSingleSignOnRequest();
        final ActionRequestValidationException validationException = request1.validate();
        assertNotNull(validationException);
        assertThat(validationException.validationErrors().size(), equalTo(2));
        assertThat(validationException.validationErrors().get(0), containsString("entity_id is missing"));
        assertThat(validationException.validationErrors().get(1), containsString("acs is missing"));
    }

    public void testBlankAttributeKeysValidation() {
        // Create request with valid required fields
        final SamlInitiateSingleSignOnRequest request = new SamlInitiateSingleSignOnRequest();
        request.setSpEntityId("https://kibana_url");
        request.setAssertionConsumerService("https://kibana_url/acs");

        // Test with valid attribute keys
        Map<String, List<String>> attributeMap = new HashMap<>();
        attributeMap.put("key1", Collections.singletonList("value1"));
        attributeMap.put("key2", Arrays.asList("value2A", "value2B"));
        SamlInitiateSingleSignOnAttributes attributes = new SamlInitiateSingleSignOnAttributes(attributeMap);
        request.setAttributes(attributes);

        // Should pass validation
        ActionRequestValidationException validationException = request.validate();
        assertNull("Request with valid attribute keys should pass validation", validationException);

        // Test with empty attribute key - should be invalid
        attributeMap = new HashMap<>();
        attributeMap.put("", Collections.singletonList("value1"));
        attributeMap.put("unique_key", Collections.singletonList("value2"));
        attributes = new SamlInitiateSingleSignOnAttributes(attributeMap);
        request.setAttributes(attributes);

        // Should fail validation with appropriate error message
        validationException = request.validate();
        assertNotNull("Request with empty attribute key should fail validation", validationException);
        assertThat(validationException.validationErrors().size(), equalTo(1));
        assertThat(validationException.validationErrors().get(0), containsString("attribute key cannot be null or empty"));
    }
}
