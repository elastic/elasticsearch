/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.saml;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class SamlSpMetadataRequestTests extends ESTestCase {

    public void testValidateFailsWhenRealmEmpty() {
        final SamlSpMetadataRequest samlSPMetadataRequest = new SamlSpMetadataRequest("");
        final ActionRequestValidationException validationException = samlSPMetadataRequest.validate();
        assertThat(validationException.getMessage(), containsString("Realm name may not be empty"));
    }

    public void testValidateSerialization() throws IOException {
        final SamlSpMetadataRequest samlSPMetadataRequest = new SamlSpMetadataRequest("saml1");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            samlSPMetadataRequest.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                final SamlSpMetadataRequest serialized = new SamlSpMetadataRequest(in);
                assertEquals(samlSPMetadataRequest.getRealmName(), serialized.getRealmName());
            }
        }
    }

    public void testValidateToString() {
        final SamlSpMetadataRequest samlSPMetadataRequest = new SamlSpMetadataRequest("saml1");
        assertThat(samlSPMetadataRequest.toString(), containsString("{realmName=saml1}"));
    }
}
