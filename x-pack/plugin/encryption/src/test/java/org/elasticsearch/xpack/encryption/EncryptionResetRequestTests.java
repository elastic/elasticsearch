/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class EncryptionResetRequestTests extends ESTestCase {

    public void testRoundTripPreservesAcceptDataLossFlag() throws IOException {
        boolean acceptDataLoss = randomBoolean();
        EncryptionResetRequest request = new EncryptionResetRequest(
            TimeValue.timeValueSeconds(30),
            TimeValue.timeValueSeconds(30),
            acceptDataLoss
        );

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                EncryptionResetRequest deserialized = new EncryptionResetRequest(in);
                assertEquals(acceptDataLoss, deserialized.acceptDataLoss());
                assertEquals(request.masterNodeTimeout(), deserialized.masterNodeTimeout());
                assertEquals(request.ackTimeout(), deserialized.ackTimeout());
            }
        }
    }

    public void testValidateRejectsWhenAcceptDataLossIsFalse() {
        EncryptionResetRequest request = new EncryptionResetRequest(TimeValue.timeValueSeconds(30), TimeValue.timeValueSeconds(30), false);
        var exception = request.validate();
        assertNotNull(exception);
        assertTrue(exception.getMessage().contains("accept_data_loss must be set to true"));
    }

    public void testValidatePassesWhenAcceptDataLossIsTrue() {
        EncryptionResetRequest request = new EncryptionResetRequest(TimeValue.timeValueSeconds(30), TimeValue.timeValueSeconds(30), true);
        assertNull(request.validate());
    }
}
