/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class SnapshotEncryptedDataTests extends ESTestCase {

    public void testWireRoundTrip() throws IOException {
        SnapshotEncryptedData original = new SnapshotEncryptedData(
            new SecureString(randomAlphaOfLengthBetween(15, 30).toCharArray()),
            randomBoolean() ? null : randomAlphaOfLengthBetween(3, 10)
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                SnapshotEncryptedData deserialized = new SnapshotEncryptedData(in);
                assertEquals(original, deserialized);
                assertEquals(original.hashCode(), deserialized.hashCode());
            }
        }
    }

    public void testFromMap() {
        SnapshotEncryptedData parsed = SnapshotEncryptedData.fromMap(
            Map.of("type", "password", "password", "a-perfectly-valid-password", "password_id", "my-password-id")
        );
        assertEquals(SnapshotEncryptedData.TYPE_PASSWORD, parsed.type());
        assertEquals("a-perfectly-valid-password", parsed.password().toString());
        assertEquals("my-password-id", parsed.passwordId());

        assertNull(SnapshotEncryptedData.fromMap(Map.of("type", "password", "password", "a-perfectly-valid-password")).passwordId());
    }

    public void testFromMapRejectsMalformedObjects() {
        assertParseError("not-an-object", "malformed encrypted_data, should be an object");
        assertParseError(Map.of("password", "a-perfectly-valid-password"), "encrypted_data.type is required");
        assertParseError(
            Map.of("type", "aws_kms", "password", "a-perfectly-valid-password"),
            "unknown encrypted_data.type [aws_kms], only [password] is supported"
        );
        assertParseError(Map.of("type", "password"), "encrypted_data.password is required");
        assertParseError(Map.of("type", "password", "password", 12345), "malformed encrypted_data.password, should be a string");
        assertParseError(
            Map.of("type", "password", "password", "a-perfectly-valid-password", "password_id", 12345),
            "malformed encrypted_data.password_id, should be a string"
        );
        assertParseError(
            Map.of("type", "password", "password", "a-perfectly-valid-password", "passwrd_id", "typo"),
            "unknown encrypted_data field [passwrd_id]"
        );
    }

    private static void assertParseError(Object value, String expectedMessage) {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> SnapshotEncryptedData.fromMap(value));
        assertThat(e.getMessage(), containsString(expectedMessage));
    }
}
