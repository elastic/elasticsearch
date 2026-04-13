/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

public class DataSourceStoredSettingTests extends ESTestCase {

    public void testWriteableRoundTripString() throws IOException {
        var setting = new DataSourceStoredSetting("my-value", false);
        var deserialized = writeableRoundTrip(setting);
        assertEquals(setting, deserialized);
        assertEquals("my-value", deserialized.value());
        assertFalse(deserialized.secret());
    }

    public void testWriteableRoundTripInteger() throws IOException {
        var setting = new DataSourceStoredSetting(42, true);
        var deserialized = writeableRoundTrip(setting);
        assertEquals(setting, deserialized);
        assertEquals(42, deserialized.value());
        assertTrue(deserialized.secret());
    }

    public void testWriteableRoundTripBoolean() throws IOException {
        var setting = new DataSourceStoredSetting(true, false);
        var deserialized = writeableRoundTrip(setting);
        assertEquals(true, deserialized.value());
    }

    public void testWriteableRoundTripNull() throws IOException {
        var setting = new DataSourceStoredSetting(null, false);
        var deserialized = writeableRoundTrip(setting);
        assertNull(deserialized.value());
    }

    public void testDecryptedValue() {
        var setting = new DataSourceStoredSetting("secret-key", true);
        assertEquals("secret-key", setting.decryptedValue());
    }

    public void testMaskedOrDecryptedValueSecret() {
        var setting = new DataSourceStoredSetting("secret-key", true);
        assertEquals("**********", setting.maskedOrDecryptedValue());
    }

    public void testMaskedOrDecryptedValuePlaintext() {
        var setting = new DataSourceStoredSetting("us-east-1", false);
        assertEquals("us-east-1", setting.maskedOrDecryptedValue());
    }

    public void testToStringMasksSecrets() {
        var secret = new DataSourceStoredSetting("password123", true);
        assertFalse(secret.toString().contains("password123"));
        assertTrue(secret.toString().contains("***"));

        var plaintext = new DataSourceStoredSetting("us-east-1", false);
        assertTrue(plaintext.toString().contains("us-east-1"));
    }

    public void testEqualsAndHashCode() {
        var a = new DataSourceStoredSetting("val", true);
        var b = new DataSourceStoredSetting("val", true);
        var c = new DataSourceStoredSetting("val", false);
        var d = new DataSourceStoredSetting("other", true);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
        assertNotEquals(a, d);
    }

    public void testWriteableRoundTripLong() throws IOException {
        var setting = new DataSourceStoredSetting(9_999_999_999L, false);
        var deserialized = writeableRoundTrip(setting);
        assertEquals(setting, deserialized);
        assertEquals(9_999_999_999L, deserialized.value());
    }

    public void testWriteableRoundTripDouble() throws IOException {
        var setting = new DataSourceStoredSetting(3.14159, false);
        var deserialized = writeableRoundTrip(setting);
        assertEquals(setting, deserialized);
        assertEquals(3.14159, (Double) deserialized.value(), 0.0);
    }

    public void testXContentRoundTrip() throws IOException {
        // Cover all JSON-native value types (String, Integer, Long, Double, Boolean, null) — the implementation relies
        // on JsonXContentParser.objectText() preserving the parsed type, so a refactor to .text() would silently
        // stringify non-string values without this coverage.
        assertXContentRoundTrip(new DataSourceStoredSetting("my-value", true));
        assertXContentRoundTrip(new DataSourceStoredSetting(42, false));
        assertXContentRoundTrip(new DataSourceStoredSetting(9_999_999_999L, false));
        assertXContentRoundTrip(new DataSourceStoredSetting(3.14159, false));
        assertXContentRoundTrip(new DataSourceStoredSetting(true, false));
        assertXContentRoundTrip(new DataSourceStoredSetting(null, false));
    }

    private void assertXContentRoundTrip(DataSourceStoredSetting setting) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        setting.toXContent(builder, null);

        XContentParser parser = createParser(JsonXContent.jsonXContent, org.elasticsearch.common.bytes.BytesReference.bytes(builder));
        var deserialized = DataSourceStoredSetting.fromXContent(parser);
        assertEquals(setting.value(), deserialized.value());
        assertEquals(setting.secret(), deserialized.secret());
    }

    private static DataSourceStoredSetting writeableRoundTrip(DataSourceStoredSetting setting) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        setting.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        return new DataSourceStoredSetting(in);
    }
}
