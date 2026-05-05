/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

public class DataSourceSettingTests extends ESTestCase {

    public void testWriteableRoundTripString() throws IOException {
        var setting = new DataSourceSetting("my-value", false);
        var deserialized = writeableRoundTrip(setting);
        assertEquals(setting, deserialized);
        assertEquals("my-value", deserialized.nonSecretValue());
        assertFalse(deserialized.secret());
    }

    public void testWriteableRoundTripStringSecret() throws IOException {
        var setting = new DataSourceSetting("AKIA123", true);
        var deserialized = writeableRoundTrip(setting);
        assertEquals(setting, deserialized);
        try (var s = deserialized.secretValue()) {
            assertEquals("AKIA123", s.toString());
        }
        assertTrue(deserialized.secret());
    }

    public void testWriteableRoundTripInteger() throws IOException {
        var setting = new DataSourceSetting(42, false);
        var deserialized = writeableRoundTrip(setting);
        assertEquals(setting, deserialized);
        assertEquals(42, deserialized.nonSecretValue());
    }

    public void testWriteableRoundTripBoolean() throws IOException {
        var setting = new DataSourceSetting(true, false);
        var deserialized = writeableRoundTrip(setting);
        assertEquals(true, deserialized.nonSecretValue());
    }

    public void testWriteableRoundTripNull() throws IOException {
        var setting = new DataSourceSetting(null, false);
        var deserialized = writeableRoundTrip(setting);
        assertNull(deserialized.nonSecretValue());
    }

    public void testWriteableRoundTripLong() throws IOException {
        var setting = new DataSourceSetting(9_999_999_999L, false);
        var deserialized = writeableRoundTrip(setting);
        assertEquals(setting, deserialized);
        assertEquals(9_999_999_999L, deserialized.nonSecretValue());
    }

    public void testWriteableRoundTripDouble() throws IOException {
        var setting = new DataSourceSetting(3.14159, false);
        var deserialized = writeableRoundTrip(setting);
        assertEquals(setting, deserialized);
        assertEquals(3.14159, (Double) deserialized.nonSecretValue(), 0.0);
    }

    public void testNonSecretValueThrowsIfSecret() {
        var setting = new DataSourceSetting("secret-key", true);
        expectThrows(IllegalStateException.class, setting::nonSecretValue);
    }

    public void testPresentationValueSecret() {
        var setting = new DataSourceSetting("secret-key", true);
        assertEquals("::es_redacted::", setting.presentationValue());
    }

    public void testPresentationValuePlaintext() {
        var setting = new DataSourceSetting("us-east-1", false);
        assertEquals("us-east-1", setting.presentationValue());
    }

    public void testToStringMasksSecrets() {
        var secret = new DataSourceSetting("password123", true);
        assertFalse(secret.toString().contains("password123"));
        assertTrue(secret.toString().contains("::es_redacted::"));

        var plaintext = new DataSourceSetting("us-east-1", false);
        assertTrue(plaintext.toString().contains("us-east-1"));
    }

    public void testEqualsAndHashCode() {
        var a = new DataSourceSetting("val", true);
        var b = new DataSourceSetting("val", true);
        var c = new DataSourceSetting("val", false);
        var d = new DataSourceSetting("other", true);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
        assertNotEquals(a, d);
    }

    public void testSecretMustBeString() {
        // Per the class invariant, a secret setting must carry a String value (or null).
        // Numeric and boolean payloads aren't valid sensitive types in the domain; rejecting
        // them at construction prevents a later layer from accidentally producing one.
        var ex = expectThrows(IllegalArgumentException.class, () -> new DataSourceSetting(42, true));
        assertTrue(ex.getMessage().contains("must be String-valued"));
        expectThrows(IllegalArgumentException.class, () -> new DataSourceSetting(9_999_999_999L, true));
        expectThrows(IllegalArgumentException.class, () -> new DataSourceSetting(3.14159, true));
        expectThrows(IllegalArgumentException.class, () -> new DataSourceSetting(true, true));
    }

    public void testSecretMayBeNull() {
        // Null is the explicit "no value" state and is allowed even when secret=true.
        var setting = new DataSourceSetting(null, true);
        assertNull(setting.secretValue());
    }

    public void testSecretValueReturnsSecureString() {
        var setting = new DataSourceSetting("AKIA_THE_REAL_KEY", true);
        try (SecureString s = setting.secretValue()) {
            assertEquals("AKIA_THE_REAL_KEY", s.toString());
        }
    }

    public void testSecretValueWithNull() {
        var setting = new DataSourceSetting(null, true);
        assertNull(setting.secretValue());
    }

    public void testSecretValueThrowsIfNotSecret() {
        var setting = new DataSourceSetting("us-east-1", false);
        expectThrows(IllegalStateException.class, setting::secretValue);
    }

    public void testXContentRoundTrip() throws IOException {
        // Cover all JSON-native value types (String, Integer, Long, Double, Boolean, null) — the implementation relies
        // on JsonXContentParser.objectText() preserving the parsed type, so a refactor to .text() would silently
        // stringify non-string values without this coverage.
        assertXContentRoundTrip(new DataSourceSetting("my-value", true));
        assertXContentRoundTrip(new DataSourceSetting(42, false));
        assertXContentRoundTrip(new DataSourceSetting(9_999_999_999L, false));
        assertXContentRoundTrip(new DataSourceSetting(3.14159, false));
        assertXContentRoundTrip(new DataSourceSetting(true, false));
        assertXContentRoundTrip(new DataSourceSetting(null, false));
    }

    private void assertXContentRoundTrip(DataSourceSetting setting) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        setting.toXContent(builder, null);

        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        var deserialized = DataSourceSetting.fromXContent(parser);
        assertEquals(setting, deserialized);
    }

    private static DataSourceSetting writeableRoundTrip(DataSourceSetting setting) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        setting.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        return new DataSourceSetting(in);
    }
}
