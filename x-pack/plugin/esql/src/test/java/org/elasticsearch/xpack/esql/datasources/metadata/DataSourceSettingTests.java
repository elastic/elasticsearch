/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.metadata;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xcontent.smile.SmileXContent;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;

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
        assertEquals("AKIA123", deserialized.rawValue());
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

    public void testPresentationValueWipedSecret() {
        var setting = new DataSourceSetting(null, true);
        assertNull(setting.presentationValue());
    }

    public void testWipedSecretRoundTrip() throws IOException {
        var setting = new DataSourceSetting(null, true);
        assertEquals(setting, writeableRoundTrip(setting));
        assertXContentRoundTrip(setting);
        assertSmileRoundTrip(setting);
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

    public void testSecretMayBeNull() {
        var setting = new DataSourceSetting(null, true);
        assertNull(setting.rawValue());
        assertFalse(setting.isEncrypted());
    }

    public void testEncryptedSecretIsDetectedAndEqualByContent() {
        var a = new DataSourceSetting(encryptedData("AKIA123"), true);
        var b = new DataSourceSetting(encryptedData("AKIA123"), true);
        assertTrue(a.isEncrypted());
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, new DataSourceSetting(encryptedData("different"), true));
    }

    public void testEncryptedSecretWriteableRoundTrip() throws IOException {
        EncryptedData carrier = encryptedData("AKIA_secret");
        var setting = new DataSourceSetting(carrier, true);
        var deserialized = writeableRoundTrip(setting);
        assertEquals(setting, deserialized);
        assertTrue(deserialized.isEncrypted());
        assertTrue(deserialized.secret());
        assertEquals(carrier, deserialized.rawValue());
    }

    public void testEncryptedSecretXContentRoundTrip() throws IOException {
        // The encrypted carrier is a nested {key_id, data} object; round-trips through both JSON and the
        // SMILE container that PersistedClusterStateService actually uses.
        assertXContentRoundTrip(new DataSourceSetting(encryptedData("AKIA_secret"), true));
        assertSmileRoundTrip(new DataSourceSetting(encryptedData("AKIA_secret"), true));
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

    public void testSmileRoundTripPreservesValueTypes() throws IOException {
        // Cluster-state persistence (PersistedClusterStateService) uses SMILE, so the GATEWAY path must
        // preserve non-secret value types across a restart — objectText() keeps the parsed type rather
        // than stringifying. Guards against a regression to text() that would silently coerce to String.
        assertSmileRoundTrip(new DataSourceSetting(42, false));
        assertSmileRoundTrip(new DataSourceSetting(9_999_999_999L, false));
        assertSmileRoundTrip(new DataSourceSetting(3.14159, false));
        assertSmileRoundTrip(new DataSourceSetting(true, false));
        assertSmileRoundTrip(new DataSourceSetting(null, false));
    }

    private void assertSmileRoundTrip(DataSourceSetting setting) throws IOException {
        XContentBuilder builder = SmileXContent.contentBuilder();
        setting.toXContent(builder, null);
        XContentParser parser = createParser(SmileXContent.smileXContent, BytesReference.bytes(builder));
        assertEquals(setting, DataSourceSetting.fromXContent(parser));
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

    private static EncryptedData encryptedData(String plaintext) {
        return new EncryptedData("test-key", plaintext.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
}
