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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xcontent.smile.SmileXContent;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class DataSourceTests extends AbstractXContentSerializingTestCase<DataSource> {

    @Override
    protected DataSource doParseInstance(XContentParser parser) throws IOException {
        return DataSource.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<DataSource> instanceReader() {
        return DataSource::new;
    }

    @Override
    protected DataSource createTestInstance() {
        return randomDataSource();
    }

    @Override
    protected DataSource mutateInstance(DataSource instance) {
        return switch (randomIntBetween(0, 3)) {
            case 0 -> new DataSource(
                randomValueOtherThan(instance.name(), () -> randomAlphaOfLength(8).toLowerCase(Locale.ROOT)),
                instance.type(),
                instance.description(),
                instance.settings()
            );
            case 1 -> new DataSource(
                instance.name(),
                randomValueOtherThan(instance.type(), () -> randomFrom("s3", "gcs", "azure")),
                instance.description(),
                instance.settings()
            );
            case 2 -> new DataSource(
                instance.name(),
                instance.type(),
                randomValueOtherThan(instance.description(), () -> randomAlphaOfLengthBetween(1, 16)),
                instance.settings()
            );
            default -> new DataSource(
                instance.name(),
                instance.type(),
                instance.description(),
                randomValueOtherThan(instance.settings(), DataSourceTests::randomSettings)
            );
        };
    }

    private static DataSource randomDataSource() {
        return new DataSource(
            randomAlphaOfLength(8).toLowerCase(Locale.ROOT),
            randomFrom("s3", "gcs", "azure"),
            randomBoolean() ? null : randomAlphaOfLengthBetween(0, 32),
            randomSettings()
        );
    }

    private static DataSourceSettings randomSettings() {
        int count = randomIntBetween(0, 4);
        Map<String, DataSourceSetting> settings = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            boolean secret = randomBoolean();
            Object value = secret ? randomAlphaOfLength(8) : randomFrom(randomAlphaOfLength(8), randomInt(), randomBoolean());
            settings.put(randomAlphaOfLength(6).toLowerCase(Locale.ROOT), new DataSourceSetting(value, secret));
        }
        return new DataSourceSettings(settings);
    }

    public void testWriteableRoundTripExplicit() throws IOException {
        var dataSource = new DataSource(
            "my-s3",
            "s3",
            "Production S3 bucket",
            Map.of("access_key", new DataSourceSetting("AKIA123", true), "region", new DataSourceSetting("us-east-1", false))
        );

        BytesStreamOutput out = new BytesStreamOutput();
        dataSource.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        var deserialized = new DataSource(in);

        assertEquals(dataSource, deserialized);
        assertEquals("my-s3", deserialized.name());
        assertEquals("s3", deserialized.type());
        assertEquals("Production S3 bucket", deserialized.description());
        assertEquals(2, deserialized.settings().size());
    }

    public void testToPresentationMap() {
        var dataSource = new DataSource(
            "my-s3",
            "s3",
            null,
            Map.of("access_key", new DataSourceSetting("AKIA123", true), "region", new DataSourceSetting("us-east-1", false))
        );

        Map<String, Object> masked = dataSource.settings().toPresentationMap();
        assertEquals("::es_redacted::", masked.get("access_key"));
        assertEquals("us-east-1", masked.get("region"));
    }

    public void testToPresentationMapWithNullValues() {
        // The secret's masked sentinel applies to its classification, not its value, so a null secret value still
        // surfaces as null in the presentation map. A null non-secret value also stays null.
        Map<String, DataSourceSetting> settings = new HashMap<>();
        settings.put("optional_secret", new DataSourceSetting(null, true));
        settings.put("optional_region", new DataSourceSetting(null, false));
        settings.put("present_secret", new DataSourceSetting("AKIA", true));
        var dataSource = new DataSource("my-s3", "s3", null, settings);

        Map<String, Object> masked = dataSource.settings().toPresentationMap();
        assertTrue(masked.containsKey("optional_secret"));
        assertEquals("::es_redacted::", masked.get("optional_secret"));
        assertTrue(masked.containsKey("optional_region"));
        assertNull(masked.get("optional_region"));
        assertEquals("::es_redacted::", masked.get("present_secret"));
    }

    public void testNullDescription() throws IOException {
        var dataSource = new DataSource("test", "s3", null, Map.of());
        BytesStreamOutput out = new BytesStreamOutput();
        dataSource.writeTo(out);
        var deserialized = new DataSource(out.bytes().streamInput());
        assertNull(deserialized.description());
    }

    public void testEmptySettings() throws IOException {
        var dataSource = new DataSource("test", "s3", null, Map.of());
        BytesStreamOutput out = new BytesStreamOutput();
        dataSource.writeTo(out);
        var deserialized = new DataSource(out.bytes().streamInput());
        assertTrue(deserialized.settings().isEmpty());
    }

    public void testRequiresName() {
        expectThrows(NullPointerException.class, () -> new DataSource(null, "s3", null, Map.of()));
    }

    public void testRequiresType() {
        expectThrows(NullPointerException.class, () -> new DataSource("test", null, null, Map.of()));
    }

    public void testRequiresSettings() {
        expectThrows(NullPointerException.class, () -> new DataSource("test", "s3", null, (Map<String, DataSourceSetting>) null));
    }

    public void testToStringMasksSecretsAndIncludesPlaintext() {
        // toString() routes through toPresentationMap() so it can include settings without leaking raw secret values.
        var dataSource = new DataSource(
            "my-s3",
            "s3",
            "Production S3 bucket",
            Map.of(
                "access_key",
                new DataSourceSetting("AKIA_ABSOLUTELY_SECRET", true),
                "secret_key",
                new DataSourceSetting("wJal_VERY_PRIVATE", true),
                "region",
                new DataSourceSetting("us-east-1", false)
            )
        );
        String summary = dataSource.toString();
        assertFalse("raw access_key leaked in toString: " + summary, summary.contains("AKIA_ABSOLUTELY_SECRET"));
        assertFalse("raw secret_key leaked in toString: " + summary, summary.contains("wJal_VERY_PRIVATE"));
        assertTrue("masked sentinel not present in toString: " + summary, summary.contains("::es_redacted::"));
        assertTrue("non-secret value missing from toString: " + summary, summary.contains("us-east-1"));
    }

    public void testXContentRoundTripHeterogeneousSettings() throws IOException {
        // Exercises all JSON-native value types inside the settings map (String, Integer, Long, Double, Boolean, null)
        // to verify both the per-setting XContent contract and the containing DataSource's map serialization.
        // Secrets must be String-valued (invariant), so the non-String cases are non-secret.
        Map<String, DataSourceSetting> settings = new HashMap<>();
        settings.put("access_key", new DataSourceSetting("AKIA123", true));
        settings.put("region", new DataSourceSetting("us-east-1", false));
        settings.put("max_retries", new DataSourceSetting(7, false));
        settings.put("max_attempts", new DataSourceSetting(9_999_999_999L, false));
        settings.put("backoff_multiplier", new DataSourceSetting(1.5, false));
        settings.put("use_path_style", new DataSourceSetting(true, false));
        settings.put("optional_label", new DataSourceSetting(null, false));
        var dataSource = new DataSource("my-s3", "s3", "Production S3 bucket", settings);
        assertExplicitXContentRoundTrip(dataSource);
    }

    public void testXContentRoundTripNoDescription() throws IOException {
        var dataSource = new DataSource("my-s3", "s3", null, Map.of("region", new DataSourceSetting("us-east-1", false)));
        assertExplicitXContentRoundTrip(dataSource);
    }

    public void testXContentRoundTripEmptySettings() throws IOException {
        var dataSource = new DataSource("my-s3", "s3", "desc", Map.of());
        assertExplicitXContentRoundTrip(dataSource);
    }

    private void assertExplicitXContentRoundTrip(DataSource dataSource) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        dataSource.toXContent(builder, null);

        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        DataSource deserialized = DataSource.fromXContent(parser);
        assertEquals(dataSource, deserialized);
    }

    public void testWriteableRoundTripWithEncryptedSetting() throws IOException {
        // Pins the full DataSource envelope's wire shape with an encrypted secret inside. Setting-level
        // round-trip is covered by DataSourceSettingTests; this guards the DataSource->DataSourceSettings
        // delegation so a regression in the envelope's writeTo doesn't only show up under integ tests.
        EncryptedData carrier = new EncryptedData("test-key", new byte[] { 9, 8, 7, 6, 5, 4, 3, 2, 1 });
        Map<String, DataSourceSetting> settings = new HashMap<>();
        settings.put("access_key", new DataSourceSetting(carrier, true));
        settings.put("region", new DataSourceSetting("us-east-1", false));
        var dataSource = new DataSource("my-s3", "s3", null, settings);

        BytesStreamOutput out = new BytesStreamOutput();
        dataSource.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        var deserialized = new DataSource(in);

        assertEquals(dataSource, deserialized);
        DataSourceSetting roundTrippedAccessKey = deserialized.settings().get("access_key");
        assertTrue(roundTrippedAccessKey.isEncrypted());
        assertEquals(carrier, roundTrippedAccessKey.rawValue());
    }

    public void testSmileRoundTripWithEncryptedSetting() throws IOException {
        // Cluster-state persistence goes through SMILE (PersistedClusterStateService). Pins the
        // realistic gateway-restart shape: an encrypted secret (a nested EncryptedData object) must
        // survive SMILE write+read with its key id and payload intact.
        EncryptedData carrier = new EncryptedData("test-key", new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });
        Map<String, DataSourceSetting> settings = new HashMap<>();
        settings.put("access_key", new DataSourceSetting(carrier, true));
        settings.put("region", new DataSourceSetting("us-east-1", false));
        var dataSource = new DataSource("my-s3", "s3", "smile-roundtrip", settings);

        XContentBuilder builder = SmileXContent.contentBuilder();
        dataSource.toXContent(builder, null);
        XContentParser parser = createParser(XContentType.SMILE.xContent(), BytesReference.bytes(builder));
        DataSource deserialized = DataSource.fromXContent(parser);

        assertEquals(dataSource, deserialized);
        DataSourceSetting roundTrippedAccessKey = deserialized.settings().get("access_key");
        assertTrue(roundTrippedAccessKey.isEncrypted());
        assertEquals(carrier, roundTrippedAccessKey.rawValue());
    }
}
