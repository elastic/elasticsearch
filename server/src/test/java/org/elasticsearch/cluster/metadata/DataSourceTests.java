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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

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

    private static Map<String, DataSourceSetting> randomSettings() {
        int count = randomIntBetween(0, 4);
        Map<String, DataSourceSetting> settings = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            boolean secret = randomBoolean();
            Object value = secret ? randomAlphaOfLength(8) : randomFrom(randomAlphaOfLength(8), randomInt(), randomBoolean());
            settings.put(randomAlphaOfLength(6).toLowerCase(Locale.ROOT), new DataSourceSetting(value, secret));
        }
        return settings;
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

    public void testToUnencryptedMap() {
        var dataSource = new DataSource(
            "my-s3",
            "s3",
            null,
            Map.of("access_key", new DataSourceSetting("AKIA123", true), "region", new DataSourceSetting("us-east-1", false))
        );

        Map<String, Object> plain = dataSource.toUnencryptedMap();
        assertEquals("AKIA123", plain.get("access_key"));
        assertEquals("us-east-1", plain.get("region"));
    }

    public void testToPresentationMap() {
        var dataSource = new DataSource(
            "my-s3",
            "s3",
            null,
            Map.of("access_key", new DataSourceSetting("AKIA123", true), "region", new DataSourceSetting("us-east-1", false))
        );

        Map<String, Object> masked = dataSource.toPresentationMap();
        assertEquals("::es_redacted::", masked.get("access_key"));
        assertEquals("us-east-1", masked.get("region"));
    }

    public void testToUnencryptedMapWithNullValues() {
        // Settings with value=null must round-trip through the map without NPE. HashMap (+ Collections.unmodifiableMap)
        // permit null values; Map.copyOf() does not, which is why the implementation uses the former.
        Map<String, DataSourceSetting> settings = new HashMap<>();
        settings.put("optional_secret", new DataSourceSetting(null, true));
        settings.put("optional_region", new DataSourceSetting(null, false));
        settings.put("present", new DataSourceSetting("us-east-1", false));
        var dataSource = new DataSource("my-s3", "s3", null, settings);

        Map<String, Object> plain = dataSource.toUnencryptedMap();
        assertTrue(plain.containsKey("optional_secret"));
        assertNull(plain.get("optional_secret"));
        assertTrue(plain.containsKey("optional_region"));
        assertNull(plain.get("optional_region"));
        assertEquals("us-east-1", plain.get("present"));
    }

    public void testToPresentationMapWithNullValues() {
        // Same null-safety property as toUnencryptedMap: the secret's masked sentinel applies to its classification,
        // not its value, so a null secret value still surfaces as null in the presentation map. A null non-secret
        // value also stays null.
        Map<String, DataSourceSetting> settings = new HashMap<>();
        settings.put("optional_secret", new DataSourceSetting(null, true));
        settings.put("optional_region", new DataSourceSetting(null, false));
        settings.put("present_secret", new DataSourceSetting("AKIA", true));
        var dataSource = new DataSource("my-s3", "s3", null, settings);

        Map<String, Object> masked = dataSource.toPresentationMap();
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
        expectThrows(NullPointerException.class, () -> new DataSource("test", "s3", null, null));
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
}
