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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;

public class DatasourceTests extends ESTestCase {

    public void testWriteableRoundTrip() throws IOException {
        var datasource = new Datasource(
            "my-s3",
            "s3",
            "Production S3 bucket",
            Map.of("access_key", new DataSourceSetting("AKIA123", true), "region", new DataSourceSetting("us-east-1", false))
        );

        BytesStreamOutput out = new BytesStreamOutput();
        datasource.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        var deserialized = new Datasource(in);

        assertEquals(datasource, deserialized);
        assertEquals("my-s3", deserialized.name());
        assertEquals("s3", deserialized.type());
        assertEquals("Production S3 bucket", deserialized.description());
        assertEquals(2, deserialized.settings().size());
    }

    public void testToUnencryptedMap() {
        var datasource = new Datasource(
            "my-s3",
            "s3",
            null,
            Map.of("access_key", new DataSourceSetting("AKIA123", true), "region", new DataSourceSetting("us-east-1", false))
        );

        Map<String, Object> plain = datasource.toUnencryptedMap();
        assertEquals("AKIA123", plain.get("access_key"));
        assertEquals("us-east-1", plain.get("region"));
    }

    public void testToMaskedMap() {
        var datasource = new Datasource(
            "my-s3",
            "s3",
            null,
            Map.of("access_key", new DataSourceSetting("AKIA123", true), "region", new DataSourceSetting("us-east-1", false))
        );

        Map<String, Object> masked = datasource.toMaskedMap();
        assertEquals("**********", masked.get("access_key"));
        assertEquals("us-east-1", masked.get("region"));
    }

    public void testNullDescription() throws IOException {
        var datasource = new Datasource("test", "s3", null, Map.of());
        BytesStreamOutput out = new BytesStreamOutput();
        datasource.writeTo(out);
        var deserialized = new Datasource(out.bytes().streamInput());
        assertNull(deserialized.description());
    }

    public void testEmptySettings() throws IOException {
        var datasource = new Datasource("test", "s3", null, Map.of());
        BytesStreamOutput out = new BytesStreamOutput();
        datasource.writeTo(out);
        var deserialized = new Datasource(out.bytes().streamInput());
        assertTrue(deserialized.settings().isEmpty());
    }

    public void testRequiresName() {
        expectThrows(NullPointerException.class, () -> new Datasource(null, "s3", null, Map.of()));
    }

    public void testRequiresType() {
        expectThrows(NullPointerException.class, () -> new Datasource("test", null, null, Map.of()));
    }

    public void testRequiresSettings() {
        expectThrows(NullPointerException.class, () -> new Datasource("test", "s3", null, null));
    }

    public void testToStringMasksSecretsAndIncludesPlaintext() {
        // toString() routes through toMaskedMap() so it can include settings without leaking raw secret values.
        var datasource = new Datasource(
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
        String summary = datasource.toString();
        assertFalse("raw access_key leaked in toString: " + summary, summary.contains("AKIA_ABSOLUTELY_SECRET"));
        assertFalse("raw secret_key leaked in toString: " + summary, summary.contains("wJal_VERY_PRIVATE"));
        assertTrue("masked sentinel not present in toString: " + summary, summary.contains("**********"));
        assertTrue("non-secret value missing from toString: " + summary, summary.contains("us-east-1"));
    }

    public void testXContentRoundTripHeterogeneousSettings() throws IOException {
        // Exercises all JSON-native value types inside the settings map (String, Integer, Boolean)
        // to verify both the per-setting XContent contract and the containing Datasource's map serialization.
        var datasource = new Datasource(
            "my-s3",
            "s3",
            "Production S3 bucket",
            Map.of(
                "access_key",
                new DataSourceSetting("AKIA123", true),
                "region",
                new DataSourceSetting("us-east-1", false),
                "max_retries",
                new DataSourceSetting(7, false),
                "use_path_style",
                new DataSourceSetting(true, false)
            )
        );
        assertXContentRoundTrip(datasource);
    }

    public void testXContentRoundTripNoDescription() throws IOException {
        var datasource = new Datasource("my-s3", "s3", null, Map.of("region", new DataSourceSetting("us-east-1", false)));
        assertXContentRoundTrip(datasource);
    }

    public void testXContentRoundTripEmptySettings() throws IOException {
        var datasource = new Datasource("my-s3", "s3", "desc", Map.of());
        assertXContentRoundTrip(datasource);
    }

    private void assertXContentRoundTrip(Datasource datasource) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        datasource.toXContent(builder, null);

        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        Datasource deserialized = Datasource.fromXContent(parser);
        assertEquals(datasource, deserialized);
    }
}
