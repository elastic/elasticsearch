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

import java.io.IOException;
import java.util.Map;

public class DatasourceTests extends ESTestCase {

    public void testWriteableRoundTrip() throws IOException {
        var datasource = new Datasource(
            "my-s3",
            "s3",
            "Production S3 bucket",
            Map.of("access_key", new DataSourceStoredSetting("AKIA123", true), "region", new DataSourceStoredSetting("us-east-1", false))
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

    public void testToPlainMap() {
        var datasource = new Datasource(
            "my-s3",
            "s3",
            null,
            Map.of("access_key", new DataSourceStoredSetting("AKIA123", true), "region", new DataSourceStoredSetting("us-east-1", false))
        );

        Map<String, Object> plain = datasource.toPlainMap();
        assertEquals("AKIA123", plain.get("access_key"));
        assertEquals("us-east-1", plain.get("region"));
    }

    public void testToMaskedMap() {
        var datasource = new Datasource(
            "my-s3",
            "s3",
            null,
            Map.of("access_key", new DataSourceStoredSetting("AKIA123", true), "region", new DataSourceStoredSetting("us-east-1", false))
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
}
