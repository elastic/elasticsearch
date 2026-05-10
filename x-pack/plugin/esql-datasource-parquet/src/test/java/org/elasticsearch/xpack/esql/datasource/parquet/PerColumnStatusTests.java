/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class PerColumnStatusTests extends ESTestCase {

    public void testWireRoundTrip() throws Exception {
        PerColumnStatus original = randomStatus();
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        try (StreamInput in = out.bytes().streamInput()) {
            PerColumnStatus roundTripped = new PerColumnStatus(in);
            assertEquals(original, roundTripped);
        }
    }

    public void testWireRoundTripWithNullMaterialization() throws Exception {
        PerColumnStatus original = new PerColumnStatus(10L, 20L, 30L, 40L, 5L, null);
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        try (StreamInput in = out.bytes().streamInput()) {
            PerColumnStatus roundTripped = new PerColumnStatus(in);
            assertEquals(original, roundTripped);
            assertNull(roundTripped.materialization());
        }
    }

    public void testToXContentSnakeCaseKeys() throws Exception {
        PerColumnStatus s = new PerColumnStatus(11L, 22L, 33L, 44L, 5L, PerColumnStatus.MATERIALIZATION_LATE);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        s.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = BytesReference.bytes(builder).utf8ToString();

        assertThat(json, containsString("\"compressed_bytes\":11"));
        assertThat(json, containsString("\"decompressed_bytes\":22"));
        assertThat(json, containsString("\"decompression_nanos\":33"));
        assertThat(json, containsString("\"decode_nanos\":44"));
        assertThat(json, containsString("\"data_pages_read\":5"));
        assertThat(json, containsString("\"materialization\":\"late\""));
    }

    public void testToXContentOmitsNullMaterialization() throws Exception {
        PerColumnStatus s = new PerColumnStatus(0L, 0L, 0L, 0L, 0L, null);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        s.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = BytesReference.bytes(builder).utf8ToString();
        assertThat(json, not(containsString("materialization")));
    }

    public void testNegativeAssertions() {
        AssertionError e = expectThrows(
            AssertionError.class,
            () -> new PerColumnStatus(-1L, 0L, 0L, 0L, 0L, PerColumnStatus.MATERIALIZATION_EAGER)
        );
        assertThat(e.getMessage(), containsString("non-negative"));
    }

    public void testMaterializationConstants() {
        assertEquals("eager", PerColumnStatus.MATERIALIZATION_EAGER);
        assertEquals("late", PerColumnStatus.MATERIALIZATION_LATE);
    }

    private PerColumnStatus randomStatus() {
        String mat = randomFrom(PerColumnStatus.MATERIALIZATION_EAGER, PerColumnStatus.MATERIALIZATION_LATE, null);
        return new PerColumnStatus(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            mat
        );
    }
}
