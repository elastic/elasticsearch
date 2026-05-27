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
        PerColumnStatus original = new PerColumnStatus(
            randomFrom(PerColumnStatus.MATERIALIZATION_EAGER, PerColumnStatus.MATERIALIZATION_LATE, null)
        );
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        try (StreamInput in = out.bytes().streamInput()) {
            PerColumnStatus roundTripped = new PerColumnStatus(in);
            assertEquals(original, roundTripped);
        }
    }

    public void testWireRoundTripWithNullMaterialization() throws Exception {
        PerColumnStatus original = new PerColumnStatus((String) null);
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        try (StreamInput in = out.bytes().streamInput()) {
            PerColumnStatus roundTripped = new PerColumnStatus(in);
            assertEquals(original, roundTripped);
            assertNull(roundTripped.materialization());
        }
    }

    public void testToXContent() throws Exception {
        PerColumnStatus s = new PerColumnStatus(PerColumnStatus.MATERIALIZATION_LATE);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        s.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(BytesReference.bytes(builder).utf8ToString(), containsString("\"materialization\":\"late\""));
    }

    public void testToXContentOmitsNullMaterialization() throws Exception {
        PerColumnStatus s = new PerColumnStatus((String) null);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        s.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(BytesReference.bytes(builder).utf8ToString(), not(containsString("materialization")));
    }

    public void testMaterializationConstants() {
        assertEquals("eager", PerColumnStatus.MATERIALIZATION_EAGER);
        assertEquals("late", PerColumnStatus.MATERIALIZATION_LATE);
    }
}
