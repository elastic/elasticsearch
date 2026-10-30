/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.eval;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class RankVectorsEvalSpecTests extends ESTestCase {

    public void testParseAndWireRoundTrip() throws Exception {
        try (
            XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "vectors")
                .startArray("query_vector")
                .startArray()
                .value(1.0)
                .value(0.0)
                .endArray()
                .startArray()
                .value(0.0)
                .value(1.0)
                .endArray()
                .endArray()
                .field("k", 2)
                .field("num_candidates", 20)
                .field("allow_exact", true)
                .endObject();
            XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))
        ) {
            RankVectorsEvalSpec parsed = RankVectorsEvalSpec.parse(parser);
            assertEquals("vectors", parsed.field());
            assertEquals(2, parsed.queryVector().length);
            assertEquals(parsed, copyWriteable(parsed, new NamedWriteableRegistry(List.of()), RankVectorsEvalSpec::new));
        }
    }

    public void testExactRequiresOptIn() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new RankVectorsEvalSpec("vectors", new float[][] { { 1f } }, 1, 1, false)
        );
        assertThat(exception.getMessage(), containsString("[allow_exact] must be true"));
    }

    public void testRequiresUnitQueryVectors() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new RankVectorsEvalSpec("vectors", new float[][] { { 1f, 1f } }, 1, 1, true)
        );
        assertThat(exception.getMessage(), containsString("must have unit magnitude"));

        exception = expectThrows(
            IllegalArgumentException.class,
            () -> new RankVectorsEvalSpec("vectors", new float[][] { { 1.00075f } }, 1, 1, true)
        );
        assertThat(exception.getMessage(), containsString("must have unit magnitude"));
    }

    public void testCaps() {
        expectThrows(IllegalArgumentException.class, () -> new RankVectorsEvalSpec("vectors", new float[][] { { 1f } }, 1001, 1001, true));
        expectThrows(IllegalArgumentException.class, () -> new RankVectorsEvalSpec("vectors", new float[][] { { 1f } }, 1, 10001, true));
        float[][] queryVectors = new float[RankVectorsEvalSpec.MAX_QUERY_VECTORS][];
        for (int i = 0; i < queryVectors.length; i++) {
            queryVectors[i] = new float[] { 1f };
        }
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new RankVectorsEvalSpec("vectors", queryVectors, 1, RankVectorsEvalSpec.MAX_NUM_CANDIDATES, true)
        );
        assertThat(exception.getMessage(), containsString("exact MaxSim work"));
        assertEquals(Long.MAX_VALUE, TransportRankVectorsEvalAction.saturatingMultiply(Long.MAX_VALUE, 2));
    }
}
