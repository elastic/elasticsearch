/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.query;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.rank.vectors.mapper.RankVectorsFieldMapper;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class RankVectorsQueryBuilderTests extends ESTestCase {

    public void testXContentParsing() throws IOException {
        String json = """
            {
              "field": "tokens",
              "query_vector": [[1.0, 0.0], [0.0, 1.0]],
              "k": 2,
              "num_candidates": 10
            }
            """;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            RankVectorsQueryBuilder builder = RankVectorsQueryBuilder.fromXContent(parser);
            assertEquals("tokens", builder.getFieldName());
            assertArrayEquals(new float[] { 1f, 0f }, builder.getQueryVectors()[0], 0f);
            assertArrayEquals(new float[] { 0f, 1f }, builder.getQueryVectors()[1], 0f);
            assertEquals(2, builder.getK());
            assertEquals(10, builder.getNumCandidates());
        }
    }

    public void testWireRoundTrip() throws IOException {
        RankVectorsQueryBuilder builder = new RankVectorsQueryBuilder("tokens", new float[][] { { 1f, 0f }, { 0f, 1f } }, 2, 10).boost(1.5f)
            .queryName("late-interaction");
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setTransportVersion(TransportVersion.current());
            builder.writeTo(output);
            try (
                StreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), new NamedWriteableRegistry(List.of()))
            ) {
                input.setTransportVersion(TransportVersion.current());
                assertEquals(builder, new RankVectorsQueryBuilder(input));
            }
        }
    }

    public void testDefensiveCopy() {
        float[][] queryVectors = { { 1f, 0f } };
        RankVectorsQueryBuilder builder = new RankVectorsQueryBuilder("tokens", queryVectors, 1, 10);
        queryVectors[0][0] = 0f;
        assertEquals(1f, builder.getQueryVectors()[0][0], 0f);
        float[][] returned = builder.getQueryVectors();
        returned[0][0] = 0f;
        assertEquals(1f, builder.getQueryVectors()[0][0], 0f);
    }

    public void testValidation() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new RankVectorsQueryBuilder("tokens", new float[0][], 1, 10)
        );
        assertThat(e.getMessage(), containsString("must not be empty"));

        e = expectThrows(IllegalArgumentException.class, () -> new RankVectorsQueryBuilder("tokens", new float[][] { { 1f } }, 0, 10));
        assertThat(e.getMessage(), containsString("[k] must be greater than 0"));

        e = expectThrows(IllegalArgumentException.class, () -> new RankVectorsQueryBuilder("tokens", new float[][] { { 1f } }, 2, 1));
        assertThat(e.getMessage(), containsString("[num_candidates] must be greater than or equal to [k]"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> new RankVectorsQueryBuilder("tokens", new float[][] { { Float.NaN } }, 1, 1)
        );
        assertThat(e.getMessage(), containsString("non-finite"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> new RankVectorsQueryBuilder("tokens", new float[RankVectorsFieldMapper.MAX_MUVERA_VECTORS + 1][1], 1, 1)
        );
        assertThat(e.getMessage(), containsString("vector count"));

        assertNotNull(new RankVectorsQueryBuilder("tokens", new float[92][1], 10, 2_000));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> new RankVectorsQueryBuilder("tokens", new float[RankVectorsFieldMapper.MAX_MUVERA_VECTORS][1], 1, 10_000)
        );
        assertThat(e.getMessage(), containsString("exact MaxSim work"));
    }
}
