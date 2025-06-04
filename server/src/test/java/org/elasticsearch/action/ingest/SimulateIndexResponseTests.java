/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RandomObjects;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class SimulateIndexResponseTests extends ESTestCase {

    public void testToXContent() throws IOException {
        String id = randomAlphaOfLength(10);
        String index = randomAlphaOfLength(5);
        long version = randomLongBetween(0, 500);
        final List<String> pipelines = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(0, 20); i++) {
            pipelines.add(randomAlphaOfLength(20));
        }
        String source = """
            {"doc": {"key1": "val1", "key2": "val2"}}""";
        BytesReference sourceBytes = BytesReference.fromByteBuffer(ByteBuffer.wrap(source.getBytes(StandardCharsets.UTF_8)));

        SimulateIndexResponse indexResponse = new SimulateIndexResponse(
            id,
            index,
            version,
            sourceBytes,
            XContentType.JSON,
            pipelines,
            List.of(),
            null
        );

        assertEquals(
            XContentHelper.stripWhitespace(
                Strings.format(
                    """
                        {
                          "_id": "%s",
                          "_index": "%s",
                          "_version": %d,
                          "_source": %s,
                          "executed_pipelines": [%s]
                        }""",
                    id,
                    index,
                    version,
                    source,
                    pipelines.stream().map(pipeline -> "\"" + pipeline + "\"").collect(Collectors.joining(","))
                )
            ),
            Strings.toString(indexResponse)
        );

        SimulateIndexResponse indexResponseWithException = new SimulateIndexResponse(
            id,
            index,
            version,
            sourceBytes,
            XContentType.JSON,
            pipelines,
            List.of(),
            new ElasticsearchException("Some failure")
        );

        assertEquals(
            XContentHelper.stripWhitespace(
                Strings.format(
                    """
                        {
                          "_id": "%s",
                          "_index": "%s",
                          "_version": %d,
                          "_source": %s,
                          "executed_pipelines": [%s],
                          "error":{"type":"exception","reason":"Some failure"}
                        }""",
                    id,
                    index,
                    version,
                    source,
                    pipelines.stream().map(pipeline -> "\"" + pipeline + "\"").collect(Collectors.joining(","))
                )
            ),
            Strings.toString(indexResponseWithException)
        );

        SimulateIndexResponse indexResponseWithIgnoredFields = new SimulateIndexResponse(
            id,
            index,
            version,
            sourceBytes,
            XContentType.JSON,
            pipelines,
            List.of("abc", "def"),
            null
        );

        assertEquals(
            XContentHelper.stripWhitespace(
                Strings.format(
                    """
                        {
                          "_id": "%s",
                          "_index": "%s",
                          "_version": %d,
                          "_source": %s,
                          "executed_pipelines": [%s],
                          "ignored_fields": [{"field": "abc"}, {"field": "def"}]
                        }""",
                    id,
                    index,
                    version,
                    source,
                    pipelines.stream().map(pipeline -> "\"" + pipeline + "\"").collect(Collectors.joining(","))
                )
            ),
            Strings.toString(indexResponseWithIgnoredFields)
        );
    }

    public void testSerialization() throws IOException {
        // Note: SimulateIndexRequest does not implement equals or hashCode, so we can't test serialization in the usual way for a Writable
        SimulateIndexResponse response = randomIndexResponse();
        IndexResponse copy = copyWriteable(response, null, SimulateIndexResponse::new);
        assertThat(Strings.toString(response), equalTo(Strings.toString(copy)));
    }

    /**
     * Returns a tuple of {@link IndexResponse}s.
     * <p>
     * The left element is the actual {@link IndexResponse} to serialize while the right element is the
     * expected {@link IndexResponse} after parsing.
     */
    private static SimulateIndexResponse randomIndexResponse() {
        String id = randomAlphaOfLength(10);
        String index = randomAlphaOfLength(5);
        long version = randomLongBetween(0, 500);
        final List<String> pipelines = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(0, 20); i++) {
            pipelines.add(randomAlphaOfLength(20));
        }
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference sourceBytes = RandomObjects.randomSource(random(), xContentType);
        return new SimulateIndexResponse(
            id,
            index,
            version,
            sourceBytes,
            xContentType,
            pipelines,
            randomList(0, 20, () -> randomAlphaOfLength(15)),
            randomBoolean() ? null : new ElasticsearchException("failed")
        );
    }
}
