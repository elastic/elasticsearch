/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ChunkedRestResponseBodyPartTests extends ESTestCase {

    public void testEncodesChunkedXContentCorrectly() throws IOException {
        final ChunkedToXContent chunkedToXContent = (ToXContent.Params outerParams) -> Iterators.forArray(
            new ToXContent[] { (b, p) -> b.startObject(), (b, p) -> {
                if (randomBoolean()) {
                    b.flush();
                }
                b.mapContents(Map.of("foo", "bar", "some_other_key", "some_other_value"));
                return b;
            }, (b, p) -> b.stringListField("list_field", List.of("string", "otherString")).endObject() }
        );
        final XContent randomXContent = randomFrom(XContentType.values()).xContent();
        final XContentBuilder builderDirect = XContentBuilder.builder(randomXContent);
        var iter = chunkedToXContent.toXContentChunked(ToXContent.EMPTY_PARAMS);
        while (iter.hasNext()) {
            iter.next().toXContent(builderDirect, ToXContent.EMPTY_PARAMS);
        }
        final var bytesDirect = BytesReference.bytes(builderDirect);

        var firstBodyPart = ChunkedRestResponseBodyPart.fromXContent(
            chunkedToXContent,
            ToXContent.EMPTY_PARAMS,
            new FakeRestChannel(
                new FakeRestRequest.Builder(xContentRegistry()).withContent(BytesArray.EMPTY, randomXContent.type()).build(),
                randomBoolean(),
                1
            )
        );

        final List<BytesReference> refsGenerated = new ArrayList<>();
        while (firstBodyPart.isPartComplete() == false) {
            refsGenerated.add(firstBodyPart.encodeChunk(randomIntBetween(2, 10), BytesRefRecycler.NON_RECYCLING_INSTANCE));
        }
        assertTrue(firstBodyPart.isLastPart());

        assertEquals(bytesDirect, CompositeBytesReference.of(refsGenerated.toArray(new BytesReference[0])));
    }

    public void testFromTextChunks() throws IOException {
        final var chunks = randomList(1000, () -> randomUnicodeOfLengthBetween(1, 100));
        var firstBodyPart = ChunkedRestResponseBodyPart.fromTextChunks(
            "text/plain",
            Iterators.map(chunks.iterator(), s -> w -> w.write(s))
        );
        final List<BytesReference> refsGenerated = new ArrayList<>();
        while (firstBodyPart.isPartComplete() == false) {
            refsGenerated.add(firstBodyPart.encodeChunk(randomIntBetween(2, 10), BytesRefRecycler.NON_RECYCLING_INSTANCE));
        }
        assertTrue(firstBodyPart.isLastPart());
        final BytesReference chunkedBytes = CompositeBytesReference.of(refsGenerated.toArray(new BytesReference[0]));

        try (var outputStream = new ByteArrayOutputStream(); var writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)) {
            for (final var chunk : chunks) {
                writer.write(chunk);
            }
            writer.flush();
            assertEquals(new BytesArray(outputStream.toByteArray()), chunkedBytes);
        }
    }
}
