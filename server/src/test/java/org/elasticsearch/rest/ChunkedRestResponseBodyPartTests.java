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
import org.elasticsearch.core.Tuple;
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
import java.lang.ref.WeakReference;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.bytes.BytesReferenceTestUtils.equalBytes;
import static org.hamcrest.Matchers.equalTo;

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
                randomBoolean()
            )
        );

        final List<BytesReference> refsGenerated = new ArrayList<>();
        while (firstBodyPart.isPartComplete() == false) {
            refsGenerated.add(firstBodyPart.encodeChunk(randomIntBetween(2, 10), BytesRefRecycler.NON_RECYCLING_INSTANCE));
        }
        assertTrue(firstBodyPart.isLastPart());

        assertThat(CompositeBytesReference.of(refsGenerated.toArray(new BytesReference[0])), equalBytes(bytesDirect));
    }

    /** The body part lives until the response has been written, so it must not keep the {@link ChunkedToXContent} reachable. */
    public void testDoesNotRetainChunkedToXContent() throws Exception {
        final var channel = new FakeRestChannel(new FakeRestRequest.Builder(xContentRegistry()).build(), randomBoolean());
        final var bodyPartAndRef = bodyPartOfUnreferencedXContent(channel);
        final var bodyPart = bodyPartAndRef.v1();

        // Retry until the instance has been collected
        assertBusy(() -> {
            System.gc();
            assertNull(bodyPartAndRef.v2().get());
        });
        final List<BytesReference> refsGenerated = new ArrayList<>();
        while (bodyPart.isPartComplete() == false) {
            refsGenerated.add(bodyPart.encodeChunk(randomIntBetween(2, 10), BytesRefRecycler.NON_RECYCLING_INSTANCE));
        }
        assertThat(CompositeBytesReference.of(refsGenerated.toArray(new BytesReference[0])).utf8ToString(), equalTo("{}"));
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
            assertThat(chunkedBytes, equalBytes(new BytesArray(outputStream.toByteArray())));
        }
    }

    private static Tuple<ChunkedRestResponseBodyPart, WeakReference<ChunkedToXContent>> bodyPartOfUnreferencedXContent(RestChannel channel)
        throws IOException {
        final ChunkedToXContent chunkedToXContent = new EmptyObject();
        return Tuple.tuple(
            ChunkedRestResponseBodyPart.fromXContent(chunkedToXContent, ToXContent.EMPTY_PARAMS, channel),
            new WeakReference<>(chunkedToXContent)
        );
    }

    /** A class rather than a lambda, since a non-capturing lambda is a singleton and would never be collected. */
    private static final class EmptyObject implements ChunkedToXContent {
        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.single((b, p) -> b.startObject().endObject());
        }
    }
}
