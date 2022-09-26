/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Tests for the {@link ReservedStateMetadata}, {@link ReservedStateErrorMetadata}, {@link ReservedStateHandlerMetadata} classes
 */
public class ReservedStateMetadataTests extends ESTestCase {

    private void equalsTest(boolean addHandlers, boolean addErrors) {
        final ReservedStateMetadata meta = createRandom(addHandlers, addErrors);
        assertThat(meta, equalTo(ReservedStateMetadata.builder(meta.namespace(), meta).build()));
        final ReservedStateMetadata.Builder newMeta = ReservedStateMetadata.builder(meta.namespace(), meta);
        newMeta.putHandler(new ReservedStateHandlerMetadata("1", Collections.emptySet()));
        assertThat(newMeta.build(), not(meta));
    }

    public void testEquals() {
        equalsTest(true, true);
        equalsTest(true, false);
        equalsTest(false, true);
        equalsTest(false, false);
    }

    private void serializationTest(boolean addHandlers, boolean addErrors) throws IOException {
        final ReservedStateMetadata meta = createRandom(addHandlers, addErrors);
        final BytesStreamOutput out = new BytesStreamOutput();
        meta.writeTo(out);
        assertThat(ReservedStateMetadata.readFrom(out.bytes().streamInput()), equalTo(meta));
    }

    public void testSerialization() throws IOException {
        serializationTest(true, true);
        serializationTest(true, false);
        serializationTest(false, true);
        serializationTest(false, false);
    }

    private void xContentTest(boolean addHandlers, boolean addErrors) throws IOException {
        final ReservedStateMetadata meta = createRandom(addHandlers, addErrors);
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        meta.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        parser.nextToken(); // the beginning of the object
        assertThat(ReservedStateMetadata.fromXContent(parser), equalTo(meta));
    }

    public void testXContent() throws IOException {
        xContentTest(true, true);
        xContentTest(false, true);
        xContentTest(true, false);
        xContentTest(false, false);
    }

    public void testReservedStateVersionWithError() {
        final ReservedStateMetadata meta = createRandom(false, true);
        assertEquals(-1L, meta.version().longValue());
    }

    private static ReservedStateMetadata createRandom(boolean addHandlers, boolean addErrors) {
        List<ReservedStateHandlerMetadata> handlers = randomList(
            0,
            10,
            () -> new ReservedStateHandlerMetadata(randomAlphaOfLength(5), randomSet(1, 5, () -> randomAlphaOfLength(6)))
        );

        List<ReservedStateErrorMetadata> errors = randomList(
            0,
            10,
            () -> new ReservedStateErrorMetadata(
                1L,
                randomFrom(ReservedStateErrorMetadata.ErrorKind.values()),
                randomList(1, 5, () -> randomAlphaOfLength(10))
            )
        );

        ReservedStateMetadata.Builder builder = ReservedStateMetadata.builder(randomAlphaOfLength(7));
        if (addHandlers) {
            for (var handlerMeta : handlers) {
                builder.putHandler(handlerMeta);
            }
        }
        if (addErrors) {
            for (var errorMeta : errors) {
                builder.errorMetadata(errorMeta);
            }
        }

        return builder.build();
    }
}
