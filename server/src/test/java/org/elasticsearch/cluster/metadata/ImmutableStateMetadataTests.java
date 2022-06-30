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
 * Tests for the {@link ImmutableStateMetadata}, {@link ImmutableStateErrorMetadata}, {@link ImmutableStateHandlerMetadata} classes
 */
public class ImmutableStateMetadataTests extends ESTestCase {

    private void equalsTest(boolean addHandlers, boolean addErrors) {
        final ImmutableStateMetadata meta = createRandom(addHandlers, addErrors);
        assertThat(meta, equalTo(ImmutableStateMetadata.builder(meta.namespace(), meta).build()));
        final ImmutableStateMetadata.Builder newMeta = ImmutableStateMetadata.builder(meta.namespace(), meta);
        newMeta.putHandler(new ImmutableStateHandlerMetadata("1", Collections.emptySet()));
        assertThat(newMeta.build(), not(meta));
    }

    public void testEquals() {
        equalsTest(true, true);
        equalsTest(true, false);
        equalsTest(false, true);
        equalsTest(false, false);
    }

    private void serializationTest(boolean addHandlers, boolean addErrors) throws IOException {
        final ImmutableStateMetadata meta = createRandom(addHandlers, addErrors);
        final BytesStreamOutput out = new BytesStreamOutput();
        meta.writeTo(out);
        assertThat(ImmutableStateMetadata.readFrom(out.bytes().streamInput()), equalTo(meta));
    }

    public void testSerialization() throws IOException {
        serializationTest(true, true);
        serializationTest(true, false);
        serializationTest(false, true);
        serializationTest(false, false);
    }

    private void xContentTest(boolean addHandlers, boolean addErrors) throws IOException {
        final ImmutableStateMetadata meta = createRandom(addHandlers, addErrors);
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        meta.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        parser.nextToken(); // the beginning of the object
        assertThat(ImmutableStateMetadata.fromXContent(parser), equalTo(meta));
    }

    public void testXContent() throws IOException {
        xContentTest(true, true);
        xContentTest(false, true);
        xContentTest(true, false);
        xContentTest(false, false);
    }

    private static ImmutableStateMetadata createRandom(boolean addHandlers, boolean addErrors) {
        List<ImmutableStateHandlerMetadata> handlers = randomList(
            0,
            10,
            () -> new ImmutableStateHandlerMetadata(randomAlphaOfLength(5), randomSet(1, 5, () -> randomAlphaOfLength(6)))
        );

        List<ImmutableStateErrorMetadata> errors = randomList(
            0,
            10,
            () -> new ImmutableStateErrorMetadata(
                1L,
                randomFrom(ImmutableStateErrorMetadata.ErrorKind.values()),
                randomList(1, 5, () -> randomAlphaOfLength(10))
            )
        );

        ImmutableStateMetadata.Builder builder = ImmutableStateMetadata.builder(randomAlphaOfLength(7));
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
