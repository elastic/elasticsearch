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
 * Tests for the {@link OperatorMetadata}, {@link OperatorErrorMetadata}, {@link OperatorHandlerMetadata} classes
 */
public class OperatorMetadataTests extends ESTestCase {

    public void testEquals() {
        final OperatorMetadata meta = createRandom();
        assertThat(meta, equalTo(OperatorMetadata.builder(meta.namespace(), meta).build()));
        final OperatorMetadata.Builder newMeta = OperatorMetadata.builder(meta.namespace(), meta);
        newMeta.putHandler(new OperatorHandlerMetadata("1", Collections.emptySet()));
        assertThat(newMeta.build(), not(meta));
    }

    public void testSerialization() throws IOException {
        final OperatorMetadata meta = createRandom();
        final BytesStreamOutput out = new BytesStreamOutput();
        meta.writeTo(out);
        assertThat(OperatorMetadata.readFrom(out.bytes().streamInput()), equalTo(meta));
    }

    public void testXContent() throws IOException {
        final OperatorMetadata meta = createRandom();
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        meta.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        parser.nextToken(); // the beginning of the object
        assertThat(OperatorMetadata.fromXContent(parser), equalTo(meta));
    }

    private static OperatorMetadata createRandom() {
        List<OperatorHandlerMetadata> handlers = randomList(
            0,
            10,
            () -> new OperatorHandlerMetadata.Builder(randomAlphaOfLength(5)).keys(randomSet(1, 5, () -> randomAlphaOfLength(6))).build()
        );

        List<OperatorErrorMetadata> errors = randomList(
            0,
            10,
            () -> new OperatorErrorMetadata.Builder().version(1L)
                .errorKind(randomFrom(OperatorErrorMetadata.ErrorKind.values()))
                .errors(randomList(1, 5, () -> randomAlphaOfLength(10)))
                .build()
        );

        OperatorMetadata.Builder builder = OperatorMetadata.builder(randomAlphaOfLength(7));

        for (var handlerMeta : handlers) {
            builder.putHandler(handlerMeta);
        }

        for (var errorMeta : errors) {
            builder.errorMetadata(errorMeta);
        }

        return builder.build();
    }

}
