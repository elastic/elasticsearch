/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.core.type.NestedContainerField;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Stores the child documents of one parent, a page with one position per child and one block per child field, as the single
 * value of a container column. The page uses the blocks' own wire format, so every type and multivalue a block holds survives.
 * Producer and consumer can be nodes of different versions, so both read and write it at {@link NestedContainerField#VERSION}.
 */
public final class NestedContainerCodec {
    private NestedContainerCodec() {}

    public static BytesRef encode(Page children) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(NestedContainerField.VERSION);
            children.writeTo(out);
            return new BytesRef(BytesReference.toBytes(out.bytes()));
        }
    }

    public static Page decode(BlockFactory blockFactory, BytesRef bytes) throws IOException {
        try (
            BlockStreamInput in = new BlockStreamInput(
                new ByteBufferStreamInput(ByteBuffer.wrap(bytes.bytes, bytes.offset, bytes.length)),
                blockFactory
            )
        ) {
            in.setTransportVersion(NestedContainerField.VERSION);
            return new Page(in);
        }
    }
}
