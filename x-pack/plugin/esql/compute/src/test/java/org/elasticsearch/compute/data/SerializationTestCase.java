/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;

abstract class SerializationTestCase extends ESTestCase {

    final NamedWriteableRegistry registry = new NamedWriteableRegistry(Block.getNamedWriteables());

    Page serializeDeserializePage(Page origPage) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            origPage.writeTo(out);
            StreamInput in = new NamedWriteableAwareStreamInput(ByteBufferStreamInput.wrap(BytesReference.toBytes(out.bytes())), registry);
            return new Page(in);
        }
    }

    @SuppressWarnings("unchecked")
    <T extends Block> T serializeDeserializeBlock(T origBlock) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeNamedWriteable(origBlock);
            StreamInput in = new NamedWriteableAwareStreamInput(ByteBufferStreamInput.wrap(BytesReference.toBytes(out.bytes())), registry);
            return (T) in.readNamedWriteable(Block.class);
        }
    }

    <T extends Block> T uncheckedSerializeDeserializeBlock(T origBlock) {
        try {
            return serializeDeserializeBlock(origBlock);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
