/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.recycler.Recycler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public record ESONFlat(List<ESONEntry> keys, ESONSource.Values values, AtomicReference<BytesReference> serializedKeyBytes) {

    private static final byte[] EMPTY_KEY = new byte[0];

    public ESONFlat(List<ESONEntry> keys, ESONSource.Values values) {
        this(keys, values, new AtomicReference<>());
    }

    public ESONFlat(StreamInput in) throws IOException {
        this(readKeys(in), new ESONSource.Values(in.readBytesReference()), new AtomicReference<>());
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeBytesReference(getSerializedKeyBytes());
        out.writeBytesReference(values.data());
    }

    private static List<ESONEntry> readKeys(StreamInput in) throws IOException {
        // TODO: Because bytes reference
        in.readVInt();
        int expected = in.readVInt();
        ArrayList<ESONEntry> keys = new ArrayList<>(expected);
        for (int i = 0; i < expected; ++i) {
            int stringLength = in.readVInt();
            byte[] stringBytes = new byte[stringLength];
            in.readBytes(stringBytes, 0, stringLength);
            String key = stringBytes.length == 0 ? null : new String(stringBytes, StandardCharsets.UTF_8);
            byte type = in.readByte();
            int offsetOrCount = in.readInt();
            ESONEntry entry = switch (type) {
                case ESONEntry.TYPE_OBJECT -> new ESONEntry.ObjectEntry(key);
                case ESONEntry.TYPE_ARRAY -> new ESONEntry.ArrayEntry(key);
                default -> new ESONEntry.FieldEntry(key, type, offsetOrCount);
            };
            entry.offsetOrCount(offsetOrCount);
            keys.add(entry);
        }
        return keys;
    }

    public BytesReference getSerializedKeyBytes() {
        if (serializedKeyBytes.get() == null) {
            // TODO: Better estimate
            // for (ESONEntry entry : keys) {
            // String key = entry.key();
            // estimate += key == null ? 0 : key.length() + 5;
            // }
            try (RecyclerBytesStreamOutput streamOutput = new RecyclerBytesStreamOutput(getBytesRefRecycler())) {
                streamOutput.writeVInt(keys.size());
                for (ESONEntry entry : keys) {
                    String key = entry.key();
                    byte[] bytes = key == null ? EMPTY_KEY : key.getBytes(StandardCharsets.UTF_8);
                    streamOutput.writeVInt(bytes.length);
                    streamOutput.writeBytes(bytes, 0, bytes.length);
                    streamOutput.writeByte(entry.type());
                    // TODO: Combine
                    if (entry instanceof ESONEntry.FieldEntry fieldEntry) {
                        streamOutput.writeInt(fieldEntry.value.offset());
                    } else {
                        streamOutput.writeInt(entry.offsetOrCount());
                    }
                }
                BytesReference bytes = streamOutput.bytes();
                ByteArrayOutputStream os = new ByteArrayOutputStream(bytes.length());
                bytes.writeTo(os);
                serializedKeyBytes.set(bytes);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return serializedKeyBytes.get();
    }

    private static final ThreadLocal<BytesRef> BYTES_REF = ThreadLocal.withInitial(() -> new BytesRef(new byte[16384]));

    public static Recycler<BytesRef> getBytesRefRecycler() {
        return new Recycler<>() {

            private boolean first = true;

            @Override
            public V<BytesRef> obtain() {
                final BytesRef bytesRef;
                if (first) {
                    first = false;
                    bytesRef = BYTES_REF.get();
                } else {
                    bytesRef = new BytesRef(new byte[16384]);
                }
                return new V<>() {

                    @Override
                    public BytesRef v() {
                        return bytesRef;
                    }

                    @Override
                    public boolean isRecycled() {
                        return false;
                    }

                    @Override
                    public void close() {}
                };
            }

            @Override
            public int pageSize() {
                return 16384;
            }
        };
    }
}
