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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.recycler.Recycler;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public record ESONFlat(
    AtomicReference<List<ESONEntry>> keys,
    ESONSource.Values values,
    AtomicReference<BytesReference> serializedKeyBytes,
    AtomicReference<HashMap<String, byte[]>> sharedKeyBytes
) {

    public ESONFlat(List<ESONEntry> keys, ESONSource.Values values) {
        this(new AtomicReference<>(keys), values, new AtomicReference<>(), new AtomicReference<>());
    }

    public static ESONFlat readFrom(StreamInput in) throws IOException {
        BytesReference keys = in.readBytesReference();
        // TODO: Find way to share
        return new ESONFlat(
            new AtomicReference<>(),
            new ESONSource.Values(in.readBytesReference()),
            new AtomicReference<>(keys),
            new AtomicReference<>()
        );
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeBytesReference(getSerializedKeyBytes());
        out.writeBytesReference(values.data());
    }

    public List<ESONEntry> getKeys() {
        List<ESONEntry> esonEntries = keys.get();
        if (esonEntries == null) {
            try {
                keys.set(readKeys(serializedKeyBytes.get().streamInput()));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return keys.get();
    }

    private static List<ESONEntry> readKeys(StreamInput in) throws IOException {
        int expected = in.readVInt();
        ESONStack esonStack = new ESONStack();
        ArrayList<ESONEntry> keys = new ArrayList<>(expected);
        byte startType = in.readByte();
        assert startType == ESONEntry.TYPE_OBJECT;
        int count = in.readInt();
        keys.add(new ESONEntry.ObjectEntry(null, count));
        esonStack.pushObject(count);
        for (int i = 1; i < expected; ++i) {
            int stackValue = esonStack.currentStackValue();
            final String key;
            if (ESONStack.isObject(stackValue)) {
                int stringLength = in.readVInt();
                byte[] stringBytes = new byte[stringLength];
                in.readBytes(stringBytes, 0, stringLength);
                key = new String(stringBytes, StandardCharsets.UTF_8);
            } else {
                key = null;
            }
            byte type = in.readByte();
            int offsetOrCount;
            if (type == ESONEntry.TYPE_NULL || type == ESONEntry.TYPE_TRUE || type == ESONEntry.TYPE_FALSE) {
                offsetOrCount = -1;
            } else {
                offsetOrCount = in.readInt();
            }
            esonStack.updateRemainingFields(stackValue - 1);
            ESONEntry entry = switch (type) {
                case ESONEntry.TYPE_OBJECT -> {
                    esonStack.pushObject(offsetOrCount);
                    yield new ESONEntry.ObjectEntry(key, offsetOrCount);
                }
                case ESONEntry.TYPE_ARRAY -> {
                    esonStack.pushArray(offsetOrCount);
                    yield new ESONEntry.ArrayEntry(key, offsetOrCount);
                }
                default -> new ESONEntry.FieldEntry(key, type, offsetOrCount);
            };
            keys.add(entry);
            while (esonStack.isEmpty() == false && ESONStack.fieldsRemaining(esonStack.currentStackValue()) == 0) {
                esonStack.popContainer();
            }
        }
        return keys;
    }

    public BytesReference getSerializedKeyBytes() {
        if (serializedKeyBytes.get() == null) {
            HashMap<String, byte[]> sharedKeyBytesMap = sharedKeyBytes.get();
            assert keys.get() != null;
            // TODO: Better estimate
            // for (ESONEntry entry : keys) {
            // String key = entry.key();
            // estimate += key == null ? 0 : key.length() + 5;
            // }
            try (RecyclerBytesStreamOutput streamOutput = new RecyclerBytesStreamOutput(getBytesRefRecycler())) {
                List<ESONEntry> esonEntries = keys.get();
                streamOutput.writeVInt(esonEntries.size());
                for (ESONEntry entry : esonEntries) {
                    String key = entry.key();
                    if (key != null) {
                        byte[] bytes = sharedKeyBytesMap == null
                            ? key.getBytes(StandardCharsets.UTF_8)
                            : sharedKeyBytesMap.computeIfAbsent(key, k -> key.getBytes(StandardCharsets.UTF_8));
                        streamOutput.writeVInt(bytes.length);
                        streamOutput.writeBytes(bytes, 0, bytes.length);
                        // streamOutput.writeUTF8String(key);
                    }
                    byte type = entry.type();
                    streamOutput.writeByte(type);
                    if (type <= ESONEntry.TYPE_TRUE) {
                        streamOutput.writeInt(entry.offsetOrCount());
                    }
                }
                BytesReference bytes = streamOutput.bytes();
                final BytesRef bytesRef;
                if (bytes.hasArray()) {
                    bytesRef = BytesRef.deepCopyOf(bytes.toBytesRef());
                } else {
                    bytesRef = bytes.toBytesRef();
                }
                serializedKeyBytes.set(new BytesArray(bytesRef));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        // Only called on translog or writing out. Either case, we no longer need the list.
        keys.set(null);
        return serializedKeyBytes.get();
    }

    private static final ThreadLocal<BytesRef> BYTES_REF = ThreadLocal.withInitial(() -> new BytesRef(new byte[16384]));

    public static Recycler<BytesRef> getBytesRefRecycler() {
        return new ThreadLocalRecycler();
    }

    private static class ThreadLocalRecycler implements Recycler<BytesRef> {

        private boolean first = true;

        @Override
        public V<BytesRef> obtain() {
            final BytesRef bytesRef;
            if (first) {
                first = false;
                bytesRef = BYTES_REF.get();
                bytesRef.offset = 0;
                bytesRef.length = bytesRef.bytes.length;
            } else {
                bytesRef = new BytesRef(new byte[16384]);
            }
            return new VImpl(bytesRef);
        }

        private record VImpl(BytesRef bytesRef) implements V<BytesRef> {

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
        }

        @Override
        public int pageSize() {
            return 16384;
        }
    }
}
