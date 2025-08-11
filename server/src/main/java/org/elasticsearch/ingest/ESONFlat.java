/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public record ESONFlat(List<ESONEntry> keys, ESONSource.Values values, AtomicReference<BytesReference> serializedKeyBytes) {

    public ESONFlat(List<ESONEntry> keys, ESONSource.Values values) {
        this(keys, values, new AtomicReference<>());
    }

    public ESONFlat(StreamInput in) throws IOException {
        this(readKeys(in), new ESONSource.Values(in.readBytesReference()), new AtomicReference<>());
    }

    private static List<ESONEntry> readKeys(StreamInput in) throws IOException {
        try (
            ReleasableBytesReference bytesReference = in.readReleasableBytesReference();
            StreamInput streamInput = bytesReference.streamInput()
        ) {
            int expected = streamInput.readVInt();
            ArrayList<ESONEntry> keys = new ArrayList<>(expected);
            for (int i = 0; i < expected; ++i) {
                // TODO: Use UTF-8 byte length eventually
                String key = streamInput.readString();
                byte type = streamInput.readByte();
                int offsetOrCount = streamInput.readInt();
                ESONEntry entry = switch (type) {
                    case ESONEntry.TYPE_OBJECT -> new ESONEntry.ObjectEntry(key.isEmpty() ? null : key);
                    case ESONEntry.TYPE_ARRAY -> new ESONEntry.ArrayEntry(key);
                    default -> new ESONEntry.FieldEntry(key, type, offsetOrCount);
                };
                entry.offsetOrCount(offsetOrCount);
                keys.add(entry);
            }
            return keys;
        }
    }

    public BytesReference getSerializedKeyBytes() {
        if (serializedKeyBytes.get() == null) {
            // TODO: Better estimate
            int estimate = 0;
            for (ESONEntry entry : keys) {
                String key = entry.key();
                estimate += key == null ? 0 : key.length() + 5;
            }
            try (BytesStreamOutput streamOutput = new BytesStreamOutput((int) (estimate * 1.1))) {
                streamOutput.writeVInt(keys.size());
                for (ESONEntry entry : keys) {
                    String key = entry.key();
                    // byte[] bytes = key == null ? EMPTY_KET : key.getBytes(StandardCharsets.UTF_8);
                    // streamOutput.writeVInt(bytes.length);
                    // streamOutput.writeBytes(bytes);
                    // TODO: Use UTF-8 byte length eventually
                    streamOutput.writeString(key == null ? "" : key);
                    streamOutput.writeByte(entry.type());
                    // TODO: Combine
                    if (entry instanceof ESONEntry.FieldEntry fieldEntry) {
                        streamOutput.writeInt(fieldEntry.value.offset());
                    } else {
                        streamOutput.writeInt(entry.offsetOrCount());
                    }
                }
                serializedKeyBytes.set(streamOutput.bytes());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return serializedKeyBytes.get();
    }
}
