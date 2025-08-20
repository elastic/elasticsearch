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
import org.elasticsearch.common.io.stream.BytesStreamOutput;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

public class ESONBytes {

    private final BytesReference keyBytes;
    private final BytesReference valueBytes;

    public ESONBytes(ESONFlat flat) {
        this.valueBytes = flat.values().data();
        try (BytesStreamOutput out = new BytesStreamOutput((int) (flat.values().data().length() * 0.5))) {
            for (ESONEntry entry : flat.getKeys()) {
                out.writeByte(entry.type());
                String key = entry.key();
                if (key != null) {
                    byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
                    out.writeVInt(bytes.length);
                    out.writeBytes(bytes);
                } else {
                    out.writeVInt(0);
                }
                int offsetOrCount = entry.offsetOrCount();
                out.writeInt(offsetOrCount);
            }
            keyBytes = out.bytes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
