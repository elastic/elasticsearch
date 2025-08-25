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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

public class ESONBytesXContentParser extends ESONXContentParser {

    private final StreamInput streamInput;
    private boolean readOpenObject = false;

    public ESONBytesXContentParser(
        BytesReference keyBytes,
        ESONSource.Values values,
        NamedXContentRegistry registry,
        DeprecationHandler deprecationHandler,
        XContentType xContentType
    ) {
        super(values, registry, deprecationHandler, xContentType);
        try {
            streamInput = keyBytes.streamInput();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected ESONEntry nextEntry() throws IOException {
        if (readOpenObject) {
            String key;
            if (ESONStack.isObject(containerStack.currentStackValue())) {
                int stringLength = streamInput.readVInt();
                byte[] stringBytes = new byte[stringLength];
                streamInput.readBytes(stringBytes, 0, stringLength);
                key = new String(stringBytes, StandardCharsets.UTF_8);
            } else {
                key = null;
            }
            byte type = streamInput.readByte();
            int offsetOrCount;
            if (type == ESONEntry.TYPE_NULL || type == ESONEntry.TYPE_TRUE || type == ESONEntry.TYPE_FALSE) {
                offsetOrCount = -1;
            } else {
                offsetOrCount = streamInput.readInt();
            }
            return switch (type) {
                case ESONEntry.TYPE_OBJECT -> new ESONEntry.ObjectEntry(key, offsetOrCount);
                case ESONEntry.TYPE_ARRAY -> new ESONEntry.ArrayEntry(key, offsetOrCount);
                default -> new ESONEntry.FieldEntry(key, type, offsetOrCount);
            };
        } else {
            // Skip the number of entries
            streamInput.readVInt();
            byte startType = streamInput.readByte();
            assert startType == ESONEntry.TYPE_OBJECT;
            int count = streamInput.readInt();
            readOpenObject = true;
            return new ESONEntry.ObjectEntry(null, count);

        }
    }

    @Override
    public void close() {
        super.close();
        IOUtils.closeWhileHandlingException(streamInput);
    }
}
