/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.blobstore.support;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.ChunkedLoggingStream;
import org.elasticsearch.common.unit.ByteSizeUnit;

import java.io.IOException;
import java.io.InputStream;

public class BlobContainerUtils {
    private BlobContainerUtils() {
        // no instances
    }

    public static final int MAX_REGISTER_CONTENT_LENGTH = 2 * Long.BYTES;

    public static void ensureValidRegisterContent(BytesReference bytesReference) {
        if (bytesReference.length() > MAX_REGISTER_CONTENT_LENGTH) {
            final var message = "invalid register content with length [" + bytesReference.length() + "]";
            assert false : message;
            throw new IllegalStateException(message);
        }
    }

    private static final Logger logger = LogManager.getLogger(BlobContainerUtils.class);

    /**
     * Many blob stores have consistent (linearizable/atomic) read semantics and in these casees it is safe to implement {@link
     * BlobContainer#getRegister} by simply reading the blob using this utility.
     *
     * NB it is not safe for the supplied stream to resume a partial downloads, because the resumed stream may see a different state from
     * the original.
     */
    public static BytesReference getRegisterUsingConsistentRead(InputStream inputStream, String container, String key) throws IOException {
        int len = MAX_REGISTER_CONTENT_LENGTH;
        int pos = 0;
        final byte[] bytes = new byte[len];
        while (len > 0) {
            final var read = inputStream.read(bytes, pos, len);
            if (read == -1) {
                break;
            }
            len -= read;
            pos += read;
        }
        final int nextByte = inputStream.read();
        if (nextByte != -1) {
            try (
                var cls = ChunkedLoggingStream.create(
                    logger,
                    Level.ERROR,
                    "getRegisterUsingConsistentRead including trailing data",
                    ReferenceDocs.LOGGING
                )
            ) {
                cls.write(bytes);
                cls.write(nextByte);
                final var buffer = new byte[ByteSizeUnit.KB.toIntBytes(1)];
                while (true) {
                    final var readSize = inputStream.read(buffer);
                    if (readSize == -1) {
                        break;
                    }
                    cls.write(buffer, 0, readSize);
                }
            }

            throw new IllegalStateException(
                Strings.format("[%s] failed reading register [%s] due to unexpected trailing data", container, key)
            );
        }
        return new BytesArray(bytes, 0, pos);
    }

}
