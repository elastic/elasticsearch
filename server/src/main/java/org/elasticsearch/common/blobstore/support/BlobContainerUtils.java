/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore.support;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.util.OptionalLong;

public class BlobContainerUtils {
    private BlobContainerUtils() {
        // no instances
    }

    /**
     * Many blob stores have consistent (linearizable/atomic) read semantics and in these casees it is safe to implement {@link
     * BlobContainer#getRegister} by simply reading the blob using this utility.
     *
     * NB it is not safe for the supplied stream to resume a partial downloads, because the resumed stream may see a different state from
     * the original.
     */
    public static OptionalLong getRegisterUsingConsistentRead(InputStream inputStream, String container, String key) throws IOException {
        try (inputStream) {
            int len = Long.BYTES;
            int pos = 0;
            final byte[] bytes = new byte[len];
            while (len > 0) {
                final var read = inputStream.read(bytes, pos, len);
                if (read == -1) {
                    throw new IllegalStateException(
                        Strings.format("[%s] failed reading register [%s] due to truncation at [%d] bytes", container, key, pos)
                    );
                }
                len -= read;
                pos += read;
            }
            if (inputStream.read() != -1) {
                throw new IllegalStateException(
                    Strings.format("[%s] failed reading register [%s] due to unexpected trailing data", container, key)
                );
            }
            return OptionalLong.of(ByteBuffer.wrap(bytes).getLong());
        } catch (NoSuchFileException e) {
            return OptionalLong.of(0L);
        }
    }
}
