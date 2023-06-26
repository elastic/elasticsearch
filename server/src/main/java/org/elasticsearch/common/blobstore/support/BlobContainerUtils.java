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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;
import java.io.InputStream;

public class BlobContainerUtils {
    private BlobContainerUtils() {
        // no instances
    }

    public static final int MAX_REGISTER_CONTENT_LENGTH = Long.BYTES;

    public static void ensureValidRegisterContent(BytesReference bytesReference) {
        if (bytesReference.length() > MAX_REGISTER_CONTENT_LENGTH) {
            final var message = "invalid register content with length [" + bytesReference.length() + "]";
            assert false : message;
            throw new IllegalStateException(message);
        }
    }

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
        if (inputStream.read() != -1) {
            throw new IllegalStateException(
                Strings.format("[%s] failed reading register [%s] due to unexpected trailing data", container, key)
            );
        }
        return new BytesArray(bytes, 0, pos);
    }

}
