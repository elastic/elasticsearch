/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;

/**
 * A potentially-missing {@link BytesReference}, used to represent the contents of a blobstore register along with the possibility that the
 * register could not be read.
 */
public final class OptionalBytesReference {

    public static final OptionalBytesReference MISSING = new OptionalBytesReference(null);

    public static final OptionalBytesReference EMPTY = new OptionalBytesReference(BytesArray.EMPTY);

    private final BytesReference bytesReference;

    private OptionalBytesReference(BytesReference bytesReference) {
        this.bytesReference = bytesReference;
    }

    public static OptionalBytesReference of(BytesReference bytesReference) {
        if (bytesReference.length() == 0) {
            return EMPTY;
        } else {
            return new OptionalBytesReference(bytesReference);
        }
    }

    public boolean isPresent() {
        return bytesReference != null;
    }

    public BytesReference bytesReference() {
        if (bytesReference == null) {
            assert false : "missing";
            throw new IllegalStateException("cannot get bytesReference() on OptionalBytesReference#MISSING");
        }
        return bytesReference;
    }
}
