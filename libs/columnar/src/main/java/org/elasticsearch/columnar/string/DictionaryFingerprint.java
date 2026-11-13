/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Names the contents of a column's dictionary: its terms, in ordinal order. Two columns with the same fingerprint have the
 * same dictionary, so the same ordinal names the same term in both. That lets a reader carry what it learned about an
 * ordinal in one segment over to the next, without reading either dictionary to compare them.
 *
 * <p>A truncated SHA-256 rather than a cheaper hash, because a reader trusts an equal fingerprint without checking: two
 * different dictionaries that collide would count documents under the wrong terms. 128 bits keeps that out of reach of any
 * number of segments, and the dictionary is small enough, being bounded by the dictionary policy, that hashing it costs
 * nothing next to writing the column.
 */
public record DictionaryFingerprint(long high, long low) {

    void writeTo(DataOutput out) throws IOException {
        out.writeLong(high);
        out.writeLong(low);
    }

    static DictionaryFingerprint readFrom(DataInput in) throws IOException {
        final long high = in.readLong();
        final long low = in.readLong();
        return new DictionaryFingerprint(high, low);
    }

    /** Builds the fingerprint of a dictionary from its terms, handed over in ordinal order. */
    static final class Builder {
        private final MessageDigest digest;
        private final byte[] length = new byte[Integer.BYTES];
        private int terms;

        Builder() {
            try {
                digest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                throw new AssertionError("every JVM provides SHA-256", e);
            }
        }

        /** Adds the next term. Its length goes in first, so the terms {@code ab, c} and {@code a, bc} differ. */
        void add(BytesRef term) {
            ByteBuffer.wrap(length).putInt(term.length);
            digest.update(length);
            digest.update(term.bytes, term.offset, term.length);
            terms++;
        }

        DictionaryFingerprint build() {
            // The term count goes in too, so a dictionary is not named like one with a trailing empty term.
            ByteBuffer.wrap(length).putInt(terms);
            digest.update(length);
            final ByteBuffer hash = ByteBuffer.wrap(digest.digest());
            return new DictionaryFingerprint(hash.getLong(), hash.getLong());
        }
    }
}
