/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.internal.fieldnames;

/**
 * Thread-confined field name cache used during JSON parsing. Implementations canonicalize
 * raw UTF-8 byte ranges into interned {@link String} instances so that repeated field names
 * across documents share the same object, reducing allocation and enabling identity comparisons.
 *
 * <p>A lookup returns the cached {@link String} or {@code null} on miss. On miss the caller
 * should call {@link #insert} to register the name. Separating lookup and insert allows the
 * caller to defer UTF-8 decoding until a miss is confirmed.
 *
 * <p>Implementations typically operate in two phases: a <em>learning</em> phase (linear scan)
 * while new field names are still appearing, followed by a <em>frozen</em> phase (hash table)
 * once the schema stabilizes. {@link #maybeFreezeAfterDocument()} or {@link #freeze()}
 * triggers the transition; {@link #release()} merges discoveries back to a shared parent
 * (if applicable) and prepares the instance for reuse.
 *
 * <p>Instances are <strong>not thread-safe</strong>. Each parsing thread should own its own
 * instance, typically obtained from a parent/root table's {@code makeChild()} method.
 */
public interface FieldNameLookup {

    /**
     * Looks up the canonical {@link String} for the field name at {@code buf[off, off+len)}.
     *
     * @param buf  the source byte buffer
     * @param off  start offset of the field name bytes
     * @param len  length of the field name in bytes
     * @param hash precomputed hash from {@link FieldNameHash#hashName}
     * @return the cached String, or {@code null} if not found
     */
    String lookup(byte[] buf, int off, int len, int hash);

    /**
     * Looks up the canonical {@link String} using a pre-computed prefix8 value,
     * avoiding a re-read of the first 8 bytes of the field name for prefix comparison.
     * The default implementation ignores the prefix and delegates to {@link #lookup(byte[], int, int, int)}.
     *
     * @param buf     the source byte buffer
     * @param off     start offset of the field name bytes
     * @param len     length of the field name in bytes
     * @param hash    precomputed hash from {@link FieldNameHash#hashName} or {@link FieldNameHash#hashWord}
     * @param prefix8 the first min(len, 8) bytes as a little-endian long, zero-padded
     * @return the cached String, or {@code null} if not found
     */
    default String lookup(byte[] buf, int off, int len, int hash, long prefix8) {
        return lookup(buf, off, len, hash);
    }

    /**
     * Inserts a new field name into the cache. Called after a {@link #lookup} miss.
     *
     * @param buf  the source byte buffer
     * @param off  start offset of the field name bytes
     * @param len  length of the field name in bytes
     * @param hash precomputed hash from {@link FieldNameHash#hashName}
     * @return the canonical String for this field name
     */
    String insert(byte[] buf, int off, int len, int hash);

    /**
     * Looks up a field name and returns a {@link ResolvedFieldName} carrying the dense ordinal
     * when the table is frozen. Default delegates to {@link #lookup(byte[], int, int, int, long)}.
     */
    default ResolvedFieldName lookupField(byte[] buf, int off, int len, int hash, long prefix8) {
        String name = lookup(buf, off, len, hash, prefix8);
        return name == null ? null : new ResolvedFieldName(name, -1);
    }

    /**
     * Inserts a field name and returns a {@link ResolvedFieldName}. Default delegates to
     * {@link #insert(byte[], int, int, int)}.
     */
    default ResolvedFieldName insertField(byte[] buf, int off, int len, int hash) {
        return new ResolvedFieldName(insert(buf, off, len, hash), -1);
    }

    /**
     * Marks the start of a new document walk. Implementations may reset per-document
     * learning state used to decide when to freeze.
     */
    default void beginDocument() {}

    /**
     * Freezes the cache into an optimized read-only structure. After this call,
     * lookups should be faster but new names may go to an overflow area.
     */
    void freeze();

    /**
     * Attempts to freeze after a document when the field-name set has stabilized.
     * Default is a no-op; {@link FrozenFieldNameTable} freezes once a document
     * completes without introducing new names.
     */
    default void maybeFreezeAfterDocument() {}

    /**
     * Merges any new entries back to a shared parent (if applicable) and prepares
     * this instance for reuse with the next batch/document.
     */
    void release();
}
