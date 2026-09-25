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
 * raw UTF-8 byte ranges into a cached {@link String} instance so that repeated field names
 * across documents share the same object, reducing allocation.
 *
 * <p>That sharing is stable only <em>within</em> one {@link #release()} cycle, not across one:
 * {@link #release()} may re-sync this instance against a shared parent, and a name this instance
 * previously resolved can then either resolve to a different, but {@code equals}, instance another
 * instance published first, or - rarely - stop resolving at all and require a fresh {@link #insert}
 * (see {@link #release()}). Callers must compare resolved names with {@code equals}, not {@code ==},
 * and must not assume a name that resolved once will keep resolving forever.
 *
 * <p>A lookup returns the cached {@link String} or {@code null} on miss. On miss the caller
 * should call {@link #insert} to register the name. Separating lookup and insert allows the
 * caller to defer UTF-8 decoding until a miss is confirmed.
 *
 * <p>Implementations typically operate in two phases: a <em>learning</em> phase (linear scan)
 * during the first document, followed by a <em>frozen</em> phase (hash table) for subsequent
 * documents. {@link #freeze()} triggers the transition; {@link #release()} merges discoveries
 * back to a shared parent (if applicable) and prepares the instance for reuse.
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
     * Freezes the cache into an optimized read-only structure. After this call,
     * lookups should be faster but new names may go to an overflow area.
     */
    void freeze();

    /**
     * Merges any new entries back to a shared parent (if applicable), re-syncs this instance
     * against the shared table so it picks up names other instances have published since its last
     * release, and prepares this instance for reuse with the next batch/document.
     *
     * <p>Re-syncing usually only changes identity, not resolvability: a name already in this
     * instance's frozen table keeps its instance as the shared table grows, while a name previously
     * resolved through its overflow may start resolving to a different, but {@code equals}, instance
     * if another instance published it first. But shared-table growth is capped, and the table may
     * occasionally be reset rather than grown as a safety valve for long-running processes; a reset
     * discards every name the previous shared table held, including ones this instance's own frozen
     * table already resolved. After a reset, such a name stops resolving here until it is looked up
     * as a miss and inserted again, exactly as if this instance had never seen it. Safe to call
     * repeatedly — a call with nothing new to publish or adopt is a no-op.
     */
    void release();
}
