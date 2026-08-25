/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.flattened;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;

/**
 * Optional capability mixed into the {@link BinaryDocValues} returned for a flattened
 * {@code ._keyed} field stored in a columnar layout. Callers that only need one sub-field
 * should {@code instanceof}-check for this abstract class and use its keyed API instead of
 * decoding the whole {@link BinaryDocValues#binaryValue()} blob: the implementation reads
 * and decompresses only that sub-field's compressed run.
 *
 * <p>Each instance holds one {@link org.apache.lucene.index.BinaryDocValues} clone with independent cursor state per
 * key ordinal. Alternating between keys on one instance does not evict a cache but does require re-seeks on the shared
 * underlying {@link org.apache.lucene.store.IndexInput}. Prefer one instance per key when only one sub-field is needed.
 *
 * <p>The returned {@link BytesRef} from {@link #nextKeyValue()} is reusable and valid only
 * until the next call on this instance. Calling {@link #nextKeyValue()} more times than the
 * slot count returned by {@link #advanceExactKey(int)} is not detectable at runtime — the
 * implementation returns {@code null} for both null slots and exhausted iteration.
 */
public abstract class ColumnarKeyedBinaryDocValues extends BinaryDocValues {

    /**
     * Resolves a key to its segment-wide ordinal, which can then be passed to
     * {@link #advanceExactKey(int)} on every document without repeating the key lookup.
     *
     * @param key the sub-field key bytes (without the {@code \0} separator suffix)
     * @return the non-negative segment ordinal, or {@code -1} if the key does not appear
     *         anywhere in this segment — callers can skip the entire segment in that case.
     */
    public abstract int lookupKeyOrdinal(BytesRef key);

    /**
     * Positions on the slots of {@code keyOrdinal} for the document the iterator is
     * currently on (after a successful {@link #advanceExact} / {@link #advance}).
     *
     * @param keyOrdinal the key ordinal from {@link #lookupKeyOrdinal(BytesRef)}
     * @return the number of slots (including null slots) in document order for this key;
     *         {@code 0} when the document has no entry for this key.
     * @throws IOException on I/O error
     */
    public abstract int advanceExactKey(int keyOrdinal) throws IOException;

    /**
     * Returns the next slot value in document order for the key positioned by
     * {@link #advanceExactKey(int)}. {@code null} indicates a null slot (as written by
     * {@code KeyedArrayOrderInlineNull.recordNull}).
     *
     * <p>Must only be called after {@link #advanceExactKey(int)} has returned a positive
     * count, and at most that many times.
     *
     * @return the next value bytes, or {@code null} for a null slot
     * @throws IOException on I/O error
     */
    @Nullable
    public abstract BytesRef nextKeyValue() throws IOException;

    /**
     * Returns a batch reader that loads an entire page of documents for the single column
     * identified by {@code keyOrdinal} in one forward scan, instead of seeking per document.
     *
     * <p>The returned reader has its own independent column cursor — using it does not affect
     * the iterator position of this instance, and interleaving calls to
     * {@link #advanceExactKey}/{@link #nextKeyValue} with calls to the returned reader is safe.
     *
     * <p>The reader produces output identical to the per-doc keyed path: non-null slot values
     * only, sorted and deduplicated. A document with exactly one surviving value is emitted as
     * a plain {@code appendBytesRef} (no position entry); a document with zero surviving values
     * emits a null entry. {@link BlockLoader.Docs} must provide doc ids in non-decreasing order
     * (the contract of {@link BlockLoader.Docs}); duplicate ids are handled correctly.
     *
     * <p>The returned reader is not {@link org.elasticsearch.core.Releasable}; ownership of
     * any underlying {@link org.apache.lucene.store.IndexInput} clone stays with the producer.
     *
     * @param keyOrdinal the key ordinal from {@link #lookupKeyOrdinal(BytesRef)}
     * @return a batch reader, or {@code null} if batch reading is not available for this ordinal
     *         (e.g. the ordinal is out of range or this is not a columnar implementation)
     * @throws IOException on I/O error
     */
    @Nullable
    public abstract BlockLoader.OptionalColumnAtATimeReader keyColumnReader(int keyOrdinal) throws IOException;
}
