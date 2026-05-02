/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.eirf.EirfEncoder;
import org.elasticsearch.xcontent.XContentString;

/**
 * Accumulates the routing-relevant data of a single document during an EIRF parse pass and produces
 * a shard id for it without re-parsing the source.
 *
 * <p>Obtained from {@link IndexRouting#newRoutingExtractor()}. Routing strategies that do not need
 * to inspect the document source (id-or-routing strategies such as {@code Unpartitioned} and
 * {@code Partitioned}) return {@code null} from that factory and callers should use
 * {@link IndexRouting#indexShard(IndexRequest)} directly.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>Caller plugs the extractor into {@link EirfEncoder#parseToScratch} as the
 *       {@link EirfEncoder.LeafSink LeafSink}.</li>
 *   <li>The encoder fires the per-type primitive callbacks ({@link #onTextPrimitive},
 *       {@link #onLongPrimitive}, {@link #onDoublePrimitive}, {@link #onBooleanPrimitive}) and
 *       {@link #onArrayLeaf} for every leaf in the document, dispatched based on
 *       {@link Mode mode()}.</li>
 *   <li>If the parse completes normally, the caller invokes {@link #computeShardId(IndexRequest)}
 *       to obtain the shard id and apply any post-routing state (e.g. tsid stash on the request).</li>
 *   <li>If the parse throws (today: an array at a matched routing column, which the encoder
 *       packs into a single packed-array column and cannot replay element-by-element), the caller
 *       catches the exception, abandons EIRF encoding for the entire bulk, and routes every item
 *       through the inline-source path instead.</li>
 *   <li>{@link #reset()} prepares the extractor for the next document in the same bulk.</li>
 * </ol>
 *
 * <p>Concrete subclasses provide:
 * <ul>
 *   <li>{@link Mode mode()} — whether this extractor wants raw UTF-8 text bytes for every
 *       primitive leaf or typed values per primitive type.</li>
 *   <li>{@link #matchesField} — the strategy's path predicate, invoked at most once per leaf
 *       column on first encounter and cached for the remainder of the encoder's lifetime.</li>
 *   <li>One or more of {@link #handleTextPrimitive}, {@link #handleLongPrimitive},
 *       {@link #handleDoublePrimitive}, {@link #handleBooleanPrimitive} — the per-type builder
 *       feed for primitive leaves at matched columns.</li>
 *   <li>{@link #resetBuilderState} — per-document builder reset, called from {@link #reset()}.</li>
 *   <li>{@link #computeShardId} — strategy-specific finalization.</li>
 * </ul>
 *
 * <p>The column-level cache is intentionally <b>not</b> cleared between documents: column indices
 * are stable for the lifetime of the {@link EirfEncoder} they're attached to (the schema is built
 * up cumulatively across all docs in the bulk), so once column N's predicate result is known it
 * remains valid for every subsequent document.
 */
public abstract class RoutingExtractor implements EirfEncoder.LeafSink {

    private FixedBitSet evaluated = new FixedBitSet(64);
    private FixedBitSet matched = new FixedBitSet(64);

    @Override
    public final void onTextPrimitive(int columnIndex, String dottedPath, byte type, XContentString.UTF8Bytes textBytes) {
        if (isMatchedColumn(columnIndex, dottedPath)) {
            handleTextPrimitive(dottedPath, type, textBytes);
        }
    }

    @Override
    public final void onLongPrimitive(int columnIndex, String dottedPath, byte type, long value) {
        if (isMatchedColumn(columnIndex, dottedPath)) {
            handleLongPrimitive(dottedPath, type, value);
        }
    }

    @Override
    public final void onDoublePrimitive(int columnIndex, String dottedPath, byte type, double value) {
        if (isMatchedColumn(columnIndex, dottedPath)) {
            handleDoublePrimitive(dottedPath, type, value);
        }
    }

    @Override
    public final void onBooleanPrimitive(int columnIndex, String dottedPath, boolean value) {
        if (isMatchedColumn(columnIndex, dottedPath)) {
            handleBooleanPrimitive(dottedPath, value);
        }
    }

    @Override
    public final void onArrayLeaf(int columnIndex, String dottedPath) {
        // Both routing strategies' source-parser paths descend into arrays at matched columns and
        // emit one entry per element; the encoder packs arrays into a single column and cannot
        // replay that here. Throwing aborts the parse and is caught by BulkBatchEncoders, which
        // disables EIRF encoding for the rest of the bulk and routes every item through the
        // inline-source path.
        if (isMatchedColumn(columnIndex, dottedPath)) {
            throw new RoutingExtractionException("array at routing column [" + dottedPath + "] is not supported in batch encoding");
        }
    }

    /** Clear per-document state so the extractor can be reused for the next document in the bulk. */
    public final void reset() {
        resetBuilderState();
    }

    /**
     * Compute the shard id from the data accumulated since the last {@link #reset()}, applying
     * any per-request post-processing the routing strategy needs.
     */
    public abstract int computeShardId(IndexRequest indexRequest);

    /**
     * Strategy-specific path predicate. Invoked at most once per leaf column index across the
     * lifetime of this extractor; the result is cached internally.
     */
    protected abstract boolean matchesField(String dottedPath);

    /**
     * Called for {@code STRING} leaves at matched columns in any mode, and for every primitive
     * leaf at matched columns when {@link #mode()} is {@link Mode#RAW_TEXT}. Default no-op so
     * typed-mode subclasses that don't care about strings can leave it alone.
     */
    protected void handleTextPrimitive(String dottedPath, byte type, XContentString.UTF8Bytes textBytes) {}

    /**
     * Called for {@code INT} / {@code LONG} leaves at matched columns when {@link #mode()} is
     * {@link Mode#TYPED}. Default no-op so raw-mode subclasses can ignore typed callbacks.
     */
    protected void handleLongPrimitive(String dottedPath, byte type, long value) {}

    /**
     * Called for {@code FLOAT} / {@code DOUBLE} leaves at matched columns when {@link #mode()} is
     * {@link Mode#TYPED}.
     */
    protected void handleDoublePrimitive(String dottedPath, byte type, double value) {}

    /** Called for boolean leaves at matched columns when {@link #mode()} is {@link Mode#TYPED}. */
    protected void handleBooleanPrimitive(String dottedPath, boolean value) {}

    /** Reset per-document builder state. The column-level cache is preserved across documents. */
    protected abstract void resetBuilderState();

    private boolean isMatchedColumn(int columnIndex, String dottedPath) {
        ensureCapacity(columnIndex);
        if (evaluated.get(columnIndex) == false) {
            if (matchesField(dottedPath)) {
                matched.set(columnIndex);
            }
            evaluated.set(columnIndex);
        }
        return matched.get(columnIndex);
    }

    private void ensureCapacity(int columnIndex) {
        if (columnIndex < evaluated.length()) {
            return;
        }
        evaluated = FixedBitSet.ensureCapacity(evaluated, columnIndex);
        matched = FixedBitSet.ensureCapacity(matched, columnIndex);
    }
}
