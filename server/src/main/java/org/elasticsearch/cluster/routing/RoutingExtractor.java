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
 * <p><b>Invariant:</b> a given {@code RoutingExtractor} instance is only fed fields from documents
 * targeting the same index. Callers must obtain a fresh extractor via
 * {@link IndexRouting#newRoutingExtractor()} when switching indices, since both the routing
 * strategy and the column-index-to-path mapping are per-index.
 *
 * <p>The column-level cache is intentionally <b>not</b> cleared between documents: column indices
 * are stable for the lifetime of the {@link EirfEncoder} they're attached to (the schema is built
 * up cumulatively across all docs in the bulk for a single index), so once column N's predicate
 * result is known it remains valid for every subsequent document fed to this extractor.
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
        // emit one entry per element.
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
     * Called for {@code STRING} leaves at matched columns regardless of {@link #passRawText()},
     * and for every primitive leaf at matched columns when {@link #passRawText()} is {@code true}.
     */
    protected void handleTextPrimitive(String dottedPath, byte type, XContentString.UTF8Bytes textBytes) {}

    /**
     * Called for {@code INT} / {@code LONG} leaves at matched columns when {@link #passRawText()}
     * is {@code false}. Default no-op so raw-text subclasses can ignore typed callbacks.
     */
    protected void handleLongPrimitive(String dottedPath, byte type, long value) {}

    /**
     * Called for {@code FLOAT} / {@code DOUBLE} leaves at matched columns when
     * {@link #passRawText()} is {@code false}.
     */
    protected void handleDoublePrimitive(String dottedPath, byte type, double value) {}

    /**
     * Called for boolean leaves at matched columns when {@link #passRawText()} is {@code false}.
     */
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
