/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.DocOriginTable;
import org.elasticsearch.compute.lucene.AlwaysReferencedIndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Encodes a {@code _doc} column. On the node that read the documents a row keeps its shard slot and pins its shard. Rows
 * that left their node, on a coordinator that fetches their columns later, keep the number of their origin in a
 * {@link DocOriginTable} instead, and pin nothing.
 * <p>
 * The plan marks such a column with {@link #detached()}. Each operator binds it to a table of its own with
 * {@link #withOrigins}, so operators on different drivers never share a table and the table needs no locking.
 */
public class DocVectorEncoder extends DefaultUnsortableTopNEncoder {
    private static final DocVectorEncoder DETACHED = new DocVectorEncoder(AlwaysReferencedIndexedByShardId.INSTANCE, true, null);

    private final IndexedByShardId<? extends RefCounted> refCounteds;
    private final boolean detached;
    /**
     * The table of the operator for a detached column, {@code null} for a column of this node or one no operator bound.
     */
    @Nullable
    private final DocOriginTable origins;

    /**
     * For the rows of the node that read them. Each row pins its shard through {@code refCounteds}.
     */
    public DocVectorEncoder(IndexedByShardId<? extends RefCounted> refCounteds) {
        this(refCounteds, false, null);
    }

    private DocVectorEncoder(IndexedByShardId<? extends RefCounted> refCounteds, boolean detached, @Nullable DocOriginTable origins) {
        this.refCounteds = refCounteds;
        this.detached = detached;
        this.origins = origins;
    }

    /**
     * For rows that left their node. Each operator numbers their origins in a table of its own.
     */
    public static DocVectorEncoder detached() {
        return DETACHED;
    }

    /**
     * A table for the detached {@code _doc} columns of one operator, {@code null} if it has none.
     */
    @Nullable
    static DocOriginTable originTableFor(List<TopNEncoder> encoders, CircuitBreaker breaker) {
        for (TopNEncoder encoder : encoders) {
            if (encoder instanceof DocVectorEncoder docEncoder && docEncoder.detached) {
                return new DocOriginTable(breaker);
            }
        }
        return null;
    }

    /**
     * The encoders of one operator, with every detached {@code _doc} column numbering its origins in {@code origins}.
     */
    static List<TopNEncoder> withOrigins(List<TopNEncoder> encoders, DocOriginTable origins) {
        List<TopNEncoder> bound = new ArrayList<>(encoders.size());
        for (TopNEncoder encoder : encoders) {
            bound.add(encoder instanceof DocVectorEncoder docEncoder ? docEncoder.withOrigins(origins) : encoder);
        }
        return List.copyOf(bound);
    }

    /**
     * This encoder numbering its origins in {@code origins} if its rows left their node, this encoder otherwise.
     */
    DocVectorEncoder withOrigins(DocOriginTable origins) {
        return detached ? new DocVectorEncoder(refCounteds, true, Objects.requireNonNull(origins)) : this;
    }

    public IndexedByShardId<? extends RefCounted> refCounteds() {
        return refCounteds;
    }

    /**
     * The origin table of the operator, {@code null} when the rows are on the node that read them.
     *
     * @throws IllegalStateException if the rows left their node and no operator bound a table with {@link #withOrigins}
     */
    @Nullable
    DocOriginTable origins() {
        if (detached && origins == null) {
            throw new IllegalStateException("detached [_doc] columns need the origin table of their operator");
        }
        return origins;
    }

    @Override
    public String toString() {
        return "Doc";
    }
}
