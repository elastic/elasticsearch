/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.DocOriginTable;
import org.elasticsearch.compute.lucene.AlwaysReferencedIndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;

import java.util.Objects;

/**
 * Encodes a {@code _doc} column. On the node that read the documents a row keeps its shard slot and pins its shard. Rows
 * that left their node, on a coordinator that fetches their columns later, keep the number of their origin in the
 * {@link DocOriginTable} of the query instead, and pin nothing.
 */
public class DocVectorEncoder extends DefaultUnsortableTopNEncoder {
    private final IndexedByShardId<? extends RefCounted> refCounteds;
    @Nullable
    private final DocOriginTable origins;

    /**
     * For the rows of the node that read them. Each row pins its shard through {@code refCounteds}.
     */
    public DocVectorEncoder(IndexedByShardId<? extends RefCounted> refCounteds) {
        this(refCounteds, null);
    }

    /**
     * For rows that left their node, numbered in the origin table of the query.
     */
    public DocVectorEncoder(DocOriginTable origins) {
        this(AlwaysReferencedIndexedByShardId.INSTANCE, Objects.requireNonNull(origins));
    }

    private DocVectorEncoder(IndexedByShardId<? extends RefCounted> refCounteds, @Nullable DocOriginTable origins) {
        this.refCounteds = refCounteds;
        this.origins = origins;
    }

    public IndexedByShardId<? extends RefCounted> refCounteds() {
        return refCounteds;
    }

    /**
     * The origin table of the query, {@code null} when the rows are on the node that read them.
     */
    @Nullable
    public DocOriginTable origins() {
        return origins;
    }

    @Override
    public String toString() {
        return "Doc";
    }
}
