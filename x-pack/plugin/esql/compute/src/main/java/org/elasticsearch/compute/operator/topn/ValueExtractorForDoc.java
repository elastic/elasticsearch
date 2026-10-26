/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.DocOriginTable;
import org.elasticsearch.compute.data.DocOrigins;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;

import java.util.Arrays;

/**
 * Writes the shard, segment and doc of a row. For rows that left their node the shard is the number of the row's origin
 * in the {@link DocOriginTable} of the query, because the ordinals of a page's dictionary mean nothing for the next page.
 */
class ValueExtractorForDoc implements ValueExtractor {
    private final DocVector vector;
    private final TopNEncoder encoder;
    @Nullable
    private final DocOriginTable table;
    /**
     * The table number of each origin of the page's dictionary, filled as rows reference them.
     */
    @Nullable
    private final int[] tableOrdinals;

    @Override
    public RefCounted getRefCountedForShard(int position) {
        return vector().shardRefCounted(position);
    }

    ValueExtractorForDoc(TopNEncoder encoder, DocVector vector) {
        this.encoder = encoder;
        this.vector = vector;
        this.table = encoder instanceof DocVectorEncoder docEncoder ? docEncoder.origins() : null;
        DocOrigins origins = vector.origins();
        if ((origins == null) != (table == null)) {
            // a table numbers origins, and the shard slots of this node have none
            throw new IllegalStateException(origins == null ? "expected a detached DocVector" : "expected an attached DocVector");
        }
        if (origins == null) {
            this.tableOrdinals = null;
        } else {
            this.tableOrdinals = new int[origins.size()];
            Arrays.fill(tableOrdinals, -1);
        }
    }

    DocVector vector() {
        return vector;
    }

    @Override
    public void writeValue(BreakingBytesRefBuilder values, int position) {
        encoder.encodeInt(shard(position), values);
        encoder.encodeInt(vector.segments().getInt(position), values);
        encoder.encodeInt(vector.docs().getInt(position), values);
    }

    private int shard(int position) {
        int shard = vector.shards().getInt(position);
        if (tableOrdinals == null) {
            return shard;
        }
        int ordinal = tableOrdinals[shard];
        if (ordinal < 0) {
            ordinal = table.ordinal(vector.origins().get(shard));
            tableOrdinals[shard] = ordinal;
        }
        return ordinal;
    }

    @Override
    public String toString() {
        return "ValueExtractorForDoc";
    }
}
