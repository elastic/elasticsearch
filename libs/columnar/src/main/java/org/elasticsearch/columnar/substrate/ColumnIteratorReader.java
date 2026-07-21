/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;

/**
 * Rebuilds a field's {@link ColumnIterator} from its {@link ColumnIteratorMetadata}. Each
 * {@link #iterator()} call returns a fresh, independent iterator, so a reader is safe to share
 * across the value-access paths that each need their own cursor over the same column-iterator data.
 */
public final class ColumnIteratorReader {

    private final ColumnIteratorMetadata meta;
    private final IndexInput data;

    /**
     * @param meta the field's column-iterator metadata
     * @param data the data file holding the sparse structure; cloned per iterator, so the caller
     *             keeps ownership. May be {@code null} for empty or dense fields, which read no data.
     */
    public ColumnIteratorReader(ColumnIteratorMetadata meta, IndexInput data) {
        this.meta = meta;
        this.data = data;
    }

    public ColumnIterator iterator() throws IOException {
        if (meta.isEmpty()) {
            return ColumnIterator.emptyColumn();
        }
        if (meta.isDense()) {
            return ColumnIterator.dense(meta.maxDoc());
        }
        IndexedDISI disi = new IndexedDISI(
            data.clone(),
            meta.offset(),
            meta.length(),
            meta.jumpTableEntryCount(),
            meta.denseRankPower(),
            meta.numDocsWithField()
        );
        return ColumnIterator.sparse(disi);
    }
}
