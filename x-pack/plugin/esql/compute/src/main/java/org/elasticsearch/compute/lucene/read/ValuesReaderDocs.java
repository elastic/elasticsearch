/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.BlockLoader;

/**
 * Implementation of {@link BlockLoader.Docs} for ESQL. It's important that
 * only this implementation, and the implementation returned by {@link #mapped}
 * exist. This allows the jvm to inline the {@code invokevirtual}s to call
 * the interface in hot, hot code.
 * <p>
 *     We've investigated moving the {@code offset} parameter from the
 *     {@link BlockLoader.ColumnAtATimeReader#read} into this. That's more
 *     readable, but a clock cycle slower.
 * </p>
 * <p>
 *     When we tried having a {@link Nullable} map member instead of a subclass
 *     that was also slower.
 * </p>
 */
class ValuesReaderDocs implements BlockLoader.Docs {
    private final DocVector docs;
    private int count;

    ValuesReaderDocs(DocVector docs) {
        this.docs = docs;
        this.count = docs.getPositionCount();
    }

    final Mapped mapped(int[] forwards) {
        return new Mapped(docs, forwards);
    }

    public final void setCount(int count) {
        this.count = count;
    }

    @Override
    public final int count() {
        return count;
    }

    @Override
    public int get(int i) {
        return docs.docs().getInt(i);
    }

    private class Mapped extends ValuesReaderDocs {
        private final int[] forwards;

        private Mapped(DocVector docs, int[] forwards) {
            super(docs);
            this.forwards = forwards;
        }

        @Override
        public int get(int i) {
            return super.get(forwards[i]);
        }
    }
}
