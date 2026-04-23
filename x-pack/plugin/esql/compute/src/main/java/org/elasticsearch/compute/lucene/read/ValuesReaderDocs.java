/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.BlockLoader;

import java.util.HashSet;
import java.util.Set;

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

    final Mapped mapped(int[] forwards, int readStart, int readEnd) {
        return new Mapped(docs, forwards, readStart, readEnd);
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

    @Override
    public boolean mayContainDuplicates() {
        return docs.mayContainDuplicates();
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder("docs[");
        if (count() <= 100) {
            b.append(get(0));
            for (int i = 1; i < count(); i++) {
                b.append(", ").append(get(i));
            }
        } else {
            b.append(count()).append(" docs");
        }
        return b.append("]").toString();
    }

    private static class Mapped extends ValuesReaderDocs {
        private final int[] forwards;

        private Mapped(DocVector docs, int[] forwards, int readStart, int readEnd) {
            super(docs);
            this.forwards = forwards;
            /*
             * If we're configured not to contain duplicates *and* assertions
             * are enabled, assert that we don't have dupes. It's paranoid,
             * but it's important mayContainDuplicates() doesn't lie. Too
             * expensive to do at regular runtime, but hey.
             */
            if (Assertions.ENABLED && mayContainDuplicates() == false) {
                assertNoDupes(readStart, readEnd);
            }
        }

        @Override
        public int get(int i) {
            return super.get(forwards[i]);
        }

        private void assertNoDupes(int readStart, int readEnd) {
            Set<Integer> set = new HashSet<>(count());
            for (int i = readStart; i < readEnd; i++) {
                Integer doc = get(i);
                boolean firstTime = set.add(doc);
                if (firstTime == false) {
                    throw new IllegalStateException(
                        "configured not to contain duplicates between ["
                            + readStart
                            + "] and ["
                            + readEnd
                            + "] but "
                            + doc
                            + " was duplicated in "
                            + this
                    );
                }
            }
        }
    }
}
