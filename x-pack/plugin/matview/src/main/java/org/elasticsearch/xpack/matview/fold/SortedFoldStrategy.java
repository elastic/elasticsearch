/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.matview.fold;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Folds by merge-sorting: when the fold key is a prefix of the index sort, every segment is already ordered by key,
 * so a k-way merge over the readers visits each group's documents contiguously. Representatives and hidden
 * documents are decided in one such pass once all readers of the merge are known, and the reduced values are
 * computed by a second, streaming pass that follows the doc values writer, which consumes the merged segment in the
 * same order. State is one cursor per reader and one accumulator for the current group; no key is ever hashed or
 * kept.
 */
public final class SortedFoldStrategy implements FoldStrategy {

    private final boolean[] reverse;

    /**
     * @param reverse per key component, whether the index sort is descending on it
     */
    public SortedFoldStrategy(boolean[] reverse) {
        this.reverse = reverse;
    }

    /** The strategy for an index whose sort starts with the fold keys, or {@code null} when it does not. */
    @Nullable
    public static SortedFoldStrategy forSort(FoldSpec spec, @Nullable Sort sort) {
        if (sort == null || sort.getSort().length < spec.keys().size()) {
            return null;
        }
        boolean[] reverse = new boolean[spec.keys().size()];
        for (int i = 0; i < reverse.length; i++) {
            SortField field = sort.getSort()[i];
            if (spec.keys().get(i).name().equals(field.getField()) == false) {
                return null;
            }
            reverse[i] = field.getReverse();
        }
        return new SortedFoldStrategy(reverse);
    }

    @Override
    public MergeFold newMergeFold(FoldSpec spec, @Nullable FoldGuard guard) {
        return new SortedMergeFold(spec, guard, reverse);
    }

    /** Compares two keys the way the index sort orders them. */
    static int compare(FoldSpec spec, boolean[] reverse, long[] la, BytesRef[] ba, long[] lb, BytesRef[] bb) {
        for (int i = 0; i < reverse.length; i++) {
            int c = spec.keys().get(i).type() == org.elasticsearch.compute.data.ElementType.LONG
                ? Long.compare(la[i], lb[i])
                : ba[i].compareTo(bb[i]);
            if (c != 0) {
                return reverse[i] ? -c : c;
            }
        }
        return 0;
    }

    static final class SortedMergeFold implements MergeFold {
        private final FoldSpec spec;
        @Nullable
        private final FoldGuard guard;
        private final boolean[] reverse;
        private final List<Input> inputs = new ArrayList<>();
        private boolean hiddenComputed;
        /** Streaming reduction cursor shared by all readers; replaced whenever the writer starts a new pass. */
        private GroupCursor cursor;

        SortedMergeFold(FoldSpec spec, @Nullable FoldGuard guard, boolean[] reverse) {
            this.spec = spec;
            this.guard = guard;
            this.reverse = reverse;
        }

        @Override
        public synchronized CodecReader wrap(CodecReader reader) throws IOException {
            if (hiddenComputed) {
                throw new IllegalStateException("a reader was wrapped after the fold decided what to hide");
            }
            if (KeyReaders.open(reader, spec) == null) {
                return reader; // a key field is missing entirely: no keyed documents, nothing to fold
            }
            Input input = new Input(inputs.size(), reader, reader.getLiveDocs(), guard == null ? null : guard.foldable(reader));
            inputs.add(input);
            return new FoldingCodecReader(reader, spec, input);
        }

        /**
         * One k-way merge over all readers: the first document of every group becomes its representative and the
         * rest are hidden. Runs once, when the writer first asks a wrapped reader for its live docs, by which time
         * every reader of the merge has been wrapped.
         */
        private synchronized void ensureHidden() {
            if (hiddenComputed) {
                return;
            }
            try {
                PriorityQueue<Stream> queue = newQueue(false);
                long[] groupLongs = new long[spec.keys().size()];
                BytesRef[] groupBytes = new BytesRef[spec.keys().size()];
                boolean inGroup = false;
                while (queue.size() > 0) {
                    Stream top = queue.top();
                    if (inGroup && compare(spec, reverse, top.longs, top.bytes, groupLongs, groupBytes) == 0) {
                        top.input.hide(top.doc);
                    } else {
                        top.copyKeyTo(groupLongs, groupBytes);
                        inGroup = true;
                    }
                    if (top.next()) {
                        queue.updateTop();
                    } else {
                        queue.pop();
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            hiddenComputed = true;
        }

        /** Reduced values of the group with the given key, or {@code null} when no such group was folded. */
        private synchronized long[] totalsFor(long[] longs, BytesRef[] bytes, boolean[] hasValue) throws IOException {
            if (cursor == null || cursor.isPast(longs, bytes)) {
                cursor = new GroupCursor(); // the writer started a new pass over the merged segment
            }
            return cursor.totalsFor(longs, bytes, hasValue);
        }

        private PriorityQueue<Stream> newQueue(boolean withValues) throws IOException {
            PriorityQueue<Stream> queue = new PriorityQueue<>(inputs.size()) {
                @Override
                protected boolean lessThan(Stream a, Stream b) {
                    int c = compare(spec, reverse, a.longs, a.bytes, b.longs, b.bytes);
                    return c != 0 ? c < 0 : a.input.index < b.input.index;
                }
            };
            for (Input input : inputs) {
                Stream stream = new Stream(input, withValues);
                if (stream.next()) {
                    queue.add(stream);
                }
            }
            return queue;
        }

        @Override
        public synchronized void close() {
            cursor = null;
        }

        /** A merge input reader and what the fold decided about it. */
        private final class Input implements ReaderFold {
            final int index;
            final CodecReader reader;
            @Nullable
            final Bits live;
            @Nullable
            final Bits foldable;
            @Nullable
            private FixedBitSet foldedLive;
            private int hidden;
            /** Forward-only key reader for the documents the writer asks values for; reopened when it goes backwards. */
            private KeyReaders.Component[] lookup;
            private int lookupDoc = -1;
            private final long[] longs;
            private final BytesRef[] bytes;
            private final boolean[] hasValue;

            Input(int index, CodecReader reader, @Nullable Bits live, @Nullable Bits foldable) {
                this.index = index;
                this.reader = reader;
                this.live = live;
                this.foldable = foldable;
                this.longs = new long[spec.keys().size()];
                this.bytes = new BytesRef[spec.keys().size()];
                this.hasValue = new boolean[spec.fields().size()];
            }

            void hide(int doc) {
                if (foldedLive == null) {
                    foldedLive = new FixedBitSet(reader.maxDoc());
                    if (live == null) {
                        foldedLive.set(0, reader.maxDoc());
                    } else {
                        for (int i = 0; i < reader.maxDoc(); i++) {
                            if (live.get(i)) {
                                foldedLive.set(i);
                            }
                        }
                    }
                }
                foldedLive.clear(doc);
                hidden++;
            }

            boolean eligible(int doc) {
                return (live == null || live.get(doc)) && (foldable == null || foldable.get(doc));
            }

            @Override
            public Bits foldedLive() {
                ensureHidden();
                return foldedLive;
            }

            @Override
            public int numDocs() {
                ensureHidden();
                return reader.numDocs() - hidden;
            }

            @Override
            public long reducedValue(int doc, int field, long raw) throws IOException {
                if (foldable != null && foldable.get(doc) == false) {
                    return raw;
                }
                ensureHidden();
                if (lookup == null || doc <= lookupDoc) {
                    lookup = KeyReaders.open(reader, spec);
                }
                lookupDoc = doc;
                if (KeyReaders.read(lookup, doc, longs, bytes) == false) {
                    return raw; // no group
                }
                long[] totals = totalsFor(longs, bytes, hasValue);
                if (totals == null || hasValue[field] == false) {
                    return raw;
                }
                return totals[field];
            }
        }

        /** Walks one reader's foldable documents in doc order, which is key order, with their key and values. */
        private final class Stream {
            final Input input;
            private final KeyReaders.Component[] keys;
            @Nullable
            private final SortedNumericDocValues[] values;
            final long[] longs;
            final BytesRef[] bytes;
            int doc = -1;

            Stream(Input input, boolean withValues) throws IOException {
                this.input = input;
                this.keys = KeyReaders.open(input.reader, spec);
                this.longs = new long[spec.keys().size()];
                this.bytes = new BytesRef[spec.keys().size()];
                if (withValues) {
                    values = new SortedNumericDocValues[spec.fields().size()];
                    for (int f = 0; f < values.length; f++) {
                        values[f] = DocValues.getSortedNumeric(input.reader, spec.fields().get(f).name());
                    }
                } else {
                    values = null;
                }
            }

            boolean next() throws IOException {
                int maxDoc = input.reader.maxDoc();
                for (doc++; doc < maxDoc; doc++) {
                    if (input.eligible(doc) && KeyReaders.read(keys, doc, longs, bytes)) {
                        return true;
                    }
                }
                return false;
            }

            void copyKeyTo(long[] toLongs, BytesRef[] toBytes) {
                for (int i = 0; i < longs.length; i++) {
                    toLongs[i] = longs[i];
                    if (bytes[i] != null) {
                        toBytes[i] = BytesRef.deepCopyOf(bytes[i]);
                    }
                }
            }
        }

        /**
         * The streaming reduction: a k-way merge over all readers that accumulates one group at a time. The writer
         * asks for groups in merged order, so the cursor only ever moves forward.
         */
        private final class GroupCursor {
            private final PriorityQueue<Stream> queue = newQueue(true);
            private final long[] groupLongs = new long[spec.keys().size()];
            private final BytesRef[] groupBytes = new BytesRef[spec.keys().size()];
            private boolean inGroup;
            private final double[] doubleSums = new double[spec.fields().size()];
            private final long[] longSums = new long[spec.fields().size()];
            private final boolean[] hasValue = new boolean[spec.fields().size()];
            private final long[] totals = new long[spec.fields().size()];

            GroupCursor() throws IOException {}

            boolean isPast(long[] longs, BytesRef[] bytes) {
                return inGroup && compare(spec, reverse, groupLongs, groupBytes, longs, bytes) > 0;
            }

            @Nullable
            long[] totalsFor(long[] longs, BytesRef[] bytes, boolean[] hasValueOut) throws IOException {
                while (true) {
                    Stream top = queue.size() == 0 ? null : queue.top();
                    boolean groupDone = top == null
                        || inGroup == false
                        || compare(spec, reverse, top.longs, top.bytes, groupLongs, groupBytes) != 0;
                    if (inGroup && groupDone) {
                        int c = compare(spec, reverse, groupLongs, groupBytes, longs, bytes);
                        if (c == 0) {
                            return finish(hasValueOut);
                        }
                        if (c > 0) {
                            return null; // the asked group was never seen: it does not fold
                        }
                    }
                    if (top == null) {
                        return null;
                    }
                    if (inGroup == false || groupDone) {
                        top.copyKeyTo(groupLongs, groupBytes);
                        inGroup = true;
                        java.util.Arrays.fill(doubleSums, 0);
                        java.util.Arrays.fill(longSums, 0);
                        java.util.Arrays.fill(hasValue, false);
                    }
                    accumulate(top);
                    if (top.next()) {
                        queue.updateTop();
                    } else {
                        queue.pop();
                    }
                }
            }

            private void accumulate(Stream stream) throws IOException {
                for (int f = 0; f < spec.fields().size(); f++) {
                    SortedNumericDocValues dv = stream.values[f];
                    if (dv.advanceExact(stream.doc) == false) {
                        continue;
                    }
                    if (dv.docValueCount() != 1) {
                        throw new IllegalStateException(
                            "reduced field [" + spec.fields().get(f).name() + "] must be single valued, doc [" + stream.doc + "]"
                        );
                    }
                    long value = dv.nextValue();
                    hasValue[f] = true;
                    switch (spec.fields().get(f).type()) {
                        case DOUBLE -> doubleSums[f] += NumericUtils.sortableLongToDouble(value);
                        case INT, LONG -> longSums[f] += value;
                        default -> throw new IllegalArgumentException("unsupported type");
                    }
                }
            }

            private long[] finish(boolean[] hasValueOut) {
                for (int f = 0; f < totals.length; f++) {
                    hasValueOut[f] = hasValue[f];
                    totals[f] = switch (spec.fields().get(f).type()) {
                        case DOUBLE -> NumericUtils.doubleToSortableLong(doubleSums[f]);
                        case INT, LONG -> longSums[f];
                        default -> throw new IllegalArgumentException("unsupported type");
                    };
                }
                return totals;
            }
        }
    }
}
