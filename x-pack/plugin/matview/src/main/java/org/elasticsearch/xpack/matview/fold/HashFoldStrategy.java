/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.matview.fold;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Folds without relying on any document order: while readers are wrapped, every document's key is encoded and
 * registered in a hash, the first document of a group (in merge order) is its representative and later ones are
 * hidden. The reduced values are computed afterwards by a {@link Reducer} over all readers and kept per group.
 * Memory grows with the number of groups in the merge.
 */
public final class HashFoldStrategy implements FoldStrategy {

    private final Reducer reducer;

    public HashFoldStrategy(Reducer reducer) {
        this.reducer = reducer;
    }

    @Override
    public MergeFold newMergeFold(FoldSpec spec, @Nullable FoldGuard guard) {
        return new HashMergeFold(spec, reducer, guard);
    }

    static final class HashMergeFold implements MergeFold {
        private final FoldSpec spec;
        private final Reducer reducer;
        @Nullable
        private final FoldGuard guard;
        /** Reducer inputs: the merge readers narrowed to their foldable live documents. */
        private final List<CodecReader> readers = new ArrayList<>();
        private final BytesRefHash seenKeys = new BytesRefHash(1024, BigArrays.NON_RECYCLING_INSTANCE);
        private final Reducer.KeyIndex keyIndex = new Reducer.KeyIndex() {
            @Override
            public long find(BytesRef key) {
                return seenKeys.find(key);
            }

            @Override
            public long size() {
                return seenKeys.size();
            }
        };
        private Reducer.Result result;

        HashMergeFold(FoldSpec spec, Reducer reducer, @Nullable FoldGuard guard) {
            this.spec = spec;
            this.reducer = reducer;
            this.guard = guard;
        }

        @Override
        public synchronized CodecReader wrap(CodecReader reader) throws IOException {
            KeyReaders.Component[] keys = KeyReaders.open(reader, spec);
            if (keys == null) {
                return reader; // a key field is missing entirely: no keyed documents, nothing to fold
            }
            Bits live = reader.getLiveDocs();
            Bits foldable = guard == null ? null : guard.foldable(reader);
            int maxDoc = reader.maxDoc();
            int[] keyIdByDoc = new int[maxDoc];
            Arrays.fill(keyIdByDoc, -1);
            long[] longs = new long[keys.length];
            BytesRef[] bytes = new BytesRef[keys.length];
            KeyEncoder encoder = new KeyEncoder();
            FixedBitSet foldedLive = null;
            int hidden = 0;
            boolean keyed = false;
            for (int doc = 0; doc < maxDoc; doc++) {
                if (live != null && live.get(doc) == false) {
                    continue;
                }
                if (foldable != null && foldable.get(doc) == false) {
                    continue; // still part of the retained history: keeps its own identity and values
                }
                if (KeyReaders.read(keys, doc, longs, bytes) == false) {
                    continue; // some key component is missing: the document belongs to no group
                }
                keyed = true;
                encoder.reset();
                for (int i = 0; i < keys.length; i++) {
                    if (keys[i].longs() != null) {
                        encoder.addLong(longs[i]);
                    } else {
                        encoder.addBytes(bytes[i]);
                    }
                }
                long id = seenKeys.add(encoder.get());
                if (id < 0) {
                    if (foldedLive == null) {
                        foldedLive = copyLiveDocs(live, maxDoc);
                    }
                    foldedLive.clear(doc);
                    hidden++;
                    keyIdByDoc[doc] = Math.toIntExact(-1 - id);
                } else {
                    keyIdByDoc[doc] = Math.toIntExact(id);
                }
            }
            if (keyed == false) {
                return reader;
            }
            // Wrap even when nothing is hidden in this reader: a later reader may contribute to one of its groups,
            // in which case this reader's representative has to serve the combined value.
            readers.add(reducerInput(reader, live, foldable));
            return new FoldingCodecReader(reader, spec, new HashReaderFold(foldedLive, foldable, reader.numDocs() - hidden, keyIdByDoc));
        }

        private final class HashReaderFold implements ReaderFold {
            @Nullable
            private final FixedBitSet foldedLive;
            @Nullable
            private final Bits foldable;
            private final int numDocs;
            private final int[] keyIdByDoc;

            HashReaderFold(@Nullable FixedBitSet foldedLive, @Nullable Bits foldable, int numDocs, int[] keyIdByDoc) {
                this.foldedLive = foldedLive;
                this.foldable = foldable;
                this.numDocs = numDocs;
                this.keyIdByDoc = keyIdByDoc;
            }

            @Override
            public Bits foldedLive() {
                return foldedLive;
            }

            @Override
            public int numDocs() {
                return numDocs;
            }

            @Override
            public long reducedValue(int doc, int field, long raw) throws IOException {
                if (foldable != null && foldable.get(doc) == false) {
                    return raw;
                }
                int keyId = keyIdByDoc[doc];
                if (keyId < 0) {
                    return raw;
                }
                Reducer.Result result = result();
                int row = result.rowOf(keyId);
                if (row < 0 || result.hasValue(field, row) == false) {
                    return raw;
                }
                return result.value(field, row);
            }
        }

        /** The reader as the reducer sees it: only live documents that are allowed to fold. */
        private static CodecReader reducerInput(CodecReader reader, @Nullable Bits live, @Nullable Bits foldable) {
            if (foldable == null) {
                return reader;
            }
            int maxDoc = reader.maxDoc();
            FixedBitSet liveAndFoldable = new FixedBitSet(maxDoc);
            int count = 0;
            for (int doc = 0; doc < maxDoc; doc++) {
                if ((live == null || live.get(doc)) && foldable.get(doc)) {
                    liveAndFoldable.set(doc);
                    count++;
                }
            }
            return new NarrowedLiveDocsReader(reader, liveAndFoldable, count);
        }

        /** Runs the reducer on first use, by which time all readers are registered. */
        synchronized Reducer.Result result() throws IOException {
            if (result == null) {
                result = reducer.reduce(readers, keyIndex);
            }
            return result;
        }

        @Override
        public synchronized void close() {
            Releasables.close(result, seenKeys);
            result = null;
        }

        private static FixedBitSet copyLiveDocs(@Nullable Bits live, int maxDoc) {
            FixedBitSet bits = new FixedBitSet(maxDoc);
            if (live == null) {
                bits.set(0, maxDoc);
            } else {
                for (int i = 0; i < maxDoc; i++) {
                    if (live.get(i)) {
                        bits.set(i);
                    }
                }
            }
            return bits;
        }
    }

    /** Same as Lucene's package-private {@code FilterCodecReader#wrapLiveDocs}: a reader with fewer live documents. */
    private static final class NarrowedLiveDocsReader extends FilterCodecReader {
        private final Bits liveDocs;
        private final int numDocs;

        NarrowedLiveDocsReader(CodecReader in, Bits liveDocs, int numDocs) {
            super(in);
            this.liveDocs = liveDocs;
            this.numDocs = numDocs;
        }

        @Override
        public Bits getLiveDocs() {
            return liveDocs;
        }

        @Override
        public int numDocs() {
            return numDocs;
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return null;
        }
    }
}
