package org.apache.lucene.index;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.Unicode;
import org.elasticsearch.common.bloom.BloomFilter;
import org.elasticsearch.index.cache.bloom.BloomCache;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;
import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/* Tracks the stream of {@link BuffereDeletes}.
 * When DocumensWriter flushes, its buffered
 * deletes are appended to this stream.  We later
 * apply these deletes (resolve them to the actual
 * docIDs, per segment) when a merge is started
 * (only to the to-be-merged segments).  We
 * also apply to all segments when NRT reader is pulled,
 * commit/close is called, or when too many deletes are
 * buffered and must be flushed (by RAM usage or by count).
 *
 * Each packet is assigned a generation, and each flushed or
 * merged segment is also assigned a generation, so we can
 * track which BufferedDeletes packets to apply to any given
 * segment. */

// LUCENE MONITOR: We copied this class from Lucene, effectively overriding it with our implementation
// if it comes first in the classpath, allowing for faster apply deletes based on terms
class BufferedDeletesStream implements XIndexWriter.XBufferedDeletesStream {

    // TODO: maybe linked list?
    private final List<FrozenBufferedDeletes> deletes = new ArrayList<FrozenBufferedDeletes>();

    // Starts at 1 so that SegmentInfos that have never had
    // deletes applied (whose bufferedDelGen defaults to 0)
    // will be correct:
    private long nextGen = 1;

    // used only by assert
    private Term lastDeleteTerm;

    private PrintStream infoStream;
    private final AtomicLong bytesUsed = new AtomicLong();
    private final AtomicInteger numTerms = new AtomicInteger();
    private final int messageID;

    private BloomCache bloomCache;

    public BufferedDeletesStream(int messageID) {
        this.messageID = messageID;
    }

    private synchronized void message(String message) {
        if (infoStream != null) {
            infoStream.println("BD " + messageID + " [" + new Date() + "; " + Thread.currentThread().getName() + "]: " + message);
        }
    }

    public synchronized void setInfoStream(PrintStream infoStream) {
        this.infoStream = infoStream;
    }

    public void setBloomCache(BloomCache bloomCache) {
        this.bloomCache = bloomCache;
    }

    // Appends a new packet of buffered deletes to the stream,
    // setting its generation:
    public synchronized void push(FrozenBufferedDeletes packet) {
        assert packet.any();
        assert checkDeleteStats();
        assert packet.gen < nextGen;
        deletes.add(packet);
        numTerms.addAndGet(packet.numTermDeletes);
        bytesUsed.addAndGet(packet.bytesUsed);
        if (infoStream != null) {
            message("push deletes " + packet + " delGen=" + packet.gen + " packetCount=" + deletes.size());
        }
        assert checkDeleteStats();
    }

    public synchronized void clear() {
        deletes.clear();
        nextGen = 1;
        numTerms.set(0);
        bytesUsed.set(0);
    }

    public boolean any() {
        return bytesUsed.get() != 0;
    }

    public int numTerms() {
        return numTerms.get();
    }

    public long bytesUsed() {
        return bytesUsed.get();
    }

    public static class ApplyDeletesResult {
        // True if any actual deletes took place:
        public final boolean anyDeletes;

        // Current gen, for the merged segment:
        public final long gen;

        // If non-null, contains segments that are 100% deleted
        public final List<SegmentInfo> allDeleted;

        ApplyDeletesResult(boolean anyDeletes, long gen, List<SegmentInfo> allDeleted) {
            this.anyDeletes = anyDeletes;
            this.gen = gen;
            this.allDeleted = allDeleted;
        }
    }

    // Sorts SegmentInfos from smallest to biggest bufferedDelGen:
    private static final Comparator<SegmentInfo> sortByDelGen = new Comparator<SegmentInfo>() {
        // @Override -- not until Java 1.6
        public int compare(SegmentInfo si1, SegmentInfo si2) {
            final long cmp = si1.getBufferedDeletesGen() - si2.getBufferedDeletesGen();
            if (cmp > 0) {
                return 1;
            } else if (cmp < 0) {
                return -1;
            } else {
                return 0;
            }
        }
    };

    /**
     * Resolves the buffered deleted Term/Query/docIDs, into
     * actual deleted docIDs in the deletedDocs BitVector for
     * each SegmentReader.
     */
    public synchronized ApplyDeletesResult applyDeletes(IndexWriter.ReaderPool readerPool, List<SegmentInfo> infos) throws IOException {
        final long t0 = System.currentTimeMillis();

        if (infos.size() == 0) {
            return new ApplyDeletesResult(false, nextGen++, null);
        }

        assert checkDeleteStats();

        if (!any()) {
            message("applyDeletes: no deletes; skipping");
            return new ApplyDeletesResult(false, nextGen++, null);
        }

        if (infoStream != null) {
            message("applyDeletes: infos=" + infos + " packetCount=" + deletes.size());
        }

        List<SegmentInfo> infos2 = new ArrayList<SegmentInfo>();
        infos2.addAll(infos);
        Collections.sort(infos2, sortByDelGen);

        CoalescedDeletes coalescedDeletes = null;
        boolean anyNewDeletes = false;

        int infosIDX = infos2.size() - 1;
        int delIDX = deletes.size() - 1;

        List<SegmentInfo> allDeleted = null;

        while (infosIDX >= 0) {
            //System.out.println("BD: cycle delIDX=" + delIDX + " infoIDX=" + infosIDX);

            final FrozenBufferedDeletes packet = delIDX >= 0 ? deletes.get(delIDX) : null;
            final SegmentInfo info = infos2.get(infosIDX);
            final long segGen = info.getBufferedDeletesGen();

            if (packet != null && segGen < packet.gen) {
                //System.out.println("  coalesce");
                if (coalescedDeletes == null) {
                    coalescedDeletes = new CoalescedDeletes();
                }
                coalescedDeletes.update(packet);
                delIDX--;
            } else if (packet != null && segGen == packet.gen) {
                //System.out.println("  eq");

                // Lock order: IW -> BD -> RP
                assert readerPool.infoIsLive(info);
                SegmentReader reader = readerPool.get(info, false);
                int delCount = 0;
                final boolean segAllDeletes;
                try {
                    if (coalescedDeletes != null) {
                        //System.out.println("    del coalesced");
                        delCount += applyTermDeletes(coalescedDeletes.termsIterable(), reader);
                        delCount += applyQueryDeletes(coalescedDeletes.queriesIterable(), reader);
                    }
                    //System.out.println("    del exact");
                    // Don't delete by Term here; DocumentsWriter
                    // already did that on flush:
                    delCount += applyQueryDeletes(packet.queriesIterable(), reader);
                    segAllDeletes = reader.numDocs() == 0;
                } finally {
                    readerPool.release(reader);
                }
                anyNewDeletes |= delCount > 0;

                if (segAllDeletes) {
                    if (allDeleted == null) {
                        allDeleted = new ArrayList<SegmentInfo>();
                    }
                    allDeleted.add(info);
                }

                if (infoStream != null) {
                    message("seg=" + info + " segGen=" + segGen + " segDeletes=[" + packet + "]; coalesced deletes=[" + (coalescedDeletes == null ? "null" : coalescedDeletes) + "] delCount=" + delCount + (segAllDeletes ? " 100% deleted" : ""));
                }

                if (coalescedDeletes == null) {
                    coalescedDeletes = new CoalescedDeletes();
                }
                coalescedDeletes.update(packet);
                delIDX--;
                infosIDX--;
                info.setBufferedDeletesGen(nextGen);

            } else {
                //System.out.println("  gt");

                if (coalescedDeletes != null) {
                    // Lock order: IW -> BD -> RP
                    assert readerPool.infoIsLive(info);
                    SegmentReader reader = readerPool.get(info, false);
                    int delCount = 0;
                    final boolean segAllDeletes;
                    try {
                        delCount += applyTermDeletes(coalescedDeletes.termsIterable(), reader);
                        delCount += applyQueryDeletes(coalescedDeletes.queriesIterable(), reader);
                        segAllDeletes = reader.numDocs() == 0;
                    } finally {
                        readerPool.release(reader);
                    }
                    anyNewDeletes |= delCount > 0;

                    if (segAllDeletes) {
                        if (allDeleted == null) {
                            allDeleted = new ArrayList<SegmentInfo>();
                        }
                        allDeleted.add(info);
                    }

                    if (infoStream != null) {
                        message("seg=" + info + " segGen=" + segGen + " coalesced deletes=[" + (coalescedDeletes == null ? "null" : coalescedDeletes) + "] delCount=" + delCount + (segAllDeletes ? " 100% deleted" : ""));
                    }
                }
                info.setBufferedDeletesGen(nextGen);

                infosIDX--;
            }
        }

        assert checkDeleteStats();
        if (infoStream != null) {
            message("applyDeletes took " + (System.currentTimeMillis() - t0) + " msec");
        }
        // assert infos != segmentInfos || !any() : "infos=" + infos + " segmentInfos=" + segmentInfos + " any=" + any;

        return new ApplyDeletesResult(anyNewDeletes, nextGen++, allDeleted);
    }

    public synchronized long getNextGen() {
        return nextGen++;
    }

    // Lock order IW -> BD
    /* Removes any BufferedDeletes that we no longer need to
 * store because all segments in the index have had the
 * deletes applied. */
    public synchronized void prune(SegmentInfos segmentInfos) {
        assert checkDeleteStats();
        long minGen = Long.MAX_VALUE;
        for (SegmentInfo info : segmentInfos) {
            minGen = Math.min(info.getBufferedDeletesGen(), minGen);
        }

        if (infoStream != null) {
            message("prune sis=" + segmentInfos + " minGen=" + minGen + " packetCount=" + deletes.size());
        }

        final int limit = deletes.size();
        for (int delIDX = 0; delIDX < limit; delIDX++) {
            if (deletes.get(delIDX).gen >= minGen) {
                prune(delIDX);
                assert checkDeleteStats();
                return;
            }
        }

        // All deletes pruned
        prune(limit);
        assert !any();
        assert checkDeleteStats();
    }

    private synchronized void prune(int count) {
        if (count > 0) {
            if (infoStream != null) {
                message("pruneDeletes: prune " + count + " packets; " + (deletes.size() - count) + " packets remain");
            }
            for (int delIDX = 0; delIDX < count; delIDX++) {
                final FrozenBufferedDeletes packet = deletes.get(delIDX);
                numTerms.addAndGet(-packet.numTermDeletes);
                assert numTerms.get() >= 0;
                bytesUsed.addAndGet(-packet.bytesUsed);
                assert bytesUsed.get() >= 0;
            }
            deletes.subList(0, count).clear();
        }
    }

    // ES CHANGE: Add bloom filter usage
    // Delete by Term
    private synchronized long applyTermDeletes(Iterable<Term> termsIter, SegmentReader reader) throws IOException {
        long delCount = 0;

        assert checkDeleteTerm(null);

        BloomFilter filter = bloomCache == null ? BloomFilter.NONE : bloomCache.filter(reader, UidFieldMapper.NAME, true);
        UnicodeUtil.UTF8Result utf8 = new UnicodeUtil.UTF8Result();

        TermDocs docs = reader.termDocs();

        for (Term term : termsIter) {

            if (term.field() == UidFieldMapper.NAME) {
                Unicode.fromStringAsUtf8(term.text(), utf8);
                if (!filter.isPresent(utf8.result, 0, utf8.length)) {
                    continue;
                }
            }
            if (docs == null) {
                docs = reader.termDocs();
            }

            // Since we visit terms sorted, we gain performance
            // by re-using the same TermsEnum and seeking only
            // forwards
            assert checkDeleteTerm(term);
            docs.seek(term);

            while (docs.next()) {
                final int docID = docs.doc();
                reader.deleteDocument(docID);
                // TODO: we could/should change
                // reader.deleteDocument to return boolean
                // true if it did in fact delete, because here
                // we could be deleting an already-deleted doc
                // which makes this an upper bound:
                delCount++;
            }
        }

        return delCount;
    }

    public static class QueryAndLimit {
        public final Query query;
        public final int limit;

        public QueryAndLimit(Query query, int limit) {
            this.query = query;
            this.limit = limit;
        }
    }

    // Delete by query
    private synchronized long applyQueryDeletes(Iterable<QueryAndLimit> queriesIter, SegmentReader reader) throws IOException {
        long delCount = 0;

        for (QueryAndLimit ent : queriesIter) {
            Query query = ent.query;
            int limit = ent.limit;
            final DocIdSet docs = new QueryWrapperFilter(query).getDocIdSet(reader);
            if (docs != null) {
                final DocIdSetIterator it = docs.iterator();
                if (it != null) {
                    while (true) {
                        int doc = it.nextDoc();
                        if (doc >= limit)
                            break;

                        reader.deleteDocument(doc);
                        // TODO: we could/should change
                        // reader.deleteDocument to return boolean
                        // true if it did in fact delete, because here
                        // we could be deleting an already-deleted doc
                        // which makes this an upper bound:
                        delCount++;
                    }
                }
            }
        }

        return delCount;
    }

    // used only by assert
    private boolean checkDeleteTerm(Term term) {
        if (term != null) {
            assert lastDeleteTerm == null || term.compareTo(lastDeleteTerm) > 0 : "lastTerm=" + lastDeleteTerm + " vs term=" + term;
        }
        // TODO: we re-use term now in our merged iterable, but we shouldn't clone, instead copy for this assert
        lastDeleteTerm = term == null ? null : new Term(term.field(), term.text());
        return true;
    }

    // only for assert
    private boolean checkDeleteStats() {
        int numTerms2 = 0;
        long bytesUsed2 = 0;
        for (FrozenBufferedDeletes packet : deletes) {
            numTerms2 += packet.numTermDeletes;
            bytesUsed2 += packet.bytesUsed;
        }
        assert numTerms2 == numTerms.get() : "numTerms2=" + numTerms2 + " vs " + numTerms.get();
        assert bytesUsed2 == bytesUsed.get() : "bytesUsed2=" + bytesUsed2 + " vs " + bytesUsed;
        return true;
    }
}
