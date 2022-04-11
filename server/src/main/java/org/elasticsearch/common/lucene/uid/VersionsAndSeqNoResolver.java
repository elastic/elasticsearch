/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene.uid;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.CloseableThreadLocal;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

/** Utility class to resolve the Lucene doc ID, version, seqNo and primaryTerms for a given uid. */
public final class VersionsAndSeqNoResolver {

    static final ConcurrentMap<IndexReader.CacheKey, CloseableThreadLocal<PerThreadIDVersionAndSeqNoLookup[]>> lookupStates =
        ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    // Evict this reader from lookupStates once it's closed:
    private static final IndexReader.ClosedListener removeLookupState = key -> {
        CloseableThreadLocal<PerThreadIDVersionAndSeqNoLookup[]> ctl = lookupStates.remove(key);
        if (ctl != null) {
            ctl.close();
        }
    };

    private static PerThreadIDVersionAndSeqNoLookup[] getLookupState(IndexReader reader, String uidField) throws IOException {
        // We cache on the top level
        // This means cache entries have a shorter lifetime, maybe as low as 1s with the
        // default refresh interval and a steady indexing rate, but on the other hand it
        // proved to be cheaper than having to perform a CHM and a TL get for every segment.
        // See https://github.com/elastic/elasticsearch/pull/19856.
        IndexReader.CacheHelper cacheHelper = reader.getReaderCacheHelper();
        CloseableThreadLocal<PerThreadIDVersionAndSeqNoLookup[]> ctl = lookupStates.get(cacheHelper.getKey());
        if (ctl == null) {
            // First time we are seeing this reader's core; make a new CTL:
            ctl = new CloseableThreadLocal<>();
            CloseableThreadLocal<PerThreadIDVersionAndSeqNoLookup[]> other = lookupStates.putIfAbsent(cacheHelper.getKey(), ctl);
            if (other == null) {
                // Our CTL won, we must remove it when the reader is closed:
                cacheHelper.addClosedListener(removeLookupState);
            } else {
                // Another thread beat us to it: just use their CTL:
                ctl = other;
            }
        }

        PerThreadIDVersionAndSeqNoLookup[] lookupState = ctl.get();
        if (lookupState == null) {
            lookupState = new PerThreadIDVersionAndSeqNoLookup[reader.leaves().size()];
            for (LeafReaderContext leaf : reader.leaves()) {
                lookupState[leaf.ord] = new PerThreadIDVersionAndSeqNoLookup(leaf.reader(), uidField);
            }
            ctl.set(lookupState);
        }

        if (lookupState.length != reader.leaves().size()) {
            throw new AssertionError("Mismatched numbers of leaves: " + lookupState.length + " != " + reader.leaves().size());
        }

        if (lookupState.length > 0 && Objects.equals(lookupState[0].uidField, uidField) == false) {
            throw new AssertionError(
                "Index does not consistently use the same uid field: [" + uidField + "] != [" + lookupState[0].uidField + "]"
            );
        }

        return lookupState;
    }

    private VersionsAndSeqNoResolver() {}

    /** Wraps an {@link LeafReaderContext}, a doc ID <b>relative to the context doc base</b> and a version. */
    public static class DocIdAndVersion {
        public final int docId;
        public final long version;
        public final long seqNo;
        public final long primaryTerm;
        public final LeafReader reader;
        public final int docBase;

        public DocIdAndVersion(int docId, long version, long seqNo, long primaryTerm, LeafReader reader, int docBase) {
            this.docId = docId;
            this.version = version;
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
            this.reader = reader;
            this.docBase = docBase;
        }
    }

    /** Wraps an {@link LeafReaderContext}, a doc ID <b>relative to the context doc base</b> and a seqNo. */
    public static class DocIdAndSeqNo {
        public final int docId;
        public final long seqNo;
        public final LeafReaderContext context;

        DocIdAndSeqNo(int docId, long seqNo, LeafReaderContext context) {
            this.docId = docId;
            this.seqNo = seqNo;
            this.context = context;
        }
    }

    /**
     * Load the internal doc ID and version for the uid from the reader, returning<ul>
     * <li>null if the uid wasn't found,
     * <li>a doc ID and a version otherwise
     * </ul>
     */
    public static DocIdAndVersion loadDocIdAndVersion(IndexReader reader, Term term, boolean loadSeqNo) throws IOException {
        PerThreadIDVersionAndSeqNoLookup[] lookups = getLookupState(reader, term.field());
        List<LeafReaderContext> leaves = reader.leaves();
        // iterate backwards to optimize for the frequently updated documents
        // which are likely to be in the last segments
        for (int i = leaves.size() - 1; i >= 0; i--) {
            final LeafReaderContext leaf = leaves.get(i);
            PerThreadIDVersionAndSeqNoLookup lookup = lookups[leaf.ord];
            DocIdAndVersion result = lookup.lookupVersion(term.bytes(), loadSeqNo, leaf);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    public static DocIdAndVersion loadDocIdAndVersionUncached(IndexReader reader, Term term, boolean loadSeqNo) throws IOException {
        List<LeafReaderContext> leaves = reader.leaves();
        for (int i = leaves.size() - 1; i >= 0; i--) {
            final LeafReaderContext leaf = leaves.get(i);
            PerThreadIDVersionAndSeqNoLookup lookup = new PerThreadIDVersionAndSeqNoLookup(leaf.reader(), term.field(), false);
            DocIdAndVersion result = lookup.lookupVersion(term.bytes(), loadSeqNo, leaf);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    /**
     * Loads the internal docId and sequence number of the latest copy for a given uid from the provided reader.
     * The result is either null or the live and latest version of the given uid.
     */
    public static DocIdAndSeqNo loadDocIdAndSeqNo(IndexReader reader, Term term) throws IOException {
        final PerThreadIDVersionAndSeqNoLookup[] lookups = getLookupState(reader, term.field());
        final List<LeafReaderContext> leaves = reader.leaves();
        // iterate backwards to optimize for the frequently updated documents
        // which are likely to be in the last segments
        for (int i = leaves.size() - 1; i >= 0; i--) {
            final LeafReaderContext leaf = leaves.get(i);
            final PerThreadIDVersionAndSeqNoLookup lookup = lookups[leaf.ord];
            final DocIdAndSeqNo result = lookup.lookupSeqNo(term.bytes(), leaf);
            if (result != null) {
                return result;
            }
        }
        return null;
    }
}
