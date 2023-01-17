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
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Utility class to resolve the Lucene doc ID, version, seqNo and primaryTerms for a given uid. */
public final class VersionsAndSeqNoResolver {

    private static final PerThreadIDVersionAndSeqNoLookup[] EMPTY_LOOKUP = new PerThreadIDVersionAndSeqNoLookup[0];

    private static final ThreadLocal<PerThreadVersionsAndSeqNoResolver> resolversByThread = ThreadLocal.withInitial(
        PerThreadVersionsAndSeqNoResolver::new
    );

    private static class PerThreadVersionsAndSeqNoResolver {
        // We cache on the top level.
        // This means cache entries have a shorter lifetime, maybe as low as 1s with the
        // default refresh interval and a steady indexing rate, but on the other hand it
        // proved to be cheaper than having to perform a CHM and a TL get for every segment.
        // See https://github.com/elastic/elasticsearch/pull/19856.

        private final Map<IndexReader.CacheKey, PerThreadIDVersionAndSeqNoLookup[]> lookupsByReader = ConcurrentCollections
            .newConcurrentMap();

        private void removeLookup(IndexReader.CacheKey cacheKey) {
            lookupsByReader.remove(cacheKey); // not called on the owning thread, hence the need for a CHM.
        }

        private PerThreadIDVersionAndSeqNoLookup[] getLookupState(
            IndexReader.CacheHelper cacheHelper,
            List<LeafReaderContext> leaves,
            String uidField
        ) throws IOException {
            assert leaves.isEmpty() == false;
            final var lookupState = lookupsByReader.computeIfAbsent(
                cacheHelper.getKey(),
                ignored -> new PerThreadIDVersionAndSeqNoLookup[leaves.size()]
            );
            if (lookupState[0] == null) {
                for (final var leaf : leaves) {
                    assert lookupState[leaf.ord] == null;
                    lookupState[leaf.ord] = new PerThreadIDVersionAndSeqNoLookup(leaf.reader(), uidField);
                }
                cacheHelper.addClosedListener(this::removeLookup);
            }
            assert lookupState.length == leaves.size();
            assert Arrays.stream(lookupState).allMatch(leafLookup -> leafLookup != null && Objects.equals(leafLookup.uidField, uidField));
            return lookupState;
        }
    }

    // exposed for tests
    static int getCurrentThreadCacheSize() {
        return resolversByThread.get().lookupsByReader.size();
    }

    private static PerThreadIDVersionAndSeqNoLookup[] getLookupState(IndexReader reader, String uidField) throws IOException {
        final var leaves = reader.leaves();
        if (leaves.isEmpty()) {
            return EMPTY_LOOKUP;
        }
        return resolversByThread.get().getLookupState(reader.getReaderCacheHelper(), leaves, uidField);
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
