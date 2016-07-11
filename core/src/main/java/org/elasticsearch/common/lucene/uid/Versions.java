/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.lucene.uid;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReader.CoreClosedListener;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.CloseableThreadLocal;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

/** Utility class to resolve the Lucene doc ID and version for a given uid. */
public class Versions {

    /** used to indicate the write operation should succeed regardless of current version **/
    public static final long MATCH_ANY = -3L;

    /** indicates that the current document was not found in lucene and in the version map */
    public static final long NOT_FOUND = -1L;

    // -2 was used for docs that can be found in the index but do not have a version

    /**
     * used to indicate that the write operation should be executed if the document is currently deleted
     * i.e., not found in the index and/or found as deleted (with version) in the version map
     */
    public static final long MATCH_DELETED = -4L;

    // TODO: is there somewhere else we can store these?
    static final ConcurrentMap<Object, CloseableThreadLocal<PerThreadIDAndVersionLookup>> lookupStates = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    // Evict this reader from lookupStates once it's closed:
    private static final CoreClosedListener removeLookupState = new CoreClosedListener() {
        @Override
        public void onClose(Object key) {
            CloseableThreadLocal<PerThreadIDAndVersionLookup> ctl = lookupStates.remove(key);
            if (ctl != null) {
                ctl.close();
            }
        }
    };

    private static PerThreadIDAndVersionLookup getLookupState(LeafReader reader) throws IOException {
        Object key = reader.getCoreCacheKey();
        CloseableThreadLocal<PerThreadIDAndVersionLookup> ctl = lookupStates.get(key);
        if (ctl == null) {
            // First time we are seeing this reader's core; make a
            // new CTL:
            ctl = new CloseableThreadLocal<>();
            CloseableThreadLocal<PerThreadIDAndVersionLookup> other = lookupStates.putIfAbsent(key, ctl);
            if (other == null) {
                // Our CTL won, we must remove it when the
                // core is closed:
                reader.addCoreClosedListener(removeLookupState);
            } else {
                // Another thread beat us to it: just use
                // their CTL:
                ctl = other;
            }
        }

        PerThreadIDAndVersionLookup lookupState = ctl.get();
        if (lookupState == null) {
            lookupState = new PerThreadIDAndVersionLookup(reader);
            ctl.set(lookupState);
        }

        return lookupState;
    }

    private Versions() {
    }

    /** Wraps an {@link LeafReaderContext}, a doc ID <b>relative to the context doc base</b> and a version. */
    public static class DocIdAndVersion {
        public final int docId;
        public final long version;
        public final LeafReaderContext context;

        public DocIdAndVersion(int docId, long version, LeafReaderContext context) {
            this.docId = docId;
            this.version = version;
            this.context = context;
        }
    }

    /**
     * Load the internal doc ID and version for the uid from the reader, returning<ul>
     * <li>null if the uid wasn't found,
     * <li>a doc ID and a version otherwise
     * </ul>
     */
    public static DocIdAndVersion loadDocIdAndVersion(IndexReader reader, Term term) throws IOException {
        assert term.field().equals(UidFieldMapper.NAME);
        List<LeafReaderContext> leaves = reader.leaves();
        if (leaves.isEmpty()) {
            return null;
        }
        // iterate backwards to optimize for the frequently updated documents
        // which are likely to be in the last segments
        for (int i = leaves.size() - 1; i >= 0; i--) {
            LeafReaderContext context = leaves.get(i);
            LeafReader leaf = context.reader();
            PerThreadIDAndVersionLookup lookup = getLookupState(leaf);
            DocIdAndVersion result = lookup.lookup(term.bytes(), leaf.getLiveDocs(), context);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    /**
     * Load the version for the uid from the reader, returning<ul>
     * <li>{@link #NOT_FOUND} if no matching doc exists,
     * <li>the version associated with the provided uid otherwise
     * </ul>
     */
    public static long loadVersion(IndexReader reader, Term term) throws IOException {
        final DocIdAndVersion docIdAndVersion = loadDocIdAndVersion(reader, term);
        return docIdAndVersion == null ? NOT_FOUND : docIdAndVersion.version;
    }
}
