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
import org.apache.lucene.index.IndexReader.ReaderClosedListener;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.CloseableThreadLocal;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

/** Utility class to resolve the Lucene doc ID and version for a given uid. */
public class Versions {

    public static final long MATCH_ANY = -3L; // Version was not specified by the user
    public static final long NOT_FOUND = -1L;
    public static final long NOT_SET = -2L;

    // TODO: is there somewhere else we can store these?
    private static final ConcurrentMap<IndexReader, CloseableThreadLocal<PerThreadIDAndVersionLookup>> lookupStates = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    // Evict this reader from lookupStates once it's closed:
    private static final ReaderClosedListener removeLookupState = new ReaderClosedListener() {
        @Override
        public void onClose(IndexReader reader) {
            CloseableThreadLocal<PerThreadIDAndVersionLookup> ctl = lookupStates.remove(reader);
            if (ctl != null) {
                ctl.close();
            }
        }
    };

    private static PerThreadIDAndVersionLookup getLookupState(IndexReader reader) throws IOException {
        CloseableThreadLocal<PerThreadIDAndVersionLookup> ctl = lookupStates.get(reader);
        if (ctl == null) {
            // First time we are seeing this reader; make a
            // new CTL:
            ctl = new CloseableThreadLocal<>();
            CloseableThreadLocal<PerThreadIDAndVersionLookup> other = lookupStates.putIfAbsent(reader, ctl);
            if (other == null) {
                // Our CTL won, we must remove it when the
                // reader is closed:
                reader.addReaderClosedListener(removeLookupState);
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
     * <li>a doc ID and a version otherwise, the version being potentially set to {@link #NOT_SET} if the uid has no associated version
     * </ul>
     */
    public static DocIdAndVersion loadDocIdAndVersion(IndexReader reader, Term term) throws IOException {
        assert term.field().equals(UidFieldMapper.NAME);
        return getLookupState(reader).lookup(term.bytes());
    }

    /**
     * Load the version for the uid from the reader, returning<ul>
     * <li>{@link #NOT_FOUND} if no matching doc exists,
     * <li>{@link #NOT_SET} if no version is available,
     * <li>the version associated with the provided uid otherwise
     * </ul>
     */
    public static long loadVersion(IndexReader reader, Term term) throws IOException {
        final DocIdAndVersion docIdAndVersion = loadDocIdAndVersion(reader, term);
        return docIdAndVersion == null ? NOT_FOUND : docIdAndVersion.version;
    }
}
