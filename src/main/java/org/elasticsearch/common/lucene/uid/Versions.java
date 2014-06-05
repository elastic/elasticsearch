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

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexReader.ReaderClosedListener;
import org.apache.lucene.util.CloseableThreadLocal;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

/** Utility class to resolve the Lucene doc ID and version for a given uid. */
public class Versions {

    public static final long MATCH_ANY = -3L; // Version was not specified by the user
    // TODO: can we remove this now?  rolling upgrades only need to handle prev (not older than that) version...?
    // the value for MATCH_ANY before ES 1.2.0 - will be removed
    public static final long MATCH_ANY_PRE_1_2_0 = 0L;
    public static final long NOT_FOUND = -1L;
    public static final long NOT_SET = -2L;

    // TODO: is there somewhere else we can store these?
    private static final ConcurrentMap<IndexReader,CloseableThreadLocal<PerThreadIDAndVersionLookup>> lookupStates = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

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
            ctl = new CloseableThreadLocal<PerThreadIDAndVersionLookup>();
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

    public static void writeVersion(long version, StreamOutput out) throws IOException {
        if (out.getVersion().before(Version.V_1_2_0) && version == MATCH_ANY) {
            // we have to send out a value the node will understand
            version = MATCH_ANY_PRE_1_2_0;
        }
        out.writeLong(version);
    }

    public static long readVersion(StreamInput in) throws IOException {
        long version = in.readLong();
        if (in.getVersion().before(Version.V_1_2_0) && version == MATCH_ANY_PRE_1_2_0) {
            version = MATCH_ANY;
        }
        return version;
    }

    public static void writeVersionWithVLongForBW(long version, StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_1_2_0)) {
            out.writeLong(version);
            return;
        }

        if (version == MATCH_ANY) {
            // we have to send out a value the node will understand
            version = MATCH_ANY_PRE_1_2_0;
        }
        out.writeVLong(version);
    }

    public static long readVersionWithVLongForBW(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_1_2_0)) {
            return in.readLong();
        } else {
            long version = in.readVLong();
            if (version == MATCH_ANY_PRE_1_2_0) {
                return MATCH_ANY;
            }
            return version;
        }
    }

    private Versions() {
    }

    /** Wraps an {@link AtomicReaderContext}, a doc ID <b>relative to the context doc base</b> and a version. */
    public static class DocIdAndVersion {
        public final int docId;
        public final long version;
        public final AtomicReaderContext context;

        public DocIdAndVersion(int docId, long version, AtomicReaderContext context) {
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
