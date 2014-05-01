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

import org.apache.lucene.index.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.mapper.internal.VersionFieldMapper;

import java.io.IOException;
import java.util.List;

/** Utility class to resolve the Lucene doc ID and version for a given uid. */
public class Versions {

    public static final long MATCH_ANY = -3L; // Version was not specified by the user
    // the value for MATCH_ANY before ES 1.2.0 - will be removed
    public static final long MATCH_ANY_PRE_1_2_0 = 0L;
    public static final long NOT_FOUND = -1L;
    public static final long NOT_SET = -2L;

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
        // iterate backwards to optimize for the frequently updated documents
        // which are likely to be in the last segments
        final List<AtomicReaderContext> leaves = reader.leaves();
        for (int i = leaves.size() - 1; i >= 0; --i) {
            final DocIdAndVersion docIdAndVersion = loadDocIdAndVersion(leaves.get(i), term);
            if (docIdAndVersion != null) {
                assert docIdAndVersion.version != NOT_FOUND;
                return docIdAndVersion;
            }
        }
        return null;
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

    /** Same as {@link #loadDocIdAndVersion(IndexReader, Term)} but operates directly on a reader context. */
    public static DocIdAndVersion loadDocIdAndVersion(AtomicReaderContext readerContext, Term term) throws IOException {
        assert term.field().equals(UidFieldMapper.NAME);
        final AtomicReader reader = readerContext.reader();
        final Bits liveDocs = reader.getLiveDocs();
        final Terms terms = reader.terms(UidFieldMapper.NAME);
        assert terms != null : "All segments must have a _uid field, but " + reader + " doesn't";
        final TermsEnum termsEnum = terms.iterator(null);
        if (!termsEnum.seekExact(term.bytes())) {
            return null;
        }

        // Versions are stored as doc values...
        final NumericDocValues versions = reader.getNumericDocValues(VersionFieldMapper.NAME);
        if (versions != null || !terms.hasPayloads()) {
            // only the last doc that matches the _uid is interesting here: if it is deleted, then there is
            // no match otherwise previous docs are necessarily either deleted or nested docs
            final DocsEnum docs = termsEnum.docs(null, null);
            int docID = DocsEnum.NO_MORE_DOCS;
            for (int d = docs.nextDoc(); d != DocsEnum.NO_MORE_DOCS; d = docs.nextDoc()) {
                docID = d;
            }
            assert docID != DocsEnum.NO_MORE_DOCS; // would mean that the term exists but has no match at all
            if (liveDocs != null && !liveDocs.get(docID)) {
                return null;
            } else if (versions != null) {
                return new DocIdAndVersion(docID, versions.get(docID), readerContext);
            } else {
                // _uid found, but no doc values and no payloads
                return new DocIdAndVersion(docID, NOT_SET, readerContext);
            }
        }

        // ... but used to be stored as payloads
        final DocsAndPositionsEnum dpe = termsEnum.docsAndPositions(liveDocs, null, DocsAndPositionsEnum.FLAG_PAYLOADS);
        assert dpe != null; // terms has payloads
        int docID = DocsEnum.NO_MORE_DOCS;
        for (int d = dpe.nextDoc(); d != DocsEnum.NO_MORE_DOCS; d = dpe.nextDoc()) {
            docID = d;
            dpe.nextPosition();
            final BytesRef payload = dpe.getPayload();
            if (payload != null && payload.length == 8) {
                return new DocIdAndVersion(d, Numbers.bytesToLong(payload), readerContext);
            }
        }

        if (docID == DocsEnum.NO_MORE_DOCS) {
            return null;
        } else {
            return new DocIdAndVersion(docID, NOT_SET, readerContext);
        }
    }

}
