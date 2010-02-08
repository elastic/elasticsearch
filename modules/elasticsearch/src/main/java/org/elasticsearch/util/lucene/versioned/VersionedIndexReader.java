/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.util.lucene.versioned;

import org.apache.lucene.index.*;
import org.elasticsearch.util.concurrent.ThreadSafe;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
@ThreadSafe
public class VersionedIndexReader extends FilterIndexReader {

    protected final int version;

    protected final VersionedMap versionedMap;

    public VersionedIndexReader(IndexReader in, int version, VersionedMap versionedMap) {
        super(in);
        this.version = version;
        this.versionedMap = versionedMap;
    }

    @Override public TermDocs termDocs() throws IOException {
        return new VersionedTermDocs(in.termDocs());
    }

    @Override public TermDocs termDocs(Term term) throws IOException {
        return new VersionedTermDocs(in.termDocs(term));
    }

    @Override public TermPositions termPositions() throws IOException {
        return new VersionedTermPositions(in.termPositions());
    }

    @Override public TermPositions termPositions(Term term) throws IOException {
        return new VersionedTermPositions(in.termPositions(term));
    }


    private class VersionedTermDocs extends FilterTermDocs {

        public VersionedTermDocs(TermDocs in) {
            super(in);
        }

        public boolean next() throws IOException {
            while (in.next()) {
                if (versionedMap.beforeVersion(in.doc(), version)) return true;
            }
            return false;
        }

        public int read(final int[] docs, final int[] freqs) throws IOException {
            int i = 0;
            while (i < docs.length) {
                if (!in.next()) return i;

                int doc = in.doc();
                if (versionedMap.beforeVersion(doc, version)) {
                    docs[i] = doc;
                    freqs[i] = in.freq();
                    i++;
                }
            }
            return i;
        }

        public boolean skipTo(int i) throws IOException {
            if (!in.skipTo(i)) return false;
            if (versionedMap.beforeVersion(in.doc(), version)) return true;

            return next();
        }
    }

    private class VersionedTermPositions extends VersionedTermDocs implements TermPositions {
        final TermPositions _tp;

        public VersionedTermPositions(TermPositions in) {
            super(in);
            _tp = in;
        }

        public int nextPosition() throws IOException {
            return _tp.nextPosition();
        }

        public int getPayloadLength() {
            return _tp.getPayloadLength();
        }

        public byte[] getPayload(byte[] data, int offset) throws IOException {
            return _tp.getPayload(data, offset);
        }

        public boolean isPayloadAvailable() {
            return _tp.isPayloadAvailable();
        }
    }

}
