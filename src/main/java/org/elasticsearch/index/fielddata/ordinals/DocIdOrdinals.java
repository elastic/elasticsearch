/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.index.fielddata.ordinals;

import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Ordinals that effectively are single valued and map "one to one" to the
 * doc ids. Note, the docId is incremented by 1 to get the ordinal, since 0
 * denotes an empty value.
 */
public class DocIdOrdinals implements Ordinals {

    private final int numDocs;

    /**
     * Constructs a new doc id ordinals.
     */
    public DocIdOrdinals(int numDocs) {
        this.numDocs = numDocs;
    }

    @Override
    public long getMemorySizeInBytes() {
        return RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    }

    @Override
    public boolean isMultiValued() {
        return false;
    }

    @Override
    public int getNumDocs() {
        return numDocs;
    }

    @Override
    public long getNumOrds() {
        return numDocs;
    }

    @Override
    public long getMaxOrd() {
        return 1L + numDocs;
    }

    @Override
    public Ordinals.Docs ordinals() {
        return new Docs(this);
    }

    public static class Docs implements Ordinals.Docs {

        private final DocIdOrdinals parent;
        private final LongsRef longsScratch = new LongsRef(new long[1], 0, 1);
        private int docId = -1;
        private long currentOrdinal = -1;

        public Docs(DocIdOrdinals parent) {
            this.parent = parent;
        }

        @Override
        public Ordinals ordinals() {
            return parent;
        }

        @Override
        public int getNumDocs() {
            return parent.getNumDocs();
        }

        @Override
        public long getNumOrds() {
            return parent.getNumOrds();
        }

        @Override
        public long getMaxOrd() {
            return parent.getMaxOrd();
        }

        @Override
        public boolean isMultiValued() {
            return false;
        }

        @Override
        public long getOrd(int docId) {
            return currentOrdinal = docId + 1;
        }

        @Override
        public LongsRef getOrds(int docId) {
            longsScratch.longs[0] = currentOrdinal = docId + 1;
            return longsScratch;
        }

        @Override
        public long nextOrd() {
            assert docId >= 0;
            currentOrdinal = docId + 1;
            docId = -1;
            return currentOrdinal;
        }

        @Override
        public int setDocument(int docId) {
            this.docId = docId;
            return 1;
        }

        @Override
        public long currentOrd() {
            return currentOrdinal;
        }
    }
}
