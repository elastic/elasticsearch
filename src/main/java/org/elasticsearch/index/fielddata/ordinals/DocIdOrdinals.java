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

import org.apache.lucene.util.IntsRef;
import org.elasticsearch.common.RamUsage;

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
    public boolean hasSingleArrayBackingStorage() {
        return false;
    }

    @Override
    public Object getBackingStorage() {
        return null;
    }

    @Override
    public long getMemorySizeInBytes() {
        return RamUsage.NUM_BYTES_OBJECT_REF;
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
    public int getNumOrds() {
        return numDocs;
    }

    @Override
    public int getMaxOrd() {
        return numDocs + 1;
    }

    @Override
    public Ordinals.Docs ordinals() {
        return new Docs(this);
    }

    public static class Docs implements Ordinals.Docs {

        private final DocIdOrdinals parent;
        private final IntsRef intsScratch = new IntsRef(new int[1], 0, 1);
        private final SingleValueIter iter = new SingleValueIter();

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
        public int getNumOrds() {
            return parent.getNumOrds();
        }

        @Override
        public int getMaxOrd() {
            return parent.getMaxOrd();
        }

        @Override
        public boolean isMultiValued() {
            return false;
        }

        @Override
        public int getOrd(int docId) {
            return docId + 1;
        }

        @Override
        public IntsRef getOrds(int docId) {
            intsScratch.ints[0] = docId + 1;
            return intsScratch;
        }

        @Override
        public Iter getIter(int docId) {
            return iter.reset(docId + 1);
        }
    }
}
