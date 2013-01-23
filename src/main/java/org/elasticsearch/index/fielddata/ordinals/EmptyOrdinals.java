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

import org.elasticsearch.index.fielddata.util.IntArrayRef;

/**
 */
public class EmptyOrdinals implements Ordinals {

    private final int numDocs;

    public EmptyOrdinals(int numDocs) {
        this.numDocs = numDocs;
    }

    @Override
    public long getMemorySizeInBytes() {
        return 0;
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
    public boolean isMultiValued() {
        return false;
    }

    @Override
    public int getNumDocs() {
        return this.numDocs;
    }

    @Override
    public int getNumOrds() {
        return 1;
    }

    @Override
    public Docs ordinals() {
        return new Docs(this);
    }

    public static class Docs implements Ordinals.Docs {

        private final EmptyOrdinals parent;

        public Docs(EmptyOrdinals parent) {
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
        public boolean isMultiValued() {
            return false;
        }

        @Override
        public int getOrd(int docId) {
            return 0;
        }

        @Override
        public IntArrayRef getOrds(int docId) {
            return IntArrayRef.EMPTY;
        }

        @Override
        public Iter getIter(int docId) {
            return EmptyIter.INSTANCE;
        }

        @Override
        public void forEachOrdinalInDoc(int docId, OrdinalInDocProc proc) {
            proc.onOrdinal(docId, 0);
        }
    }
}
