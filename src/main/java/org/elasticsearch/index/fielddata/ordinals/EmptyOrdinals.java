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

package org.elasticsearch.index.fielddata.ordinals;

import org.elasticsearch.ElasticsearchIllegalStateException;

/**
 */
public enum EmptyOrdinals implements Ordinals {
    INSTANCE;

    @Override
    public long getMemorySizeInBytes() {
        return 0;
    }

    @Override
    public boolean isMultiValued() {
        return false;
    }

    @Override
    public long getMaxOrd() {
        return 0;
    }

    @Override
    public Docs ordinals() {
        return new Docs(this);
    }

    public static class Docs extends Ordinals.AbstractDocs {

        public Docs(EmptyOrdinals parent) {
            super(parent);
        }

        @Override
        public long getOrd(int docId) {
            return Ordinals.MISSING_ORDINAL;
        }

        @Override
        public long nextOrd() {
            throw new ElasticsearchIllegalStateException("Empty ordinals has no nextOrd");
        }

        @Override
        public int setDocument(int docId) {
            return 0;
        }

        @Override
        public long currentOrd() {
            return Ordinals.MISSING_ORDINAL;
        }
    }
}
