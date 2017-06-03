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
package org.elasticsearch.index.search;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;

import java.io.IOException;
import java.util.List;

/**
 * Avoid the expense of the TermsFilter constructor unless absolutely necessary.
 *
 * It turns out that the TermsFilter constructor is pretty expensive.  (See the source.)
 * Much of the time, the TermsFilter is not useful since we use the cached bitset anyway,
 * so defer instantiating the TermsFilter until needed.
 */
public class DeferredTermsFilter extends Filter {
    private final String field;
    // TODO: is it OK to keep a reference to AbstractFieldMapper in this class?
    private final AbstractFieldMapper mapper;
    private List values;

    // TODO: consider setting this to null to allow GC if it turns out the filter was not needed (through a public method)
    private TermsFilter filter;

    /**
     * @param mapper we need this to dynamically dispatch to AbstractFieldMapper.indexedValueForSearch()
     *               which has different implementations depending on the data type
     */
    public DeferredTermsFilter(String field, List values, AbstractFieldMapper mapper) {
        this.field = field;
        this.values = values;
        this.mapper = mapper;
    }

    private synchronized void ensureFilter() {
        if (this.filter == null) {
            BytesRef[] bytesRefs = new BytesRef[values.size()];
            for (int i = 0; i < bytesRefs.length; i++) {
                bytesRefs[i] = mapper.indexedValueForSearch(values.get(i));
            }
            this.filter = new TermsFilter(field, bytesRefs);
            this.values = null; // allow GC: this might not really be necessary
        }
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext atomicReaderContext, Bits bits) throws IOException {
        ensureFilter();
        return this.filter.getDocIdSet(atomicReaderContext, bits);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DeferredTermsFilter)) return false;
        ensureFilter();
        return this.filter.equals(((DeferredTermsFilter) obj).filter);
    }

    @Override
    public int hashCode() {
        ensureFilter();
        return this.filter.hashCode();
    }

    @Override
    public String toString() {
        ensureFilter();
        return this.filter.toString();
    }
}
