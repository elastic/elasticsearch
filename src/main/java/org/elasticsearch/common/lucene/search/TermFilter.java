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

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;

import java.io.IOException;

/**
 * A simple filter for a specific term.
 */
public class TermFilter extends Filter {

    private final Term term;

    public TermFilter(Term term) {
        this.term = term;
    }

    public Term getTerm() {
        return term;
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, final Bits acceptDocs) throws IOException {
        Terms terms = context.reader().terms(term.field());
        if (terms == null) {
            return null;
        }

        final TermsEnum termsEnum = terms.iterator(null);
        if (!termsEnum.seekExact(term.bytes(), false)) {
            return null;
        }
        return new DocIdSet() {
            @Override
            public DocIdSetIterator iterator() throws IOException {
                return termsEnum.docs(acceptDocs, null, DocsEnum.FLAG_NONE);
            }

        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TermFilter that = (TermFilter) o;

        if (term != null ? !term.equals(that.term) : that.term != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return term != null ? term.hashCode() : 0;
    }

    @Override
    public String toString() {
        return term.field() + ":" + term.text();
    }
}
