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

package org.apache.lucene.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;
import java.util.Arrays;

/**
 * Similar to {@link TermsFilter} but stores the terms in an array for better memory usage
 * when cached, and also uses bulk read
 */
// LUCENE MONITOR: Against TermsFilter
public class XTermsFilter extends Filter {

    private final Term[] terms;

    public XTermsFilter(Term term) {
        this.terms = new Term[]{term};
    }

    public XTermsFilter(Term[] terms) {
        Arrays.sort(terms);
        this.terms = terms;
    }

    public Term[] getTerms() {
        return terms;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if ((obj == null) || (obj.getClass() != this.getClass()))
            return false;
        XTermsFilter test = (XTermsFilter) obj;
        return Arrays.equals(terms, test.terms);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(terms);
    }

    @Override
    public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
        FixedBitSet result = null;
        TermDocs td = reader.termDocs();
        try {
            // batch read, in Lucene 4.0 its no longer needed
            int[] docs = new int[Lucene.BATCH_ENUM_DOCS];
            int[] freqs = new int[Lucene.BATCH_ENUM_DOCS];
            for (Term term : terms) {
                td.seek(term);
                int number = td.read(docs, freqs);
                if (number > 0) {
                    if (result == null) {
                        result = new FixedBitSet(reader.maxDoc());
                    }
                    while (number > 0) {
                        for (int i = 0; i < number; i++) {
                            result.set(docs[i]);
                        }
                        number = td.read(docs, freqs);
                    }
                }
            }
        } finally {
            td.close();
        }
        return result;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (Term term : terms) {
            if (builder.length() > 0) {
                builder.append(' ');
            }
            builder.append(term);
        }
        return builder.toString();
    }

}
