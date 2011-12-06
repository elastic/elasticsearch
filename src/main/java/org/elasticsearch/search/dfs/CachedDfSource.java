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

package org.elasticsearch.search.dfs;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;

import java.io.IOException;

/**
 *
 */
public class CachedDfSource extends Searcher {

    private final AggregatedDfs dfs;

    private final int maxDoc;

    public CachedDfSource(AggregatedDfs dfs, Similarity similarity) throws IOException {
        this.dfs = dfs;
        setSimilarity(similarity);
        if (dfs.maxDoc() > Integer.MAX_VALUE) {
            maxDoc = Integer.MAX_VALUE;
        } else {
            maxDoc = (int) dfs.maxDoc();
        }
    }

    public int docFreq(Term term) {
        int df = dfs.dfMap().get(term);
        if (df == -1) {
            return 1;
//            throw new IllegalArgumentException("df for term " + term + " not available");
        }
        return df;
    }

    public int[] docFreqs(Term[] terms) {
        int[] result = new int[terms.length];
        for (int i = 0; i < terms.length; i++) {
            result[i] = docFreq(terms[i]);
        }
        return result;
    }

    public int maxDoc() {
        return this.maxDoc;
    }

    public Query rewrite(Query query) {
        // this is a bit of a hack. We know that a query which
        // creates a Weight based on this Dummy-Searcher is
        // always already rewritten (see preparedWeight()).
        // Therefore we just return the unmodified query here
        return query;
    }

    public void close() {
        throw new UnsupportedOperationException();
    }

    public Document doc(int i) {
        throw new UnsupportedOperationException();
    }

    public Document doc(int i, FieldSelector fieldSelector) {
        throw new UnsupportedOperationException();
    }

    public Explanation explain(Weight weight, int doc) {
        throw new UnsupportedOperationException();
    }

    public void search(Weight weight, Filter filter, Collector results) {
        throw new UnsupportedOperationException();
    }

    public TopDocs search(Weight weight, Filter filter, int n) {
        throw new UnsupportedOperationException();
    }

    public TopFieldDocs search(Weight weight, Filter filter, int n, Sort sort) {
        throw new UnsupportedOperationException();
    }

}
