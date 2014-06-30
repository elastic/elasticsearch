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
package org.elasticsearch.index.search.child;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LongBitSet;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.ApplyAcceptedDocsFilter;
import org.elasticsearch.common.lucene.search.NoopCollector;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * A query that only return child documents that are linked to the parent documents that matched with the inner query.
 */
public class ParentConstantScoreQuery extends Query {

    private final ParentChildIndexFieldData parentChildIndexFieldData;
    private Query originalParentQuery;
    private final String parentType;
    private final Filter childrenFilter;

    private Query rewrittenParentQuery;
    private IndexReader rewriteIndexReader;

    public ParentConstantScoreQuery(ParentChildIndexFieldData parentChildIndexFieldData, Query parentQuery, String parentType, Filter childrenFilter) {
        this.parentChildIndexFieldData = parentChildIndexFieldData;
        this.originalParentQuery = parentQuery;
        this.parentType = parentType;
        this.childrenFilter = childrenFilter;
    }

    @Override
    // See TopChildrenQuery#rewrite
    public Query rewrite(IndexReader reader) throws IOException {
        if (rewrittenParentQuery == null) {
            rewrittenParentQuery = originalParentQuery.rewrite(reader);
            rewriteIndexReader = reader;
        }
        return this;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
        rewrittenParentQuery.extractTerms(terms);
    }

    @Override
    public Query clone() {
        ParentConstantScoreQuery q = (ParentConstantScoreQuery) super.clone();
        q.originalParentQuery = originalParentQuery.clone();
        if (q.rewrittenParentQuery != null) {
            q.rewrittenParentQuery = rewrittenParentQuery.clone();
        }
        return q;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher) throws IOException {
        IndexFieldData.WithOrdinals globalIfd = parentChildIndexFieldData.getGlobalParentChild(parentType, searcher.getIndexReader());
        assert rewrittenParentQuery != null;
        assert rewriteIndexReader == searcher.getIndexReader() : "not equal, rewriteIndexReader=" + rewriteIndexReader + " searcher.getIndexReader()=" + searcher.getIndexReader();

        final long maxOrd;
        List<AtomicReaderContext> leaves = searcher.getIndexReader().leaves();
        if (globalIfd == null || leaves.isEmpty()) {
            return Queries.newMatchNoDocsQuery().createWeight(searcher);
        } else {
            AtomicFieldData.WithOrdinals afd = globalIfd.load(leaves.get(0));
            BytesValues.WithOrdinals globalValues = afd.getBytesValues();
            maxOrd = globalValues.getMaxOrd();
        }

        if (maxOrd == 0) {
            return Queries.newMatchNoDocsQuery().createWeight(searcher);
        }

        final Query parentQuery = rewrittenParentQuery;
        ParentOrdsCollector collector = new ParentOrdsCollector(globalIfd, maxOrd);
        IndexSearcher indexSearcher = new IndexSearcher(searcher.getIndexReader());
        indexSearcher.setSimilarity(searcher.getSimilarity());
        indexSearcher.search(parentQuery, collector);

        if (collector.parentCount() == 0) {
            return Queries.newMatchNoDocsQuery().createWeight(searcher);
        }

        return new ChildrenWeight(childrenFilter, collector, globalIfd);
    }

    @Override
    public int hashCode() {
        int result = originalParentQuery.hashCode();
        result = 31 * result + parentType.hashCode();
        result = 31 * result + Float.floatToIntBits(getBoost());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        ParentConstantScoreQuery that = (ParentConstantScoreQuery) obj;
        if (!originalParentQuery.equals(that.originalParentQuery)) {
            return false;
        }
        if (!parentType.equals(that.parentType)) {
            return false;
        }
        if (this.getBoost() != that.getBoost()) {
            return false;
        }
        return true;
    }

    @Override
    public String toString(String field) {
        return "parent_filter[" + parentType + "](" + originalParentQuery + ')';
    }

    private final class ChildrenWeight extends Weight {

        private final IndexFieldData.WithOrdinals globalIfd;
        private final Filter childrenFilter;
        private final LongBitSet parentOrds;

        private float queryNorm;
        private float queryWeight;

        private ChildrenWeight(Filter childrenFilter, ParentOrdsCollector collector, IndexFieldData.WithOrdinals globalIfd) {
            this.globalIfd = globalIfd;
            this.childrenFilter = new ApplyAcceptedDocsFilter(childrenFilter);
            this.parentOrds = collector.parentOrds;
        }

        @Override
        public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
            return new Explanation(getBoost(), "not implemented yet...");
        }

        @Override
        public Query getQuery() {
            return ParentConstantScoreQuery.this;
        }

        @Override
        public float getValueForNormalization() throws IOException {
            queryWeight = getBoost();
            return queryWeight * queryWeight;
        }

        @Override
        public void normalize(float norm, float topLevelBoost) {
            this.queryNorm = norm * topLevelBoost;
            queryWeight *= this.queryNorm;
        }

        @Override
        public Scorer scorer(AtomicReaderContext context, Bits acceptDocs) throws IOException {
            DocIdSet childrenDocIdSet = childrenFilter.getDocIdSet(context, acceptDocs);
            if (DocIdSets.isEmpty(childrenDocIdSet)) {
                return null;
            }

            BytesValues.WithOrdinals globalValues = globalIfd.load(context).getBytesValues();
            if (globalValues != null) {
                DocIdSetIterator innerIterator = childrenDocIdSet.iterator();
                if (innerIterator != null) {
                    ChildrenDocIdIterator childrenDocIdIterator = new ChildrenDocIdIterator(
                            innerIterator, parentOrds, globalValues
                    );
                    return ConstantScorer.create(childrenDocIdIterator, this, queryWeight);
                }
            }
            return null;
        }

    }

    private final class ChildrenDocIdIterator extends FilteredDocIdSetIterator {

        private final LongBitSet parentOrds;
        private final BytesValues.WithOrdinals globalOrdinals;

        ChildrenDocIdIterator(DocIdSetIterator innerIterator, LongBitSet parentOrds, BytesValues.WithOrdinals globalOrdinals) {
            super(innerIterator);
            this.parentOrds = parentOrds;
            this.globalOrdinals = globalOrdinals;
        }

        @Override
        protected boolean match(int docId) {
            int globalOrd = (int) globalOrdinals.getOrd(docId);
            if (globalOrd != BytesValues.WithOrdinals.MISSING_ORDINAL) {
                return parentOrds.get(globalOrd);
            } else {
                return false;
            }
        }

    }

    private final static class ParentOrdsCollector extends NoopCollector {

        private final LongBitSet parentOrds;
        private final IndexFieldData.WithOrdinals globalIfd;

        private BytesValues.WithOrdinals globalOrdinals;

        ParentOrdsCollector(IndexFieldData.WithOrdinals globalIfd, long maxOrd) {
            this.parentOrds = new LongBitSet(maxOrd);
            this.globalIfd = globalIfd;
        }

        public void collect(int doc) throws IOException {
            // It can happen that for particular segment no document exist for an specific type. This prevents NPE
            if (globalOrdinals != null) {
                long globalOrd = globalOrdinals.getOrd(doc);
                if (globalOrd != BytesValues.WithOrdinals.MISSING_ORDINAL) {
                    parentOrds.set(globalOrd);
                }
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext readerContext) throws IOException {
            globalOrdinals = globalIfd.load(readerContext).getBytesValues();
        }

        public long parentCount() {
            return parentOrds.cardinality();
        }
    }

}

