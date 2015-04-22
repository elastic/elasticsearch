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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BitsFilteredDocIdSet;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.XFilteredDocIdSetIterator;
import org.apache.lucene.search.join.BitDocIdSetFilter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LongBitSet;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.NoopCollector;
import org.elasticsearch.index.fielddata.AtomicParentChildFieldData;
import org.elasticsearch.index.fielddata.IndexParentChildFieldData;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 *
 */
// TODO: Remove me and move the logic to ChildrenQuery when needsScore=false
public class ChildrenConstantScoreQuery extends Query {

    private final IndexParentChildFieldData parentChildIndexFieldData;
    private Query originalChildQuery;
    private final String parentType;
    private final String childType;
    private final Filter parentFilter;
    private final int shortCircuitParentDocSet;
    private final BitDocIdSetFilter nonNestedDocsFilter;

    private Query rewrittenChildQuery;
    private IndexReader rewriteIndexReader;

    public ChildrenConstantScoreQuery(IndexParentChildFieldData parentChildIndexFieldData, Query childQuery, String parentType, String childType, Filter parentFilter, int shortCircuitParentDocSet, BitDocIdSetFilter nonNestedDocsFilter) {
        this.parentChildIndexFieldData = parentChildIndexFieldData;
        this.parentFilter = parentFilter;
        this.parentType = parentType;
        this.childType = childType;
        this.originalChildQuery = childQuery;
        this.shortCircuitParentDocSet = shortCircuitParentDocSet;
        this.nonNestedDocsFilter = nonNestedDocsFilter;
    }

    @Override
    // See TopChildrenQuery#rewrite
    public Query rewrite(IndexReader reader) throws IOException {
        if (rewrittenChildQuery == null) {
            rewrittenChildQuery = originalChildQuery.rewrite(reader);
            rewriteIndexReader = reader;
        }
        return this;
    }

    @Override
    public Query clone() {
        ChildrenConstantScoreQuery q = (ChildrenConstantScoreQuery) super.clone();
        q.originalChildQuery = originalChildQuery.clone();
        if (q.rewrittenChildQuery != null) {
            q.rewrittenChildQuery = rewrittenChildQuery.clone();
        }
        return q;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        SearchContext sc = SearchContext.current();
        IndexParentChildFieldData globalIfd = parentChildIndexFieldData.loadGlobal(searcher.getIndexReader());
        assert rewrittenChildQuery != null;
        assert rewriteIndexReader == searcher.getIndexReader()  : "not equal, rewriteIndexReader=" + rewriteIndexReader + " searcher.getIndexReader()=" + searcher.getIndexReader();

        final long valueCount;
        List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
        if (globalIfd == null || leaves.isEmpty()) {
            return new BooleanQuery().createWeight(searcher, needsScores);
        } else {
            AtomicParentChildFieldData afd = globalIfd.load(leaves.get(0));
            SortedDocValues globalValues = afd.getOrdinalsValues(parentType);
            valueCount = globalValues.getValueCount();
        }

        if (valueCount == 0) {
            return new BooleanQuery().createWeight(searcher, needsScores);
        }

        Query childQuery = rewrittenChildQuery;
        IndexSearcher indexSearcher = new IndexSearcher(searcher.getIndexReader());
        indexSearcher.setSimilarity(searcher.getSimilarity());
        ParentOrdCollector collector = new ParentOrdCollector(globalIfd, valueCount, parentType);
        indexSearcher.search(childQuery, collector);

        final long remaining = collector.foundParents();
        if (remaining == 0) {
            return new BooleanQuery().createWeight(searcher, needsScores);
        }

        Filter shortCircuitFilter = null;
        if (remaining <= shortCircuitParentDocSet) {
            shortCircuitFilter = ParentIdsFilter.createShortCircuitFilter(
                    nonNestedDocsFilter, sc, parentType, collector.values, collector.parentOrds, remaining
            );
        }
        return new ParentWeight(this, parentFilter, globalIfd, shortCircuitFilter, collector, remaining);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        ChildrenConstantScoreQuery that = (ChildrenConstantScoreQuery) obj;
        if (!originalChildQuery.equals(that.originalChildQuery)) {
            return false;
        }
        if (!childType.equals(that.childType)) {
            return false;
        }
        if (shortCircuitParentDocSet != that.shortCircuitParentDocSet) {
            return false;
        }
        if (getBoost() != that.getBoost()) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = originalChildQuery.hashCode();
        result = 31 * result + childType.hashCode();
        result = 31 * result + shortCircuitParentDocSet;
        result = 31 * result + Float.floatToIntBits(getBoost());
        return result;
    }

    @Override
    public String toString(String field) {
        return "child_filter[" + childType + "/" + parentType + "](" + originalChildQuery + ')';
    }

    private final class ParentWeight extends Weight  {

        private final Filter parentFilter;
        private final Filter shortCircuitFilter;
        private final ParentOrdCollector collector;
        private final IndexParentChildFieldData globalIfd;

        private long remaining;
        private float queryNorm;
        private float queryWeight;

        public ParentWeight(Query query, Filter parentFilter, IndexParentChildFieldData globalIfd, Filter shortCircuitFilter, ParentOrdCollector collector, long remaining) {
            super(query);
            this.parentFilter = parentFilter;
            this.globalIfd = globalIfd;
            this.shortCircuitFilter = shortCircuitFilter;
            this.collector = collector;
            this.remaining = remaining;
        }

        @Override
        public void extractTerms(Set<Term> terms) {
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            return Explanation.match(getBoost(), "not implemented yet...");
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
        public Scorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
            if (remaining == 0) {
                return null;
            }

            if (shortCircuitFilter != null) {
                DocIdSet docIdSet = shortCircuitFilter.getDocIdSet(context, acceptDocs);
                if (!DocIdSets.isEmpty(docIdSet)) {
                    DocIdSetIterator iterator = docIdSet.iterator();
                    if (iterator != null) {
                        return ConstantScorer.create(iterator, this, queryWeight);
                    }
                }
                return null;
            }

            DocIdSet parentDocIdSet = this.parentFilter.getDocIdSet(context, acceptDocs);
            if (!DocIdSets.isEmpty(parentDocIdSet)) {
                // We can't be sure of the fact that liveDocs have been applied, so we apply it here. The "remaining"
                // count down (short circuit) logic will then work as expected.
                parentDocIdSet = BitsFilteredDocIdSet.wrap(parentDocIdSet, context.reader().getLiveDocs());
                DocIdSetIterator innerIterator = parentDocIdSet.iterator();
                if (innerIterator != null) {
                    LongBitSet parentOrds = collector.parentOrds;
                    SortedDocValues globalValues = globalIfd.load(context).getOrdinalsValues(parentType);
                    if (globalValues != null) {
                        DocIdSetIterator parentIdIterator = new ParentOrdIterator(innerIterator, parentOrds, globalValues, this);
                        return ConstantScorer.create(parentIdIterator, this, queryWeight);
                    }
                }
            }
            return null;
        }

    }

    private final static class ParentOrdCollector extends NoopCollector {

        private final LongBitSet parentOrds;
        private final IndexParentChildFieldData indexFieldData;
        private final String parentType;

        private SortedDocValues values;

        private ParentOrdCollector(IndexParentChildFieldData indexFieldData, long maxOrd, String parentType) {
            // TODO: look into reusing LongBitSet#bits array
            this.parentOrds = new LongBitSet(maxOrd + 1);
            this.indexFieldData = indexFieldData;
            this.parentType = parentType;
        }

        @Override
        public void collect(int doc) throws IOException {
            if (values != null) {
                int globalOrdinal = values.getOrd(doc);
                // TODO: oversize the long bitset and remove the branch
                if (globalOrdinal >= 0) {
                    parentOrds.set(globalOrdinal);
                }
            }
        }

        @Override
        protected void doSetNextReader(LeafReaderContext context) throws IOException {
            values = indexFieldData.load(context).getOrdinalsValues(parentType);
        }

        long foundParents() {
            return parentOrds.cardinality();
        }

    }

    private final static class ParentOrdIterator extends XFilteredDocIdSetIterator {

        private final LongBitSet parentOrds;
        private final SortedDocValues ordinals;
        private final ParentWeight parentWeight;

        private ParentOrdIterator(DocIdSetIterator innerIterator, LongBitSet parentOrds, SortedDocValues ordinals, ParentWeight parentWeight) {
            super(innerIterator);
            this.parentOrds = parentOrds;
            this.ordinals = ordinals;
            this.parentWeight = parentWeight;
        }

        @Override
        protected boolean match(int doc) {
            if (parentWeight.remaining == 0) {
                throw new CollectionTerminatedException();
            }

            long parentOrd = ordinals.getOrd(doc);
            if (parentOrd >= 0) {
                boolean match = parentOrds.get(parentOrd);
                if (match) {
                    parentWeight.remaining--;
                }
                return match;
            }
            return false;
        }
    }

}
