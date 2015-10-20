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

import org.apache.lucene.index.*;
import org.apache.lucene.search.BitsFilteredDocIdSet;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredDocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.SuppressForbidden;
import org.elasticsearch.common.lucene.IndexCacheableQuery;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.fielddata.AtomicParentChildFieldData;
import org.elasticsearch.index.fielddata.IndexParentChildFieldData;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * A query that only return child documents that are linked to the parent documents that matched with the inner query.
 * @deprecated use queries from lucene/join instead
 */
@SuppressForbidden(reason="Old p/c queries still use filters")
@Deprecated
public class ParentConstantScoreQuery extends IndexCacheableQuery {

    private final ParentChildIndexFieldData parentChildIndexFieldData;
    private Query parentQuery;
    private final String parentType;
    private final Filter childrenFilter;

    public ParentConstantScoreQuery(ParentChildIndexFieldData parentChildIndexFieldData, Query parentQuery, String parentType, Filter childrenFilter) {
        this.parentChildIndexFieldData = parentChildIndexFieldData;
        this.parentQuery = parentQuery;
        this.parentType = parentType;
        this.childrenFilter = childrenFilter;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query parentRewritten = parentQuery.rewrite(reader);
        if (parentRewritten != parentQuery) {
            Query rewritten = new ParentConstantScoreQuery(parentChildIndexFieldData, parentRewritten, parentType, childrenFilter);
            rewritten.setBoost(getBoost());
            return rewritten;
        }
        return super.rewrite(reader);
    }

    @Override
    public Weight doCreateWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        IndexParentChildFieldData globalIfd = parentChildIndexFieldData.loadGlobal((DirectoryReader)searcher.getIndexReader());

        final long maxOrd;
        List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
        if (globalIfd == null || leaves.isEmpty()) {
            return new BooleanQuery.Builder().build().createWeight(searcher, needsScores);
        } else {
            AtomicParentChildFieldData afd = globalIfd.load(leaves.get(0));
            SortedDocValues globalValues = afd.getOrdinalsValues(parentType);
            maxOrd = globalValues.getValueCount();
        }

        if (maxOrd == 0) {
            return new BooleanQuery.Builder().build().createWeight(searcher, needsScores);
        }

        ParentOrdsCollector collector = new ParentOrdsCollector(globalIfd, maxOrd, parentType);
        searcher.search(parentQuery, collector);

        if (collector.parentCount() == 0) {
            return new BooleanQuery.Builder().build().createWeight(searcher, needsScores);
        }

        return new ChildrenWeight(this, childrenFilter, collector, globalIfd);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + parentQuery.hashCode();
        result = 31 * result + parentType.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (super.equals(obj) == false) {
            return false;
        }

        ParentConstantScoreQuery that = (ParentConstantScoreQuery) obj;
        if (!parentQuery.equals(that.parentQuery)) {
            return false;
        }
        if (!parentType.equals(that.parentType)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString(String field) {
        return "parent_filter[" + parentType + "](" + parentQuery + ')';
    }

    @SuppressForbidden(reason="Old p/c queries still use filters")
    private final class ChildrenWeight extends Weight {

        private final IndexParentChildFieldData globalIfd;
        private final Filter childrenFilter;
        private final LongBitSet parentOrds;

        private float queryNorm;
        private float queryWeight;

        private ChildrenWeight(Query query, Filter childrenFilter, ParentOrdsCollector collector, IndexParentChildFieldData globalIfd) {
            super(query);
            this.globalIfd = globalIfd;
            this.childrenFilter = childrenFilter;
            this.parentOrds = collector.parentOrds;
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
        public Scorer scorer(LeafReaderContext context) throws IOException {
            DocIdSet childrenDocIdSet = childrenFilter.getDocIdSet(context, null);
            if (Lucene.isEmpty(childrenDocIdSet)) {
                return null;
            }

            SortedDocValues globalValues = globalIfd.load(context).getOrdinalsValues(parentType);
            if (globalValues != null) {
                // we forcefully apply live docs here so that deleted children don't give matching parents
                childrenDocIdSet = BitsFilteredDocIdSet.wrap(childrenDocIdSet, context.reader().getLiveDocs());
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
        private final SortedDocValues globalOrdinals;

        ChildrenDocIdIterator(DocIdSetIterator innerIterator, LongBitSet parentOrds, SortedDocValues globalOrdinals) {
            super(innerIterator);
            this.parentOrds = parentOrds;
            this.globalOrdinals = globalOrdinals;
        }

        @Override
        protected boolean match(int docId) {
            int globalOrd = globalOrdinals.getOrd(docId);
            if (globalOrd >= 0) {
                return parentOrds.get(globalOrd);
            } else {
                return false;
            }
        }

    }

    private final static class ParentOrdsCollector extends SimpleCollector {

        private final LongBitSet parentOrds;
        private final IndexParentChildFieldData globalIfd;
        private final String parentType;

        private SortedDocValues globalOrdinals;

        ParentOrdsCollector(IndexParentChildFieldData globalIfd, long maxOrd, String parentType) {
            this.parentOrds = new LongBitSet(maxOrd);
            this.globalIfd = globalIfd;
            this.parentType = parentType;
        }

        @Override
        public void collect(int doc) throws IOException {
            // It can happen that for particular segment no document exist for an specific type. This prevents NPE
            if (globalOrdinals != null) {
                long globalOrd = globalOrdinals.getOrd(doc);
                if (globalOrd >= 0) {
                    parentOrds.set(globalOrd);
                }
            }
        }

        @Override
        public void doSetNextReader(LeafReaderContext readerContext) throws IOException {
            globalOrdinals = globalIfd.load(readerContext).getOrdinalsValues(parentType);
        }

        public long parentCount() {
            return parentOrds.cardinality();
        }

        @Override
        public boolean needsScores() {
            return false;
        }
    }

}

