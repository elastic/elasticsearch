/*
 * Licensed to Elasticsearch under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License
 * at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.index.search.child;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.ApplyAcceptedDocsFilter;
import org.elasticsearch.common.lucene.search.NoopCollector;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Set;

@SuppressWarnings("rawtypes")
/**
 *
 */
public class CountChildrenConstantScoreQuery extends Query {

    private final ParentChildIndexFieldData parentChildIndexFieldData;
    private Query originalChildQuery;
    private final String parentType;
    private final String childType;
    private final Filter parentFilter;
    private final int shortCircuitParentDocSet;
    private final Filter nonNestedDocsFilter;
    private final int minimumChildren;

    private Query rewrittenChildQuery;
    private IndexReader rewriteIndexReader;

    public CountChildrenConstantScoreQuery(ParentChildIndexFieldData parentChildIndexFieldData, Query childQuery, String parentType, String childType, Filter parentFilter, int shortCircuitParentDocSet, Filter nonNestedDocsFilter, int minimumChildren) {
        this.parentChildIndexFieldData = parentChildIndexFieldData;
        this.parentFilter = parentFilter;
        this.parentType = parentType;
        this.childType = childType;
        this.originalChildQuery = childQuery;
        this.shortCircuitParentDocSet = shortCircuitParentDocSet;
        this.nonNestedDocsFilter = nonNestedDocsFilter;
        this.minimumChildren = minimumChildren;
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
    public void extractTerms(Set<Term> terms) {
        rewrittenChildQuery.extractTerms(terms);
    }

    @Override
    public Query clone() {
        CountChildrenConstantScoreQuery q = (CountChildrenConstantScoreQuery) super.clone();
        q.originalChildQuery = originalChildQuery.clone();
        if (q.rewrittenChildQuery != null) {
            q.rewrittenChildQuery = rewrittenChildQuery.clone();
        }
        return q;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher) throws IOException {
        SearchContext sc = SearchContext.current();
        ParentChildIndexFieldData.WithOrdinals globalIfd = parentChildIndexFieldData.getGlobalParentChild(parentType,
                searcher.getIndexReader());
        assert rewrittenChildQuery != null;
        assert rewriteIndexReader == searcher.getIndexReader() : "not equal, rewriteIndexReader=" + rewriteIndexReader
                + " searcher.getIndexReader()=" + searcher.getIndexReader();

        final long maxOrd;
        List<AtomicReaderContext> leaves = searcher.getIndexReader().leaves();
        if (globalIfd == null || leaves.isEmpty()) {
            return Queries.newMatchNoDocsQuery().createWeight(searcher);
        } else {
            AtomicFieldData.WithOrdinals afd = globalIfd.load(leaves.get(0));
            BytesValues.WithOrdinals globalValues = afd.getBytesValues(false);
            Ordinals.Docs globalOrdinals = globalValues.ordinals();
            maxOrd = globalOrdinals.getMaxOrd();
        }

        if (maxOrd == 0) {
            return Queries.newMatchNoDocsQuery().createWeight(searcher);
        }

        Query childQuery = rewrittenChildQuery;
        IndexSearcher indexSearcher = new IndexSearcher(searcher.getIndexReader());
        indexSearcher.setSimilarity(searcher.getSimilarity());
        CountParentOrdCollector collector = new CountParentOrdCollector(globalIfd, sc.bigArrays(), maxOrd);
        indexSearcher.search(childQuery, collector);

        final long remaining = collector.foundParents();
        if (remaining == 0) {
            return Queries.newMatchNoDocsQuery().createWeight(searcher);
        }

        final Filter parentFilter;
        if (remaining <= shortCircuitParentDocSet) {
            parentFilter = ParentIdsFilter.createShortCircuitFilter(nonNestedDocsFilter, sc, parentType, collector.values,
                    collector.parentIdxs, remaining);
        } else {
            parentFilter = new ApplyAcceptedDocsFilter(this.parentFilter);
        }
        return new CountParentWeight(parentFilter, globalIfd, collector, remaining, minimumChildren);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        CountChildrenConstantScoreQuery that = (CountChildrenConstantScoreQuery) obj;
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
        if (minimumChildren != that.minimumChildren) {
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
        result = 31 * result + minimumChildren;
        return result;
    }

    @Override
    public String toString(String field) {
        return "child_filter[min(" + Integer.toString(minimumChildren) + ") of " + childType + "/" + parentType + "](" + originalChildQuery
                + ')';
    }

    private final class CountParentWeight extends Weight {

        private final Filter parentFilter;
        private final CountParentOrdCollector collector;
        private final IndexFieldData.WithOrdinals globalIfd;
        private final int minimumChildren;

        private long remaining;
        private float queryNorm;
        private float queryWeight;

        public CountParentWeight(Filter parentFilter, IndexFieldData.WithOrdinals globalIfd, CountParentOrdCollector collector, long remaining, int minimumChildren) {
            this.parentFilter = new ApplyAcceptedDocsFilter(parentFilter);
            this.globalIfd = globalIfd;
            this.collector = collector;
            this.remaining = remaining;
            this.minimumChildren = minimumChildren;
        }

        @Override
        public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
            return new Explanation(getBoost(), "not implemented yet...");
        }

        @Override
        public Query getQuery() {
            return CountChildrenConstantScoreQuery.this;
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

            DocIdSet parentDocIdSet = parentFilter.getDocIdSet(context, acceptDocs);
            if (DocIdSets.isEmpty(parentDocIdSet) || remaining == 0) {
                return null;
            }

            // We can't be sure of the fact that liveDocs have been applied, so we apply it here. The "remaining"
            // count down (short circuit) logic will then work as expected.
            DocIdSetIterator parents = BitsFilteredDocIdSet.wrap(parentDocIdSet, context.reader().getLiveDocs()).iterator();
            if (parents != null) {
                BytesValues.WithOrdinals globalValues = globalIfd.load(context).getBytesValues(false);
                if (globalValues == null) {
                    return null;
                }
                Ordinals.Docs globalOrdinals = globalValues.ordinals();
                DocIdSetIterator parentIdIterator = new CountParentOrdIterator(parents, collector.parentIdxs, collector.occurrences,
                        globalOrdinals, this, minimumChildren);
                return ConstantScorer.create(parentIdIterator, this, queryWeight);
            }
            return null;
        }

    }

    private final static class CountParentOrdCollector extends NoopCollector {

        private final IndexFieldData.WithOrdinals indexFieldData;
        private final LongHash parentIdxs;
        private final BigArrays bigArrays;

        private IntArray occurrences;
        private Ordinals.Docs globalOrdinals;
        private BytesValues.WithOrdinals values;

        private CountParentOrdCollector(ParentChildIndexFieldData.WithOrdinals indexFieldData, BigArrays bigArrays, long maxOrd) {
            this.indexFieldData = indexFieldData;
            this.bigArrays = bigArrays;
            this.parentIdxs = new LongHash(512, bigArrays);
            this.occurrences = bigArrays.newIntArray(512, false);
        }

        @Override
        public void collect(int doc) throws IOException {
            if (globalOrdinals != null) {
                final long globalOrdinal = globalOrdinals.getOrd(doc);
                if (globalOrdinal != Ordinals.MISSING_ORDINAL) {
                    long parentIdx = parentIdxs.add(globalOrdinal);
                    if (parentIdx >= 0) {
                        occurrences = bigArrays.grow(occurrences, parentIdx + 1);
                        occurrences.set(parentIdx, 1);
                    } else {
                        parentIdx = -1 - parentIdx;
                        occurrences.increment(parentIdx, 1);
                    }
                }
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            values = indexFieldData.load(context).getBytesValues(false);
            if (values != null) {
                globalOrdinals = values.ordinals();
            } else {
                globalOrdinals = null;
            }
        }

        long foundParents() {
            return parentIdxs.size();
        }

    }

    private final static class CountParentOrdIterator extends FilteredDocIdSetIterator {

        private final LongHash parentIds;
        protected final IntArray occurrences;
        private final int minimumChildren;
        private final Ordinals.Docs ordinals;
        private final CountParentWeight parentWeight;

        private CountParentOrdIterator(DocIdSetIterator innerIterator, LongHash parentIds, IntArray occurrences, Ordinals.Docs ordinals, CountParentWeight parentWeight, int minimumChildren) {
            super(innerIterator);
            this.parentIds = parentIds;
            this.occurrences = occurrences;
            this.ordinals = ordinals;
            this.parentWeight = parentWeight;
            this.minimumChildren = minimumChildren;
        }

        @Override
        protected boolean match(int doc) {
            if (parentWeight.remaining == 0) {
                try {
                    advance(DocIdSetIterator.NO_MORE_DOCS);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return false;
            }

            final long parentOrd = ordinals.getOrd(doc);
            if (parentOrd != Ordinals.MISSING_ORDINAL) {
                final long parentIdx = parentIds.find(parentOrd);
                if (parentIdx != -1) {
                    parentWeight.remaining--;
                    if (occurrences.get(parentIdx) >= minimumChildren) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

}
