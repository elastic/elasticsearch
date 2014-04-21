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
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.OpenBitSet;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.ApplyAcceptedDocsFilter;
import org.elasticsearch.common.lucene.search.NoopCollector;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Set;

/**
 *
 */
public class ChildrenConstantScoreQuery extends Query {

    private final ParentChildIndexFieldData parentChildIndexFieldData;
    private Query originalChildQuery;
    private final String parentType;
    private final String childType;
    private final Filter parentFilter;
    private final int shortCircuitParentDocSet;
    private final Filter nonNestedDocsFilter;
    private final ExecutionMode executionMode;

    private Query rewrittenChildQuery;
    private IndexReader rewriteIndexReader;

    public ChildrenConstantScoreQuery(ParentChildIndexFieldData parentChildIndexFieldData, Query childQuery, String parentType, String childType, Filter parentFilter, int shortCircuitParentDocSet, Filter nonNestedDocsFilter, ExecutionMode executionMode) {
        this.parentChildIndexFieldData = parentChildIndexFieldData;
        this.parentFilter = parentFilter;
        this.parentType = parentType;
        this.childType = childType;
        this.originalChildQuery = childQuery;
        this.shortCircuitParentDocSet = shortCircuitParentDocSet;
        this.nonNestedDocsFilter = nonNestedDocsFilter;
        this.executionMode = executionMode;
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
        ChildrenConstantScoreQuery q = (ChildrenConstantScoreQuery) super.clone();
        q.originalChildQuery = originalChildQuery.clone();
        if (q.rewrittenChildQuery != null) {
            q.rewrittenChildQuery = rewrittenChildQuery.clone();
        }
        return q;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher) throws IOException {
        final SearchContext searchContext = SearchContext.current();
        assert rewrittenChildQuery != null;
        assert rewriteIndexReader == searcher.getIndexReader()  : "not equal, rewriteIndexReader=" + rewriteIndexReader + " searcher.getIndexReader()=" + searcher.getIndexReader();

        boolean releaseParentIds = true;
        AbstractParentCollector collector = null;
        try {
            final Query childQuery = rewrittenChildQuery;
            IndexSearcher indexSearcher = new IndexSearcher(searcher.getIndexReader());
            indexSearcher.setSimilarity(searcher.getSimilarity());
            collector = executionMode.createCollector(parentType, parentChildIndexFieldData, searchContext);
            indexSearcher.search(childQuery, collector);

            long remaining = collector.foundParents();
            if (remaining == 0) {
                return Queries.newMatchNoDocsQuery().createWeight(searcher);
            }

            Filter shortCircuitFilter = null;
            if (remaining == 1) {
                BytesRef id = collector.parentIdValues().get(0, new BytesRef());
                shortCircuitFilter = new TermFilter(new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(parentType, id)));
            } else if (remaining <= shortCircuitParentDocSet) {
                shortCircuitFilter = new ParentIdsFilter(parentType, nonNestedDocsFilter, collector.parentIdValues());
            }
            final ParentWeight parentWeight = new ParentWeight(parentFilter, shortCircuitFilter, collector);
            searchContext.addReleasable(collector, SearchContext.Lifetime.COLLECTION);
            releaseParentIds = false;
            return parentWeight;
        } finally {
           if (releaseParentIds) {
               Releasables.close(collector);
           }
        }
    }

    private final class ParentWeight extends Weight  {

        private final Filter parentFilter;
        private final Filter shortCircuitFilter;
        private final AbstractParentCollector parentIds;

        private long remaining;
        private float queryNorm;
        private float queryWeight;

        public ParentWeight(Filter parentFilter, Filter shortCircuitFilter, AbstractParentCollector parentIds) {
            this.parentFilter = new ApplyAcceptedDocsFilter(parentFilter);
            this.shortCircuitFilter = shortCircuitFilter;
            this.parentIds = parentIds;
            this.remaining = parentIds.foundParents();
        }

        @Override
        public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
            return new Explanation(getBoost(), "not implemented yet...");
        }

        @Override
        public Query getQuery() {
            return ChildrenConstantScoreQuery.this;
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
        public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder, boolean topScorer, Bits acceptDocs) throws IOException {
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
                    DocIdSetIterator parentIdIterator = executionMode.createIterator(context, innerIterator, this, parentIds, parentChildIndexFieldData, parentType);
                    if (parentIdIterator != null) {
                        return ConstantScorer.create(parentIdIterator, this, queryWeight);
                    }
                }
            }
            return null;
        }

    }

    public enum ExecutionMode {

        ORDINALS(new ParseField("ordinals")) {
            @Override
            AbstractParentCollector createCollector(String type, ParentChildIndexFieldData ifd, SearchContext searchContext) {
                return new ParentIdCollector(searchContext, type, ifd);
            }

            @Override
            DocIdSetIterator createIterator(AtomicReaderContext context, DocIdSetIterator parents, ParentWeight parentWeight, AbstractParentCollector collector, ParentChildIndexFieldData parentChildIndexFieldData, String type) {
                BytesValues bytesValues = parentChildIndexFieldData.load(context).getBytesValues(type);
                if (bytesValues == null) {
                    return null;
                }
                return new ParentIdIterator(parents, parentWeight, collector.parentIdValues(), bytesValues);
            }

        },
        GLOBAL_ORDINALS(new ParseField("global_ordinals")) {
            @Override
            AbstractParentCollector createCollector(String type, ParentChildIndexFieldData ifd, SearchContext searchContext) {
                ParentChildIndexFieldData.WithOrdinals global = ifd.getGlobalParentChild(
                        type, searchContext.searcher().getIndexReader()
                );
                return new ParentOrdCollector(searchContext, global);
            }

            @Override
            DocIdSetIterator createIterator(AtomicReaderContext context, DocIdSetIterator parents, ParentWeight parentWeight, AbstractParentCollector collector, ParentChildIndexFieldData parentChildIndexFieldData, String type) {
                ParentOrdCollector parentOrdCollector = (ParentOrdCollector) collector;
                OpenBitSet parentOrds = parentOrdCollector.parentOrds;
                BytesValues.WithOrdinals topLevelValues = parentOrdCollector.indexFieldData.load(context).getBytesValues(false);
                if (topLevelValues == null) {
                    return null;
                }
                return new ParentOrdIterator(parents, parentOrds, topLevelValues.ordinals(), parentWeight);
            }

        };

        private final ParseField name;

        ExecutionMode(ParseField name) {
            this.name = name;
        }

        public ParseField getName() {
            return name;
        }

        abstract AbstractParentCollector createCollector(String type, ParentChildIndexFieldData ifd, SearchContext searchContext);

        abstract DocIdSetIterator createIterator(AtomicReaderContext context, DocIdSetIterator parents, ParentWeight parentWeight, AbstractParentCollector collector, ParentChildIndexFieldData parentChildIndexFieldData, String type);

        public static ExecutionMode fromString(String value) {
            for (ExecutionMode mode : values()) {
                if (mode.name.match(value)) {
                    return mode;
                }
            }
            throw new ElasticsearchIllegalArgumentException("Unknown `execution_hint`: [" + value + "], expected any of " + values());
        }

    }

    public abstract static class AbstractParentCollector extends NoopCollector implements Releasable {

        protected final SearchContext searchContext;

        protected AbstractParentCollector(SearchContext searchContext) {
            this.searchContext = searchContext;
        }

        abstract BytesRefHash parentIdValues();

        abstract long foundParents();

    }

    private final static class ParentOrdCollector extends AbstractParentCollector {

        private final OpenBitSet parentOrds;
        private final ParentChildIndexFieldData.WithOrdinals indexFieldData;

        private BytesValues.WithOrdinals values;
        private Ordinals.Docs ordinals;
        private BytesRefHash parentIds;

        private ParentOrdCollector(SearchContext searchContext, ParentChildIndexFieldData.WithOrdinals indexFieldData) {
            super(searchContext);
            // TODO: look into setting it to macOrd
            this.parentOrds = new OpenBitSet(512);
            this.indexFieldData = indexFieldData;
        }

        @Override
        public void collect(int doc) throws IOException {
            if (ordinals != null) {
                long globalOrd = ordinals.getOrd(doc);
                if (globalOrd != Ordinals.MISSING_ORDINAL) {
                    parentOrds.set(globalOrd);
                }
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            values = indexFieldData.load(context).getBytesValues(false);
            if (values != null) {
                ordinals = values.ordinals();
            } else {
                ordinals = null;
            }
        }

        @Override
        BytesRefHash parentIdValues() {
            parentIds = new BytesRefHash(parentOrds.cardinality(), searchContext.bigArrays());
            for (long parentOrd = parentOrds.nextSetBit(0l); parentOrd != -1; parentOrd = parentOrds.nextSetBit(parentOrd + 1)) {
                parentIds.add(values.getValueByOrd(parentOrd));
            }
            return parentIds;
        }

        @Override
        long foundParents() {
            return parentOrds.cardinality();
        }

        @Override
        public void close() throws ElasticsearchException {
            Releasables.close(parentIds);
        }
    }

    private final static class ParentOrdIterator extends FilteredDocIdSetIterator {

        private final OpenBitSet parentOrds;
        private final Ordinals.Docs ordinals;
        private final ParentWeight parentWeight;

        private ParentOrdIterator(DocIdSetIterator innerIterator, OpenBitSet parentOrds, Ordinals.Docs ordinals, ParentWeight parentWeight) {
            super(innerIterator);
            this.parentOrds = parentOrds;
            this.ordinals = ordinals;
            this.parentWeight = parentWeight;
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

            long parentOrd = ordinals.getOrd(doc);
            if (parentOrd != Ordinals.MISSING_ORDINAL) {
                boolean match = parentOrds.get(parentOrd);
                if (match) {
                    parentWeight.remaining--;
                }
                return match;
            }
            return false;
        }
    }

    private final static class ParentIdCollector extends AbstractParentCollector {

        private final BytesRefHash parentIds;
        private final String parentType;
        private final ParentChildIndexFieldData indexFieldData;

        protected BytesValues.WithOrdinals values;
        private Ordinals.Docs ordinals;

        // This remembers what ordinals have already been seen in the current segment
        // and prevents from fetch the actual id from FD and checking if it exists in parentIds
        private FixedBitSet seenOrdinals;

        protected ParentIdCollector(SearchContext searchContext, String parentType, ParentChildIndexFieldData indexFieldData) {
            super(searchContext);
            this.parentType = parentType;
            this.indexFieldData = indexFieldData;
            this.parentIds = new BytesRefHash(512, searchContext.bigArrays());
        }

        @Override
        public void collect(int doc) throws IOException {
            if (values != null) {
                int ord = (int) ordinals.getOrd(doc);
                if (!seenOrdinals.get(ord)) {
                    final BytesRef bytes  = values.getValueByOrd(ord);
                    final int hash = values.currentValueHash();
                    parentIds.add(bytes, hash);
                    seenOrdinals.set(ord);
                }
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            values = indexFieldData.load(context).getBytesValues(parentType);
            if (values != null) {
                ordinals = values.ordinals();
                final int maxOrd = (int) ordinals.getMaxOrd();
                if (seenOrdinals == null || seenOrdinals.length() < maxOrd) {
                    seenOrdinals = new FixedBitSet(maxOrd);
                } else {
                    seenOrdinals.clear(0, maxOrd);
                }
            }
        }

        @Override
        BytesRefHash parentIdValues() {
            return parentIds;
        }

        @Override
        long foundParents() {
            return parentIds.size();
        }

        @Override
        public void close() throws ElasticsearchException {
            Releasables.close(parentIds);
        }
    }

    private final static class ParentIdIterator extends FilteredDocIdSetIterator {

        private final ParentWeight parentWeight;
        private final BytesRefHash parentIds;
        private final BytesValues values;

        private ParentIdIterator(DocIdSetIterator innerIterator, ParentWeight parentWeight, BytesRefHash parentIds, BytesValues values) {
            super(innerIterator);
            this.parentWeight = parentWeight;
            this.parentIds = parentIds;
            this.values = values;
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

            values.setDocument(doc);
            BytesRef parentId = values.nextValue();
            int hash = values.currentValueHash();
            boolean match = parentIds.find(parentId, hash) >= 0;
            if (match) {
                parentWeight.remaining--;
            }
            return match;
        }
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
        StringBuilder sb = new StringBuilder();
        sb.append("child_filter[").append(childType).append("/").append(parentType).append("](").append(originalChildQuery).append(')');
        return sb.toString();
    }

}
