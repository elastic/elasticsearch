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

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;

import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.util.*;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.search.EmptyScorer;
import org.apache.lucene.search.join.BitDocIdSetFilter;
import org.elasticsearch.index.fielddata.IndexParentChildFieldData;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.SearchContext.Lifetime;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;

/**
 * A query that evaluates the top matching child documents (based on the score) in order to determine what
 * parent documents to return. This query tries to find just enough child documents to return the the requested
 * number of parent documents (or less if no other child document can be found).
 * <p/>
 * This query executes several internal searches. In the first round it tries to find ((request offset + requested size) * factor)
 * child documents. The resulting child documents are mapped into their parent documents including the aggragted child scores.
 * If not enough parent documents could be resolved then a subsequent round is executed, requesting previous requested
 * documents times incremental_factor. This logic repeats until enough parent documents are resolved or until no more
 * child documents are available.
 * <p/>
 * This query is most of the times faster than the {@link ChildrenQuery}. Usually enough parent documents can be returned
 * in the first child document query round.
 */
public class TopChildrenQuery extends Query {

    private static final ParentDocComparator PARENT_DOC_COMP = new ParentDocComparator();

    private final IndexParentChildFieldData parentChildIndexFieldData;
    private final String parentType;
    private final String childType;
    private final ScoreType scoreType;
    private final int factor;
    private final int incrementalFactor;
    private Query originalChildQuery;
    private final BitDocIdSetFilter nonNestedDocsFilter;

    // This field will hold the rewritten form of originalChildQuery, so that we can reuse it
    private Query rewrittenChildQuery;
    private IndexReader rewriteIndexReader;

    // Note, the query is expected to already be filtered to only child type docs
    public TopChildrenQuery(IndexParentChildFieldData parentChildIndexFieldData, Query childQuery, String childType, String parentType, ScoreType scoreType, int factor, int incrementalFactor, BitDocIdSetFilter nonNestedDocsFilter) {
        this.parentChildIndexFieldData = parentChildIndexFieldData;
        this.originalChildQuery = childQuery;
        this.childType = childType;
        this.parentType = parentType;
        this.scoreType = scoreType;
        this.factor = factor;
        this.incrementalFactor = incrementalFactor;
        this.nonNestedDocsFilter = nonNestedDocsFilter;
    }

    // Rewrite invocation logic:
    // 1) query_then|and_fetch (default): Rewrite is execute as part of the createWeight invocation, when search child docs.
    // 2) dfs_query_then|and_fetch:: First rewrite and then createWeight is executed. During query phase rewrite isn't
    // executed any more because searchContext#queryRewritten() returns true.
    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        if (rewrittenChildQuery == null) {
            rewrittenChildQuery = originalChildQuery.rewrite(reader);
            rewriteIndexReader = reader;
        }
        // We can always return the current instance, and we can do this b/c the child query is executed separately
        // before the main query (other scope) in a different IS#search() invocation than the main query.
        // In fact we only need override the rewrite method because for the dfs phase, to get also global document
        // frequency for the child query.
        return this;
    }

    @Override
    public Query clone() {
        TopChildrenQuery q = (TopChildrenQuery) super.clone();
        q.originalChildQuery = originalChildQuery.clone();
        if (q.rewrittenChildQuery != null) {
            q.rewrittenChildQuery = rewrittenChildQuery.clone();
        }
        return q;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        ObjectObjectOpenHashMap<Object, ParentDoc[]> parentDocs = new ObjectObjectOpenHashMap<>();
        SearchContext searchContext = SearchContext.current();

        int parentHitsResolved;
        int requestedDocs = (searchContext.from() + searchContext.size());
        if (requestedDocs <= 0) {
            requestedDocs = 1;
        }
        int numChildDocs = requestedDocs * factor;

        Query childQuery;
        if (rewrittenChildQuery == null) {
            childQuery = rewrittenChildQuery = searcher.rewrite(originalChildQuery);
        } else {
            assert rewriteIndexReader == searcher.getIndexReader() : "not equal, rewriteIndexReader=" + rewriteIndexReader + " searcher.getIndexReader()=" + searcher.getIndexReader();
            childQuery = rewrittenChildQuery;
        }

        IndexSearcher indexSearcher = new IndexSearcher(searcher.getIndexReader());
        indexSearcher.setSimilarity(searcher.getSimilarity());
        while (true) {
            parentDocs.clear();
            TopDocs topChildDocs = indexSearcher.search(childQuery, numChildDocs);
            try {
                parentHitsResolved = resolveParentDocuments(topChildDocs, searchContext, parentDocs);
            } catch (Exception e) {
                throw new IOException(e);
            }

            // check if we found enough docs, if so, break
            if (parentHitsResolved >= requestedDocs) {
                break;
            }
            // if we did not find enough docs, check if it make sense to search further
            if (topChildDocs.totalHits <= numChildDocs) {
                break;
            }
            // if not, update numDocs, and search again
            numChildDocs *= incrementalFactor;
            if (numChildDocs > topChildDocs.totalHits) {
                numChildDocs = topChildDocs.totalHits;
            }
        }

        ParentWeight parentWeight =  new ParentWeight(this, rewrittenChildQuery.createWeight(searcher, needsScores), parentDocs);
        searchContext.addReleasable(parentWeight, Lifetime.COLLECTION);
        return parentWeight;
    }

    int resolveParentDocuments(TopDocs topDocs, SearchContext context, ObjectObjectOpenHashMap<Object, ParentDoc[]> parentDocs) throws Exception {
        int parentHitsResolved = 0;
        ObjectObjectOpenHashMap<Object, IntObjectOpenHashMap<ParentDoc>> parentDocsPerReader = new ObjectObjectOpenHashMap<>(context.searcher().getIndexReader().leaves().size());
        child_hits: for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            int readerIndex = ReaderUtil.subIndex(scoreDoc.doc, context.searcher().getIndexReader().leaves());
            LeafReaderContext subContext = context.searcher().getIndexReader().leaves().get(readerIndex);
            SortedDocValues parentValues = parentChildIndexFieldData.load(subContext).getOrdinalsValues(parentType);
            int subDoc = scoreDoc.doc - subContext.docBase;

            // find the parent id
            BytesRef parentId = parentValues.get(subDoc);
            if (parentId == null) {
                // no parent found
                continue;
            }
            // now go over and find the parent doc Id and reader tuple
            for (LeafReaderContext atomicReaderContext : context.searcher().getIndexReader().leaves()) {
                LeafReader indexReader = atomicReaderContext.reader();
                BitSet nonNestedDocs = null;
                if (nonNestedDocsFilter != null) {
                    BitDocIdSet nonNestedDocIdSet = nonNestedDocsFilter.getDocIdSet(atomicReaderContext);
                    if (nonNestedDocIdSet != null) {
                        nonNestedDocs = nonNestedDocIdSet.bits();
                    }
                }

                Terms terms = indexReader.terms(UidFieldMapper.NAME);
                if (terms == null) {
                    continue;
                }
                TermsEnum termsEnum = terms.iterator();
                if (!termsEnum.seekExact(Uid.createUidAsBytes(parentType, parentId))) {
                    continue;
                }
                PostingsEnum docsEnum = termsEnum.postings(indexReader.getLiveDocs(), null, PostingsEnum.NONE);
                int parentDocId = docsEnum.nextDoc();
                if (nonNestedDocs != null && !nonNestedDocs.get(parentDocId)) {
                    parentDocId = nonNestedDocs.nextSetBit(parentDocId);
                }
                if (parentDocId != DocIdSetIterator.NO_MORE_DOCS) {
                    // we found a match, add it and break
                    IntObjectOpenHashMap<ParentDoc> readerParentDocs = parentDocsPerReader.get(indexReader.getCoreCacheKey());
                    if (readerParentDocs == null) {
                        //The number of docs in the reader and in the query both upper bound the size of parentDocsPerReader
                        int mapSize =  Math.min(indexReader.maxDoc(), context.from() + context.size());
                        readerParentDocs = new IntObjectOpenHashMap<>(mapSize);
                        parentDocsPerReader.put(indexReader.getCoreCacheKey(), readerParentDocs);
                    }
                    ParentDoc parentDoc = readerParentDocs.get(parentDocId);
                    if (parentDoc == null) {
                        parentHitsResolved++; // we have a hit on a parent
                        parentDoc = new ParentDoc();
                        parentDoc.docId = parentDocId;
                        parentDoc.count = 1;
                        parentDoc.maxScore = scoreDoc.score;
                        parentDoc.minScore = scoreDoc.score;
                        parentDoc.sumScores = scoreDoc.score;
                        readerParentDocs.put(parentDocId, parentDoc);
                    } else {
                        parentDoc.count++;
                        parentDoc.sumScores += scoreDoc.score;
                        if (scoreDoc.score < parentDoc.minScore) {
                            parentDoc.minScore = scoreDoc.score;
                        }
                        if (scoreDoc.score > parentDoc.maxScore) {
                            parentDoc.maxScore = scoreDoc.score;
                        }
                    }
                    continue child_hits;
                }
            }
        }
        boolean[] states = parentDocsPerReader.allocated;
        Object[] keys = parentDocsPerReader.keys;
        Object[] values = parentDocsPerReader.values;
        for (int i = 0; i < states.length; i++) {
            if (states[i]) {
                IntObjectOpenHashMap<ParentDoc> value = (IntObjectOpenHashMap<ParentDoc>) values[i];
                ParentDoc[] _parentDocs = value.values().toArray(ParentDoc.class);
                Arrays.sort(_parentDocs, PARENT_DOC_COMP);
                parentDocs.put(keys[i], _parentDocs);
            }
        }
        return parentHitsResolved;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        TopChildrenQuery that = (TopChildrenQuery) obj;
        if (!originalChildQuery.equals(that.originalChildQuery)) {
            return false;
        }
        if (!childType.equals(that.childType)) {
            return false;
        }
        if (incrementalFactor != that.incrementalFactor) {
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
        result = 31 * result + parentType.hashCode();
        result = 31 * result + incrementalFactor;
        result = 31 * result + Float.floatToIntBits(getBoost());
        return result;
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("score_child[").append(childType).append("/").append(parentType).append("](").append(originalChildQuery.toString(field)).append(')');
        sb.append(ToStringUtils.boost(getBoost()));
        return sb.toString();
    }

    private class ParentWeight extends Weight implements Releasable {

        private final Weight queryWeight;
        private final ObjectObjectOpenHashMap<Object, ParentDoc[]> parentDocs;

        public ParentWeight(Query query, Weight queryWeight, ObjectObjectOpenHashMap<Object, ParentDoc[]> parentDocs) throws IOException {
            super(query);
            this.queryWeight = queryWeight;
            this.parentDocs = parentDocs;
        }

        @Override
        public void extractTerms(Set<Term> terms) {
        }

        @Override
        public float getValueForNormalization() throws IOException {
            float sum = queryWeight.getValueForNormalization();
            sum *= getBoost() * getBoost();
            return sum;
        }

        @Override
        public void normalize(float norm, float topLevelBoost) {
            // Nothing to normalize
        }

        @Override
        public void close() throws ElasticsearchException {
        }

        @Override
        public Scorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
            ParentDoc[] readerParentDocs = parentDocs.get(context.reader().getCoreCacheKey());
            // We ignore the needsScores parameter here because there isn't really anything that we
            // can improve by ignoring scores. Actually this query does not really make sense
            // with needsScores=false...
            if (readerParentDocs != null) {
                if (scoreType == ScoreType.MIN) {
                    return new ParentScorer(this, readerParentDocs) {
                        @Override
                        public float score() throws IOException {
                            assert doc.docId >= 0 && doc.docId != NO_MORE_DOCS;
                            return doc.minScore;
                        }
                    };
                } else if (scoreType == ScoreType.MAX) {
                    return new ParentScorer(this, readerParentDocs) {
                        @Override
                        public float score() throws IOException {
                            assert doc.docId >= 0 && doc.docId != NO_MORE_DOCS;
                            return doc.maxScore;
                        }
                    };
                } else if (scoreType == ScoreType.AVG) {
                    return new ParentScorer(this, readerParentDocs) {
                        @Override
                        public float score() throws IOException {
                            assert doc.docId >= 0 && doc.docId != NO_MORE_DOCS;
                            return doc.sumScores / doc.count;
                        }
                    };
                } else if (scoreType == ScoreType.SUM) {
                    return new ParentScorer(this, readerParentDocs) {
                        @Override
                        public float score() throws IOException {
                            assert doc.docId >= 0 && doc.docId != NO_MORE_DOCS;
                            return doc.sumScores;
                        }

                    };
                }
                throw new ElasticsearchIllegalStateException("No support for score type [" + scoreType + "]");
            }
            return new EmptyScorer(this);
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            return Explanation.match(getBoost(), "not implemented yet...");
        }
    }

    private static abstract class ParentScorer extends Scorer {

        private final ParentDoc spare = new ParentDoc();
        protected final ParentDoc[] docs;
        protected ParentDoc doc = spare;
        private int index = -1;

        ParentScorer(ParentWeight weight, ParentDoc[] docs) throws IOException {
            super(weight);
            this.docs = docs;
            spare.docId = -1;
            spare.count = -1;
        }

        @Override
        public final int docID() {
            return doc.docId;
        }

        @Override
        public final int advance(int target) throws IOException {
            return slowAdvance(target);
        }

        @Override
        public final int nextDoc() throws IOException {
            if (++index >= docs.length) {
                doc = spare;
                doc.count = 0;
                return (doc.docId = NO_MORE_DOCS);
            }
            return (doc = docs[index]).docId;
        }

        @Override
        public final int freq() throws IOException {
            return doc.count; // The number of matches in the child doc, which is propagated to parent
        }

        @Override
        public final long cost() {
            return docs.length;
        }
    }

    private static class ParentDocComparator implements Comparator<ParentDoc> {
        @Override
        public int compare(ParentDoc o1, ParentDoc o2) {
            return o1.docId - o2.docId;
        }
    }

    private static class ParentDoc {
        public int docId;
        public int count;
        public float minScore = Float.NaN;
        public float maxScore = Float.NaN;
        public float sumScores = 0;
    }

}
