/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.search.child;

import gnu.trove.map.hash.TIntObjectHashMap;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ToStringUtils;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.lucene.search.EmptyScorer;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.trove.ExtTHashMap;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
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
public class TopChildrenQuery extends Query implements SearchContext.Rewrite {

    private final CacheRecycler cacheRecycler;
    private final String parentType;
    private final String childType;
    private final ScoreType scoreType;
    private final int factor;
    private final int incrementalFactor;
    private final Query originalChildQuery;

    // This field will hold the rewritten form of originalChildQuery, so that we can reuse it
    private Query rewrittenChildQuery;
    private Recycler.V<ExtTHashMap<Object, ParentDoc[]>> parentDocs;

    // Note, the query is expected to already be filtered to only child type docs
    public TopChildrenQuery(Query childQuery, String childType, String parentType, ScoreType scoreType, int factor, int incrementalFactor, CacheRecycler cacheRecycler) {
        this.originalChildQuery = childQuery;
        this.childType = childType;
        this.parentType = parentType;
        this.scoreType = scoreType;
        this.factor = factor;
        this.incrementalFactor = incrementalFactor;
        this.cacheRecycler = cacheRecycler;
    }

    // Rewrite invocation logic:
    // 1) query_then_fetch (default): First contextRewrite and then rewrite is executed
    // 2) dfs_query_then_fetch:: First rewrite and then contextRewrite is executed. During query phase rewrite isn't
    // executed any more because searchContext#queryRewritten() returns true.
    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        if (rewrittenChildQuery == null) {
            rewrittenChildQuery = originalChildQuery.rewrite(reader);
        }
        // We can always return the current instance, and we can do this b/c the child query is executed separately
        // before the main query (other scope) in a different IS#search() invocation than the main query.
        // In fact we only need override the rewrite method because for the dfs phase, to get also global document
        // frequency for the child query.
        return this;
    }

    @Override
    public void contextRewrite(SearchContext searchContext) throws Exception {
        this.parentDocs = cacheRecycler.hashMap(-1);
        searchContext.idCache().refresh(searchContext.searcher().getTopReaderContext().leaves());

        int parentHitsResolved;
        int numChildDocs = (searchContext.from() + searchContext.size());
        if (numChildDocs == 0) {
            numChildDocs = 1;
        }
        numChildDocs *= factor;

        Query childQuery;
        if (rewrittenChildQuery == null) {
            childQuery = rewrittenChildQuery = searchContext.searcher().rewrite(originalChildQuery);
        } else {
            childQuery = rewrittenChildQuery;
        }
        while (true) {
            parentDocs.v().clear();
            TopDocs topChildDocs = searchContext.searcher().search(childQuery, numChildDocs);
            parentHitsResolved = resolveParentDocuments(topChildDocs, searchContext);

            // check if we found enough docs, if so, break
            if (parentHitsResolved >= (searchContext.from() + searchContext.size())) {
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
    }

    @Override
    public void contextClear() {

    }

    @Override
    public void executionDone() {
        if (parentDocs != null) {
            parentDocs.release();
        }
    }

    int resolveParentDocuments(TopDocs topDocs, SearchContext context) {
        int parentHitsResolved = 0;
        Recycler.V<ExtTHashMap<Object, Recycler.V<TIntObjectHashMap<ParentDoc>>>> parentDocsPerReader = cacheRecycler.hashMap(context.searcher().getIndexReader().leaves().size());
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            int readerIndex = ReaderUtil.subIndex(scoreDoc.doc, context.searcher().getIndexReader().leaves());
            AtomicReaderContext subContext = context.searcher().getIndexReader().leaves().get(readerIndex);
            int subDoc = scoreDoc.doc - subContext.docBase;

            // find the parent id
            HashedBytesArray parentId = context.idCache().reader(subContext.reader()).parentIdByDoc(parentType, subDoc);
            if (parentId == null) {
                // no parent found
                continue;
            }
            // now go over and find the parent doc Id and reader tuple
            for (AtomicReaderContext atomicReaderContext : context.searcher().getIndexReader().leaves()) {
                AtomicReader indexReader = atomicReaderContext.reader();
                int parentDocId = context.idCache().reader(indexReader).docById(parentType, parentId);
                Bits liveDocs = indexReader.getLiveDocs();
                if (parentDocId != -1 && (liveDocs == null || liveDocs.get(parentDocId))) {
                    // we found a match, add it and break

                    Recycler.V<TIntObjectHashMap<ParentDoc>> readerParentDocs = parentDocsPerReader.v().get(indexReader.getCoreCacheKey());
                    if (readerParentDocs == null) {
                        readerParentDocs = cacheRecycler.intObjectMap(indexReader.maxDoc());
                        parentDocsPerReader.v().put(indexReader.getCoreCacheKey(), readerParentDocs);
                    }

                    ParentDoc parentDoc = readerParentDocs.v().get(parentDocId);
                    if (parentDoc == null) {
                        parentHitsResolved++; // we have a hit on a parent
                        parentDoc = new ParentDoc();
                        parentDoc.docId = parentDocId;
                        parentDoc.count = 1;
                        parentDoc.maxScore = scoreDoc.score;
                        parentDoc.sumScores = scoreDoc.score;
                        readerParentDocs.v().put(parentDocId, parentDoc);
                    } else {
                        parentDoc.count++;
                        parentDoc.sumScores += scoreDoc.score;
                        if (scoreDoc.score > parentDoc.maxScore) {
                            parentDoc.maxScore = scoreDoc.score;
                        }
                    }
                }
            }
        }

        for (Map.Entry<Object, Recycler.V<TIntObjectHashMap<ParentDoc>>> entry : parentDocsPerReader.v().entrySet()) {
            ParentDoc[] values = entry.getValue().v().values(new ParentDoc[entry.getValue().v().size()]);
            Arrays.sort(values, PARENT_DOC_COMP);
            parentDocs.v().put(entry.getKey(), values);
            entry.getValue().release();
        }
        parentDocsPerReader.release();
        return parentHitsResolved;
    }

    private static final ParentDocComparator PARENT_DOC_COMP = new ParentDocComparator();

    static class ParentDocComparator implements Comparator<ParentDoc> {
        @Override
        public int compare(ParentDoc o1, ParentDoc o2) {
            return o1.docId - o2.docId;
        }
    }

    static class ParentDoc {
        public int docId;
        public int count;
        public float maxScore = Float.NaN;
        public float sumScores = 0;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
        rewrittenChildQuery.extractTerms(terms);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher) throws IOException {
        if (parentDocs == null) {
            throw new ElasticSearchIllegalStateException("top_children query hasn't executed properly");
        }

        return new ParentWeight(searcher, rewrittenChildQuery.createWeight(searcher));
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
        return true;
    }

    @Override
    public int hashCode() {
        int result = originalChildQuery.hashCode();
        result = 31 * result + parentType.hashCode();
        result = 31 * result + incrementalFactor;
        return result;
    }

    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("score_child[").append(childType).append("/").append(parentType).append("](").append(originalChildQuery.toString(field)).append(')');
        sb.append(ToStringUtils.boost(getBoost()));
        return sb.toString();
    }

    class ParentWeight extends Weight {

        final IndexSearcher searcher;

        final Weight queryWeight;

        public ParentWeight(IndexSearcher searcher, Weight queryWeight) throws IOException {
            this.searcher = searcher;
            this.queryWeight = queryWeight;
        }

        public Query getQuery() {
            return TopChildrenQuery.this;
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
        public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder, boolean topScorer, Bits acceptDocs) throws IOException {
            ParentDoc[] readerParentDocs = parentDocs.v().get(context.reader().getCoreCacheKey());
            if (readerParentDocs != null) {
                if (scoreType == ScoreType.MAX) {
                    return new ParentScorer(this, readerParentDocs) {
                        @Override
                        public float score() throws IOException {
                            assert doc.docId >= 0 || doc.docId < NO_MORE_DOCS;
                            return doc.maxScore;
                        }
                    };
                } else if (scoreType == ScoreType.AVG) {
                    return new ParentScorer(this, readerParentDocs) {
                        @Override
                        public float score() throws IOException {
                            assert doc.docId >= 0 || doc.docId < NO_MORE_DOCS;
                            return doc.sumScores / doc.count;
                        }
                    };
                } else if (scoreType == ScoreType.SUM) {
                    return new ParentScorer(this, readerParentDocs) {
                        @Override
                        public float score() throws IOException {
                            assert doc.docId >= 0 || doc.docId < NO_MORE_DOCS;
                            return doc.sumScores;
                        }

                    };
                }
                throw new ElasticSearchIllegalStateException("No support for score type [" + scoreType + "]");
            }
            return new EmptyScorer(this);
        }

        @Override
        public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
            return new Explanation(getBoost(), "not implemented yet...");
        }
    }

    static abstract class ParentScorer extends Scorer {
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
            int doc;
            while ((doc = nextDoc()) < target) {
            }
            return doc;
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
}
