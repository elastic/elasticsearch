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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.ToStringUtils;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.lucene.search.EmptyScorer;
import org.elasticsearch.search.internal.ScopePhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.*;

/**
 *
 */
public class TopChildrenQuery extends Query implements ScopePhase.TopDocsPhase {

    public static enum ScoreType {
        MAX,
        AVG,
        SUM;

        public static ScoreType fromString(String type) {
            if ("max".equals(type)) {
                return MAX;
            } else if ("avg".equals(type)) {
                return AVG;
            } else if ("sum".equals(type)) {
                return SUM;
            }
            throw new ElasticSearchIllegalArgumentException("No score type for child query [" + type + "] found");
        }
    }

    private Query query;

    private String scope;

    private String parentType;

    private String childType;

    private ScoreType scoreType;

    private int factor;

    private int incrementalFactor;

    private Map<Object, ParentDoc[]> parentDocs;

    private int numHits = 0;

    // Note, the query is expected to already be filtered to only child type docs
    public TopChildrenQuery(Query query, String scope, String childType, String parentType, ScoreType scoreType, int factor, int incrementalFactor) {
        this.query = query;
        this.scope = scope;
        this.childType = childType;
        this.parentType = parentType;
        this.scoreType = scoreType;
        this.factor = factor;
        this.incrementalFactor = incrementalFactor;
    }

    @Override
    public Query query() {
        return this;
    }

    @Override
    public String scope() {
        return scope;
    }

    @Override
    public void clear() {
        parentDocs = null;
        numHits = 0;
    }

    @Override
    public int numHits() {
        return numHits;
    }

    @Override
    public int factor() {
        return this.factor;
    }

    @Override
    public int incrementalFactor() {
        return this.incrementalFactor;
    }

    @Override
    public void processResults(TopDocs topDocs, SearchContext context) {
        Map<Object, TIntObjectHashMap<ParentDoc>> parentDocsPerReader = new HashMap<Object, TIntObjectHashMap<ParentDoc>>();
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            int readerIndex = context.searcher().readerIndex(scoreDoc.doc);
            IndexReader subReader = context.searcher().subReaders()[readerIndex];
            int subDoc = scoreDoc.doc - context.searcher().docStarts()[readerIndex];

            // find the parent id
            HashedBytesArray parentId = context.idCache().reader(subReader).parentIdByDoc(parentType, subDoc);
            if (parentId == null) {
                // no parent found
                continue;
            }
            // now go over and find the parent doc Id and reader tuple
            for (IndexReader indexReader : context.searcher().subReaders()) {
                int parentDocId = context.idCache().reader(indexReader).docById(parentType, parentId);
                if (parentDocId != -1 && !indexReader.isDeleted(parentDocId)) {
                    // we found a match, add it and break

                    TIntObjectHashMap<ParentDoc> readerParentDocs = parentDocsPerReader.get(indexReader.getCoreCacheKey());
                    if (readerParentDocs == null) {
                        readerParentDocs = new TIntObjectHashMap<ParentDoc>();
                        parentDocsPerReader.put(indexReader.getCoreCacheKey(), readerParentDocs);
                    }

                    ParentDoc parentDoc = readerParentDocs.get(parentDocId);
                    if (parentDoc == null) {
                        numHits++; // we have a hit on a parent
                        parentDoc = new ParentDoc();
                        parentDoc.docId = parentDocId;
                        parentDoc.count = 1;
                        parentDoc.maxScore = scoreDoc.score;
                        parentDoc.sumScores = scoreDoc.score;
                        readerParentDocs.put(parentDocId, parentDoc);
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

        this.parentDocs = new HashMap<Object, ParentDoc[]>();
        for (Map.Entry<Object, TIntObjectHashMap<ParentDoc>> entry : parentDocsPerReader.entrySet()) {
            ParentDoc[] values = entry.getValue().values(new ParentDoc[entry.getValue().size()]);
            Arrays.sort(values, PARENT_DOC_COMP);
            parentDocs.put(entry.getKey(), values);
        }
    }

    private static final ParentDocComparator PARENT_DOC_COMP = new ParentDocComparator();

    static class ParentDocComparator implements Comparator<ParentDoc> {
        @Override
        public int compare(ParentDoc o1, ParentDoc o2) {
            return o1.docId - o2.docId;
        }
    }

    public static class ParentDoc {
        public int docId;
        public int count;
        public float maxScore = Float.NaN;
        public float sumScores = 0;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query newQ = query.rewrite(reader);
        if (newQ == query) return this;
        TopChildrenQuery bq = (TopChildrenQuery) this.clone();
        bq.query = newQ;
        return bq;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
        query.extractTerms(terms);
    }

    @Override
    public Weight createWeight(Searcher searcher) throws IOException {
        if (parentDocs != null) {
            return new ParentWeight(searcher, query.weight(searcher));
        }
        return query.weight(searcher);
    }

    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("score_child[").append(childType).append("/").append(parentType).append("](").append(query.toString(field)).append(')');
        sb.append(ToStringUtils.boost(getBoost()));
        return sb.toString();
    }

    class ParentWeight extends Weight {

        final Searcher searcher;

        final Weight queryWeight;

        public ParentWeight(Searcher searcher, Weight queryWeight) throws IOException {
            this.searcher = searcher;
            this.queryWeight = queryWeight;
        }

        public Query getQuery() {
            return TopChildrenQuery.this;
        }

        public float getValue() {
            return getBoost();
        }

        @Override
        public float sumOfSquaredWeights() throws IOException {
            float sum = queryWeight.sumOfSquaredWeights();
            sum *= getBoost() * getBoost();
            return sum;
        }

        @Override
        public void normalize(float norm) {
            // nothing to do here....
        }

        @Override
        public Scorer scorer(IndexReader reader, boolean scoreDocsInOrder, boolean topScorer) throws IOException {
            ParentDoc[] readerParentDocs = parentDocs.get(reader.getCoreCacheKey());
            if (readerParentDocs != null) {
                return new ParentScorer(getSimilarity(searcher), readerParentDocs);
            }
            return new EmptyScorer(getSimilarity(searcher));
        }

        @Override
        public Explanation explain(IndexReader reader, int doc) throws IOException {
            return new Explanation(getBoost(), "not implemented yet...");
        }
    }

    class ParentScorer extends Scorer {

        private final ParentDoc[] docs;

        private int index = -1;

        private ParentScorer(Similarity similarity, ParentDoc[] docs) throws IOException {
            super(similarity);
            this.docs = docs;
        }

        @Override
        public int docID() {
            if (index >= docs.length) {
                return NO_MORE_DOCS;
            }
            return docs[index].docId;
        }

        @Override
        public int advance(int target) throws IOException {
            int doc;
            while ((doc = nextDoc()) < target) {
            }
            return doc;
        }

        @Override
        public int nextDoc() throws IOException {
            if (++index >= docs.length) {
                return NO_MORE_DOCS;
            }
            return docs[index].docId;
        }

        @Override
        public float score() throws IOException {
            if (scoreType == ScoreType.MAX) {
                return docs[index].maxScore;
            } else if (scoreType == ScoreType.AVG) {
                return docs[index].sumScores / docs[index].count;
            } else if (scoreType == ScoreType.SUM) {
                return docs[index].sumScores;
            }
            throw new ElasticSearchIllegalStateException("No support for score type [" + scoreType + "]");
        }
    }
}
