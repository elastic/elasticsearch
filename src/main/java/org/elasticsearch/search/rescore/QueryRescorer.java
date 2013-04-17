package org.elasticsearch.search.rescore;
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
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.SorterTemplate;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;

final class QueryRescorer implements Rescorer {
    
    public static final Rescorer INSTANCE = new QueryRescorer();
    public static final String NAME = "query";
    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void rescore(TopDocs topDocs, SearchContext context, RescoreSearchContext rescoreContext) throws IOException{
        assert rescoreContext != null;
        QueryRescoreContext rescore = ((QueryRescoreContext) rescoreContext);
        TopDocs queryTopDocs = context.queryResult().topDocs();
        if (queryTopDocs == null || queryTopDocs.totalHits == 0 || queryTopDocs.scoreDocs.length == 0) {
            return;
        }

        ContextIndexSearcher searcher = context.searcher();
        topDocs = searcher.search(rescore.query(), new TopDocsFilter(queryTopDocs), queryTopDocs.scoreDocs.length);
        context.queryResult().topDocs(merge(queryTopDocs, topDocs, rescore));
    }

    @Override
    public Explanation explain(int topLevelDocId, SearchContext context, RescoreSearchContext rescoreContext) throws IOException {
        QueryRescoreContext rescore = ((QueryRescoreContext) context.rescore());
        ContextIndexSearcher searcher = context.searcher();
        Explanation primaryExplain = searcher.explain(context.query(), topLevelDocId);
        if (primaryExplain == null) {
            // this should not happen but just in case
            return  new ComplexExplanation(false, 0.0f, "nothing matched");
        }
        Explanation rescoreExplain = searcher.explain(rescore.query(), topLevelDocId);
        float primaryWeight = rescore.queryWeight();
        ComplexExplanation prim = new ComplexExplanation(primaryExplain.isMatch(),
                primaryExplain.getValue() * primaryWeight,
                "product of:");
        prim.addDetail(primaryExplain);
        prim.addDetail(new Explanation(primaryWeight, "primaryWeight"));
        if (rescoreExplain != null) {
            ComplexExplanation sumExpl = new ComplexExplanation();
            sumExpl.setDescription("sum of:");
            sumExpl.addDetail(prim);
            sumExpl.setMatch(prim.isMatch());
            float secondaryWeight = rescore.rescoreQueryWeight();
            ComplexExplanation sec = new ComplexExplanation(rescoreExplain.isMatch(),
                    rescoreExplain.getValue() * secondaryWeight,
                    "product of:");
            sec.addDetail(rescoreExplain);
            sec.addDetail(new Explanation(secondaryWeight, "secondaryWeight"));
            sumExpl.addDetail(sec);
            sumExpl.setValue(prim.getValue() + sec.getValue());
            return sumExpl;
        } else {
            return prim;
        }
    }

    @Override
    public RescoreSearchContext parse(XContentParser parser, SearchContext context) throws IOException {
        Token token;
        String fieldName = null;
        QueryRescoreContext rescoreContext = new QueryRescoreContext(this);
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
                if ("rescore_query".equals(fieldName)) {
                    ParsedQuery parsedQuery = context.queryParserService().parse(parser);
                    rescoreContext.setParsedQuery(parsedQuery);
                }
            } else if (token.isValue()) {
                if("query_weight".equals(fieldName)) {
                    rescoreContext.setQueryWeight(parser.floatValue());
                } else if("rescore_query_weight".equals(fieldName)) {
                    rescoreContext.setRescoreQueryWeight(parser.floatValue());
                } else {
                    throw new ElasticSearchIllegalArgumentException("rescore doesn't support [" + fieldName + "]");
                }
            }
        }
        return rescoreContext;
    }
    
    static class QueryRescoreContext extends RescoreSearchContext {
        
        public QueryRescoreContext(QueryRescorer rescorer) {
            super(NAME, 10, rescorer);
        }

        private ParsedQuery parsedQuery;
        private float queryWeight = 1.0f;
        private float rescoreQueryWeight = 1.0f;

        public void setParsedQuery(ParsedQuery parsedQuery) {
            this.parsedQuery = parsedQuery;
        }
        
        public Query query() {
            return parsedQuery.query();
        }
        
        public float queryWeight() {
            return queryWeight;
        }
        
        public float rescoreQueryWeight() {
            return rescoreQueryWeight;
        }

        public void setRescoreQueryWeight(float rescoreQueryWeight) {
            this.rescoreQueryWeight = rescoreQueryWeight;
        }

        public void setQueryWeight(float queryWeight) {
            this.queryWeight = queryWeight;
        }
        
    }
    
    
    private TopDocs merge(TopDocs primary, TopDocs secondary, QueryRescoreContext context) {
        DocIdSorter sorter = new DocIdSorter();
        sorter.array = primary.scoreDocs;
        sorter.mergeSort(0, sorter.array.length-1);
        ScoreDoc[] primaryDocs = sorter.array;
        sorter.array = secondary.scoreDocs;
        sorter.mergeSort(0, sorter.array.length-1);
        ScoreDoc[] secondaryDocs = sorter.array;
        int j = 0;
        float primaryWeight = context.queryWeight();
        float secondaryWeight = context.rescoreQueryWeight();
        for (int i = 0; i < primaryDocs.length && j < secondaryDocs.length; i++) {
            if (primaryDocs[i].doc == secondaryDocs[j].doc) {
                primaryDocs[i].score = (primaryDocs[i].score * primaryWeight) + (secondaryDocs[j++].score * secondaryWeight);
            } else {
                primaryDocs[i].score *= primaryWeight;
            }
        }
        ScoreSorter scoreSorter = new ScoreSorter();
        scoreSorter.array = primaryDocs;
        scoreSorter.mergeSort(0, primaryDocs.length-1);
        primary.setMaxScore(primaryDocs[0].score);
        return primary;
    }
    
    private static final class DocIdSorter extends SorterTemplate {
        private ScoreDoc[] array;
        private ScoreDoc pivot;
        @Override
        protected void swap(int i, int j) {
            ScoreDoc scoreDoc = array[i];
            array[i] = array[j];
            array[j] = scoreDoc;
        }

        @Override
        protected int compare(int i, int j) {
            return compareDocId(array[i], array[j]);
        }

        @Override
        protected void setPivot(int i) {
            pivot = array[i];
            
        }

        @Override
        protected int comparePivot(int j) {
            return compareDocId(pivot, array[j]);
        }
        
    }
    
    private static final int compareDocId(ScoreDoc left, ScoreDoc right) {
        if (left.doc < right.doc) {
            return 1;
        } else if (left.doc == right.doc) {
            return 0;
        }
        return -1;
    }
    
    private static final class ScoreSorter extends SorterTemplate {
        private ScoreDoc[] array;
        private ScoreDoc pivot;
        @Override
        protected void swap(int i, int j) {
            ScoreDoc scoreDoc = array[i];
            array[i] = array[j];
            array[j] = scoreDoc;
        }

        @Override
        protected int compare(int i, int j) {
            int cmp = Float.compare(array[j].score, array[i].score);
            return cmp == 0 ? compareDocId(array[i], array[j]) : cmp;
        }

        @Override
        protected void setPivot(int i) {
            pivot = array[i];
            
        }

        @Override
        protected int comparePivot(int j) {
            int cmp = Float.compare(array[j].score, pivot.score);
            return cmp == 0 ? compareDocId(pivot, array[j]) : cmp;
        }
        
    }
      
    private static final class TopDocsFilter extends Filter {

        private final int[] docIds;
        public TopDocsFilter(TopDocs topDocs) {
            this.docIds = new int[topDocs.scoreDocs.length];
            ScoreDoc[] scoreDocs = topDocs.scoreDocs;
            for (int i = 0; i < scoreDocs.length; i++) {
                docIds[i] = scoreDocs[i].doc;
            }
            Arrays.sort(docIds);
            
        }
        @Override
        public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
            final int docBase = context.docBase;
            int limit  = docBase + context.reader().maxDoc();
            int offset = Arrays.binarySearch(docIds, docBase);
            if (offset < 0 ) {
                offset = (-offset)-1;
            }
            int end = Arrays.binarySearch(docIds, limit);
            if (end < 0) {
                end = (-end)-1; 
            } 
            final int start = offset;
            final int stop = end;
            
            return new DocIdSet() {
                
                @Override
                public DocIdSetIterator iterator() throws IOException {
                    return new DocIdSetIterator() {
                        private int current = start;
                        private int docId = NO_MORE_DOCS;
                        
                        @Override
                        public int nextDoc() throws IOException {
                            if (current < stop) {
                                return docId = docIds[current++]-docBase;
                            }
                            return docId = NO_MORE_DOCS;
                        }
                        
                        @Override
                        public int docID() {
                            return docId;
                        }
                        
                        @Override
                        public int advance(int target) throws IOException {
                            if (target == NO_MORE_DOCS) {
                                current = stop;
                                return docId = NO_MORE_DOCS;
                            }
                            while(nextDoc() < target) {}
                            return docId;
                        }

                        @Override
                        public long cost() {
                            return docIds.length;
                        }
                    };
                }
            };
        }
        
    }

    @Override
    public void extractTerms(SearchContext context, RescoreSearchContext rescoreContext, Set<Term> termsSet) {
        ((QueryRescoreContext) context.rescore()).query().extractTerms(termsSet);        
    }

}
