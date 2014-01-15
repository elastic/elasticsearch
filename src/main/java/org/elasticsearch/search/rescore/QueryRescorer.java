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

package org.elasticsearch.search.rescore;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IntroSorter;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

public final class QueryRescorer implements Rescorer {

    private static enum ScoreMode {
        Avg {
            @Override
            public float combine(float primary, float secondary) {
                return (primary + secondary) / 2;
            }

            @Override
            public String toString() {
                return "avg";
            }
        },
        Max {
            @Override
            public float combine(float primary, float secondary) {
                return Math.max(primary, secondary);
            }

            @Override
            public String toString() {
                return "max";
            }
        },
        Min {
            @Override
            public float combine(float primary, float secondary) {
                return Math.min(primary, secondary);
            }

            @Override
            public String toString() {
                return "min";
            }
        },
        Total {
            @Override
            public float combine(float primary, float secondary) {
                return primary + secondary;
            }

            @Override
            public String toString() {
                return "sum";
            }
        },
        Multiply {
            @Override
            public float combine(float primary, float secondary) {
                return primary * secondary;
            }

            @Override
            public String toString() {
                return "product";
            }
        };

        public abstract float combine(float primary, float secondary);
    }

    public static final Rescorer INSTANCE = new QueryRescorer();
    public static final String NAME = "query";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void rescore(TopDocs topDocs, SearchContext context, RescoreSearchContext rescoreContext) throws IOException {
        assert rescoreContext != null;
        if (topDocs == null || topDocs.totalHits == 0 || topDocs.scoreDocs.length == 0) {
            return;
        }

        QueryRescoreContext rescore = (QueryRescoreContext) rescoreContext;
        ContextIndexSearcher searcher = context.searcher();
        TopDocsFilter filter = new TopDocsFilter(topDocs, rescoreContext.window());
        TopDocs rescored = searcher.search(rescore.query(), filter, rescoreContext.window());
        context.queryResult().topDocs(merge(topDocs, rescored, rescore));
    }

    @Override
    public Explanation explain(int topLevelDocId, SearchContext context, RescoreSearchContext rescoreContext,
            Explanation sourceExplanation) throws IOException {
        QueryRescoreContext rescore = ((QueryRescoreContext) rescoreContext);
        ContextIndexSearcher searcher = context.searcher();
        if (sourceExplanation == null) {
            // this should not happen but just in case
            return new ComplexExplanation(false, 0.0f, "nothing matched");
        }
        Explanation rescoreExplain = searcher.explain(rescore.query(), topLevelDocId);
        float primaryWeight = rescore.queryWeight();
        ComplexExplanation prim = new ComplexExplanation(sourceExplanation.isMatch(),
                sourceExplanation.getValue() * primaryWeight,
                "product of:");
        prim.addDetail(sourceExplanation);
        prim.addDetail(new Explanation(primaryWeight, "primaryWeight"));
        if (rescoreExplain != null && rescoreExplain.isMatch()) {
            float secondaryWeight = rescore.rescoreQueryWeight();
            ComplexExplanation sec = new ComplexExplanation(rescoreExplain.isMatch(),
                    rescoreExplain.getValue() * secondaryWeight,
                    "product of:");
            sec.addDetail(rescoreExplain);
            sec.addDetail(new Explanation(secondaryWeight, "secondaryWeight"));
            ScoreMode scoreMode = rescore.scoreMode();
            ComplexExplanation calcExpl = new ComplexExplanation();
            calcExpl.setDescription(scoreMode + " of:");
            calcExpl.addDetail(prim);
            calcExpl.setMatch(prim.isMatch());
            calcExpl.addDetail(sec);
            calcExpl.setValue(scoreMode.combine(prim.getValue(), sec.getValue()));
            return calcExpl;
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
                if ("query_weight".equals(fieldName)) {
                    rescoreContext.setQueryWeight(parser.floatValue());
                } else if ("rescore_query_weight".equals(fieldName)) {
                    rescoreContext.setRescoreQueryWeight(parser.floatValue());
                } else if ("score_mode".equals(fieldName)) {
                    String sScoreMode = parser.text();
                    if ("avg".equals(sScoreMode)) {
                        rescoreContext.setScoreMode(ScoreMode.Avg);
                    } else if ("max".equals(sScoreMode)) {
                        rescoreContext.setScoreMode(ScoreMode.Max);
                    } else if ("min".equals(sScoreMode)) {
                        rescoreContext.setScoreMode(ScoreMode.Min);
                    } else if ("total".equals(sScoreMode)) {
                        rescoreContext.setScoreMode(ScoreMode.Total);
                    } else if ("multiply".equals(sScoreMode)) {
                        rescoreContext.setScoreMode(ScoreMode.Multiply);
                    } else {
                        throw new ElasticsearchIllegalArgumentException("[rescore] illegal score_mode [" + sScoreMode + "]");
                    }
                } else {
                    throw new ElasticsearchIllegalArgumentException("rescore doesn't support [" + fieldName + "]");
                }
            }
        }
        return rescoreContext;
    }

    public static class QueryRescoreContext extends RescoreSearchContext {

        public QueryRescoreContext(QueryRescorer rescorer) {
            super(NAME, 10, rescorer);
            this.scoreMode = ScoreMode.Total;
        }

        private ParsedQuery parsedQuery;
        private float queryWeight = 1.0f;
        private float rescoreQueryWeight = 1.0f;
        private ScoreMode scoreMode;

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

        public ScoreMode scoreMode() {
            return scoreMode;
        }

        public void setRescoreQueryWeight(float rescoreQueryWeight) {
            this.rescoreQueryWeight = rescoreQueryWeight;
        }

        public void setQueryWeight(float queryWeight) {
            this.queryWeight = queryWeight;
        }

        public void setScoreMode(ScoreMode scoreMode) {
            this.scoreMode = scoreMode;
        }

    }


    private TopDocs merge(TopDocs primary, TopDocs secondary, QueryRescoreContext context) {
        DocIdSorter sorter = new DocIdSorter();
        sorter.array = primary.scoreDocs;
        sorter.sort(0, sorter.array.length);
        ScoreDoc[] primaryDocs = sorter.array;
        sorter.array = secondary.scoreDocs;
        sorter.sort(0, sorter.array.length);
        ScoreDoc[] secondaryDocs = sorter.array;
        int j = 0;
        float primaryWeight = context.queryWeight();
        float secondaryWeight = context.rescoreQueryWeight();
        ScoreMode scoreMode = context.scoreMode();
        for (int i = 0; i < primaryDocs.length; i++) {
            if (j < secondaryDocs.length && primaryDocs[i].doc == secondaryDocs[j].doc) {
                primaryDocs[i].score = scoreMode.combine(primaryDocs[i].score * primaryWeight, secondaryDocs[j++].score * secondaryWeight);
            } else {
                primaryDocs[i].score *= primaryWeight;
            }
        }
        ScoreSorter scoreSorter = new ScoreSorter();
        scoreSorter.array = primaryDocs;
        scoreSorter.sort(0, primaryDocs.length);
        primary.setMaxScore(primaryDocs[0].score);
        return primary;
    }

    private static final class DocIdSorter extends IntroSorter {
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

    private static final class ScoreSorter extends IntroSorter {
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

        public TopDocsFilter(TopDocs topDocs, int max) {
            ScoreDoc[] scoreDocs = topDocs.scoreDocs;
            max = Math.min(max, scoreDocs.length);
            this.docIds = new int[max];
            for (int i = 0; i < max; i++) {
                docIds[i] = scoreDocs[i].doc;
            }
            Arrays.sort(docIds);
        }

        @Override
        public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
            final int docBase = context.docBase;
            int limit = docBase + context.reader().maxDoc();
            int offset = Arrays.binarySearch(docIds, docBase);
            if (offset < 0) {
                offset = (-offset) - 1;
            }
            int end = Arrays.binarySearch(docIds, limit);
            if (end < 0) {
                end = (-end) - 1;
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
                                return docId = docIds[current++] - docBase;
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
                            while (nextDoc() < target) {
                            }
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
        ((QueryRescoreContext) rescoreContext).query().extractTerms(termsSet);
    }

}
