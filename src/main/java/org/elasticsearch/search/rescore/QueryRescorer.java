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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
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
    public TopDocs rescore(TopDocs topDocs, SearchContext context, final RescoreSearchContext rescoreContext) throws IOException {
        assert rescoreContext != null;
        if (topDocs == null || topDocs.totalHits == 0 || topDocs.scoreDocs.length == 0) {
            return topDocs;
        }
        return newRescorer((QueryRescoreContext) rescoreContext).rescore(context.searcher(), topDocs, rescoreContext.window());
    }

    @Override
    public Explanation explain(int topLevelDocId, SearchContext context, RescoreSearchContext rescoreContext,
            Explanation sourceExplanation) throws IOException {
        if (sourceExplanation == null) {
            // this should not happen but just in case
            return new ComplexExplanation(false, 0.0f, "nothing matched");
        }
        return newRescorer((QueryRescoreContext) rescoreContext).explain(context.searcher(), sourceExplanation, topLevelDocId);
    }

    private org.apache.lucene.search.Rescorer newRescorer(final QueryRescoreContext ctx) {
        return new org.apache.lucene.search.QueryRescorer(ctx.query()) {

            @Override
            protected float combine(float firstPassScore, boolean secondPassMatches, float secondPassScore) {
                if (secondPassMatches) {
                    return ctx.scoreMode.combine(firstPassScore * ctx.queryWeight(), secondPassScore * ctx.rescoreQueryWeight());
                }
                return firstPassScore * ctx.queryWeight();
            }
        };
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

    @Override
    public void extractTerms(SearchContext context, RescoreSearchContext rescoreContext, Set<Term> termsSet) {
        ((QueryRescoreContext) rescoreContext).query().extractTerms(termsSet);
    }

}
