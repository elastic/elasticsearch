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
package org.elasticsearch.common.lucene.search.function;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.query.functionscore.QueryFunctionBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * Function that scores according to the query defined for it.
 */
public class QueryFunction extends ScoreFunction {
    Query query;
    Weight weight;
    boolean needScores;

    public QueryFunction(Query query) {
        super(CombineFunction.MULTIPLY);
        this.query = query;
    }

    @Override
    public void initWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        // TODO: which weight should be created here? normalized or not?
        weight = query.createWeight(searcher, needsScores);
        this.needScores = needsScores;
    }

    @Override
    public LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) throws IOException {
        final Scorer scorer = weight.scorer(ctx);
        // TODO: not sure if this is the most efficient way to do it
        Bits bits = Lucene.asSequentialAccessBits(ctx.reader().maxDoc(), scorer);
        return new LeafScoreFunction() {

            @Override
            public double score(int docId, float subQueryScore) throws IOException {
                if (bits.get(docId)) {
                    return scorer.score();
                } else {
                    return 0;
                }
            }

            @Override
            public Explanation explainScore(int docId, Explanation subQueryScore) throws IOException {
                double score = score(docId, subQueryScore.getValue());
                Explanation explanation = Explanation.match(
                    CombineFunction.toFloat(score),
                    String.format(Locale.ROOT,
                        "%s score as computed by this query: %s: ", QueryFunctionBuilder.NAME, query.toString()), weight.explain(ctx,
                        docId));
                return explanation;
            }
        };
    }

    @Override
    public boolean needsScores() {
        return needScores;
    }

    @Override
    protected boolean doEquals(ScoreFunction o) {
        QueryFunction that = (QueryFunction) o;
        if (needScores != that.needScores) return false;
        if (query != null ? !query.equals(that.query) : that.query != null) return false;
        return weight != null ? weight.equals(that.weight) : that.weight == null;
    }

    @Override
    public int doHashCode() {
        return Objects.hash(weight, needScores, query);
    }
}
