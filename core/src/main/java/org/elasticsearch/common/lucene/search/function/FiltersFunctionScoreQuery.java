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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ToStringUtils;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * A query that allows for a pluggable boost function / filter. If it matches
 * the filter, it will be boosted by the formula.
 */
public class FiltersFunctionScoreQuery extends Query {

    public static class FilterFunction {
        public final Query filter;
        public final ScoreFunction function;

        public FilterFunction(Query filter, ScoreFunction function) {
            this.filter = filter;
            this.function = function;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            FilterFunction that = (FilterFunction) o;

            if (filter != null ? !filter.equals(that.filter) : that.filter != null)
                return false;
            if (function != null ? !function.equals(that.function) : that.function != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = filter != null ? filter.hashCode() : 0;
            result = 31 * result + (function != null ? function.hashCode() : 0);
            return result;
        }
    }

    public static enum ScoreMode {
        First, Avg, Max, Sum, Min, Multiply
    }

    Query subQuery;
    final FilterFunction[] filterFunctions;
    final ScoreMode scoreMode;
    final float maxBoost;
    private Float minScore;

    protected CombineFunction combineFunction;

    public FiltersFunctionScoreQuery(Query subQuery, ScoreMode scoreMode, FilterFunction[] filterFunctions, float maxBoost, Float minScore) {
        this.subQuery = subQuery;
        this.scoreMode = scoreMode;
        this.filterFunctions = filterFunctions;
        this.maxBoost = maxBoost;
        combineFunction = CombineFunction.MULT;
        this.minScore = minScore;
    }

    public FiltersFunctionScoreQuery setCombineFunction(CombineFunction combineFunction) {
        this.combineFunction = combineFunction;
        return this;
    }

    public Query getSubQuery() {
        return subQuery;
    }

    public FilterFunction[] getFilterFunctions() {
        return filterFunctions;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        if (getBoost() != 1.0F) {
            return super.rewrite(reader);
        }
        Query newQ = subQuery.rewrite(reader);
        if (newQ == subQuery)
            return this;
        FiltersFunctionScoreQuery bq = (FiltersFunctionScoreQuery) this.clone();
        bq.subQuery = newQ;
        return bq;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        if (needsScores == false) {
            return subQuery.createWeight(searcher, needsScores);
        }

        boolean subQueryNeedsScores = combineFunction != CombineFunction.REPLACE;
        Weight[] filterWeights = new Weight[filterFunctions.length];
        for (int i = 0; i < filterFunctions.length; ++i) {
            subQueryNeedsScores |= filterFunctions[i].function.needsScores();
            filterWeights[i] = searcher.createNormalizedWeight(filterFunctions[i].filter, false);
        }
        Weight subQueryWeight = subQuery.createWeight(searcher, subQueryNeedsScores);
        return new CustomBoostFactorWeight(this, subQueryWeight, filterWeights, subQueryNeedsScores);
    }

    class CustomBoostFactorWeight extends Weight {

        final Weight subQueryWeight;
        final Weight[] filterWeights;
        final boolean needsScores;

        public CustomBoostFactorWeight(Query parent, Weight subQueryWeight, Weight[] filterWeights, boolean needsScores) throws IOException {
            super(parent);
            this.subQueryWeight = subQueryWeight;
            this.filterWeights = filterWeights;
            this.needsScores = needsScores;
        }

        @Override
        public void extractTerms(Set<Term> terms) {
            subQueryWeight.extractTerms(terms);
        }

        @Override
        public float getValueForNormalization() throws IOException {
            return subQueryWeight.getValueForNormalization();
        }

        @Override
        public void normalize(float norm, float boost) {
            subQueryWeight.normalize(norm, boost);
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            // we ignore scoreDocsInOrder parameter, because we need to score in
            // order if documents are scored with a script. The
            // ShardLookup depends on in order scoring.
            Scorer subQueryScorer = subQueryWeight.scorer(context);
            if (subQueryScorer == null) {
                return null;
            }
            final LeafScoreFunction[] functions = new LeafScoreFunction[filterFunctions.length];
            final Bits[] docSets = new Bits[filterFunctions.length];
            for (int i = 0; i < filterFunctions.length; i++) {
                FilterFunction filterFunction = filterFunctions[i];
                functions[i] = filterFunction.function.getLeafScoreFunction(context);
                Scorer filterScorer = filterWeights[i].scorer(context);
                docSets[i] = Lucene.asSequentialAccessBits(context.reader().maxDoc(), filterScorer);
            }
            return new FiltersFunctionFactorScorer(this, subQueryScorer, scoreMode, filterFunctions, maxBoost, functions, docSets, combineFunction, minScore, needsScores);
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {

            Explanation subQueryExpl = subQueryWeight.explain(context, doc);
            if (!subQueryExpl.isMatch()) {
                return subQueryExpl;
            }
            // First: Gather explanations for all filters
            List<Explanation> filterExplanations = new ArrayList<>();
            float weightSum = 0;
            for (int i = 0; i < filterFunctions.length; ++i) {
                FilterFunction filterFunction = filterFunctions[i];

                if (filterFunction.function instanceof WeightFactorFunction) {
                    weightSum += ((WeightFactorFunction) filterFunction.function).getWeight();
                } else {
                    weightSum++;
                }

                Bits docSet = Lucene.asSequentialAccessBits(context.reader().maxDoc(),
                        filterWeights[i].scorer(context));
                if (docSet.get(doc)) {
                    Explanation functionExplanation = filterFunction.function.getLeafScoreFunction(context).explainScore(doc, subQueryExpl);
                    double factor = functionExplanation.getValue();
                    float sc = CombineFunction.toFloat(factor);
                    Explanation filterExplanation = Explanation.match(sc, "function score, product of:",
                            Explanation.match(1.0f, "match filter: " + filterFunction.filter.toString()), functionExplanation);
                    filterExplanations.add(filterExplanation);
                }
            }
            if (filterExplanations.size() == 0) {
                return subQueryExpl;
            }

            // Second: Compute the factor that would have been computed by the
            // filters
            double factor = 1.0;
            switch (scoreMode) {
            case First:

                factor = filterExplanations.get(0).getValue();
                break;
            case Max:
                factor = Double.NEGATIVE_INFINITY;
                for (int i = 0; i < filterExplanations.size(); i++) {
                    factor = Math.max(filterExplanations.get(i).getValue(), factor);
                }
                break;
            case Min:
                factor = Double.POSITIVE_INFINITY;
                for (int i = 0; i < filterExplanations.size(); i++) {
                    factor = Math.min(filterExplanations.get(i).getValue(), factor);
                }
                break;
            case Multiply:
                for (int i = 0; i < filterExplanations.size(); i++) {
                    factor *= filterExplanations.get(i).getValue();
                }
                break;
            default: // Avg / Total
                double totalFactor = 0.0f;
                for (int i = 0; i < filterExplanations.size(); i++) {
                    totalFactor += filterExplanations.get(i).getValue();
                }
                if (weightSum != 0) {
                    factor = totalFactor;
                    if (scoreMode == ScoreMode.Avg) {
                        factor /= weightSum;
                    }
                }
            }
            Explanation factorExplanation = Explanation.match(
                    CombineFunction.toFloat(factor),
                    "function score, score mode [" + scoreMode.toString().toLowerCase(Locale.ROOT) + "]",
                    filterExplanations);
            return combineFunction.explain(subQueryExpl, factorExplanation, maxBoost);
        }
    }

    static class FiltersFunctionFactorScorer extends CustomBoostFactorScorer {
        private final FilterFunction[] filterFunctions;
        private final ScoreMode scoreMode;
        private final LeafScoreFunction[] functions;
        private final Bits[] docSets;
        private final boolean needsScores;

        private FiltersFunctionFactorScorer(CustomBoostFactorWeight w, Scorer scorer, ScoreMode scoreMode, FilterFunction[] filterFunctions,
                                            float maxBoost, LeafScoreFunction[] functions, Bits[] docSets, CombineFunction scoreCombiner, Float minScore, boolean needsScores) throws IOException {
            super(w, scorer, maxBoost, scoreCombiner, minScore);
            this.scoreMode = scoreMode;
            this.filterFunctions = filterFunctions;
            this.functions = functions;
            this.docSets = docSets;
            this.needsScores = needsScores;
        }

        @Override
        public float innerScore() throws IOException {
            int docId = scorer.docID();
            double factor = 1.0f;
            // Even if the weight is created with needsScores=false, it might
            // be costly to call score(), so we explicitly check if scores
            // are needed
            float subQueryScore = needsScores ? scorer.score() : 0f;
            if (scoreMode == ScoreMode.First) {
                for (int i = 0; i < filterFunctions.length; i++) {
                    if (docSets[i].get(docId)) {
                        factor = functions[i].score(docId, subQueryScore);
                        break;
                    }
                }
            } else if (scoreMode == ScoreMode.Max) {
                double maxFactor = Double.NEGATIVE_INFINITY;
                for (int i = 0; i < filterFunctions.length; i++) {
                    if (docSets[i].get(docId)) {
                        maxFactor = Math.max(functions[i].score(docId, subQueryScore), maxFactor);
                    }
                }
                if (maxFactor != Float.NEGATIVE_INFINITY) {
                    factor = maxFactor;
                }
            } else if (scoreMode == ScoreMode.Min) {
                double minFactor = Double.POSITIVE_INFINITY;
                for (int i = 0; i < filterFunctions.length; i++) {
                    if (docSets[i].get(docId)) {
                        minFactor = Math.min(functions[i].score(docId, subQueryScore), minFactor);
                    }
                }
                if (minFactor != Float.POSITIVE_INFINITY) {
                    factor = minFactor;
                }
            } else if (scoreMode == ScoreMode.Multiply) {
                for (int i = 0; i < filterFunctions.length; i++) {
                    if (docSets[i].get(docId)) {
                        factor *= functions[i].score(docId, subQueryScore);
                    }
                }
            } else { // Avg / Total
                double totalFactor = 0.0f;
                float weightSum = 0;
                for (int i = 0; i < filterFunctions.length; i++) {
                    if (docSets[i].get(docId)) {
                        totalFactor += functions[i].score(docId, subQueryScore);
                        if (filterFunctions[i].function instanceof WeightFactorFunction) {
                            weightSum+= ((WeightFactorFunction)filterFunctions[i].function).getWeight();
                        } else {
                            weightSum++;
                        }
                    }
                }
                if (weightSum != 0) {
                    factor = totalFactor;
                    if (scoreMode == ScoreMode.Avg) {
                        factor /= weightSum;
                    }
                }
            }
            return scoreCombiner.combine(subQueryScore, factor, maxBoost);
        }
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("function score (").append(subQuery.toString(field)).append(", functions: [");
        for (FilterFunction filterFunction : filterFunctions) {
            sb.append("{filter(").append(filterFunction.filter).append("), function [").append(filterFunction.function).append("]}");
        }
        sb.append("])");
        sb.append(ToStringUtils.boost(getBoost()));
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        FiltersFunctionScoreQuery other = (FiltersFunctionScoreQuery) o;
        if (this.getBoost() != other.getBoost())
            return false;
        if (!this.subQuery.equals(other.subQuery)) {
            return false;
        }
        return Arrays.equals(this.filterFunctions, other.filterFunctions);
    }

    @Override
    public int hashCode() {
        return subQuery.hashCode() + 31 * Arrays.hashCode(filterFunctions) ^ Float.floatToIntBits(getBoost());
    }
}
