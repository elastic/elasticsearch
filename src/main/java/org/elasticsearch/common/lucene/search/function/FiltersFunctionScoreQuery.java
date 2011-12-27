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

package org.elasticsearch.common.lucene.search.function;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.ToStringUtils;
import org.elasticsearch.common.lucene.docset.DocSet;
import org.elasticsearch.common.lucene.docset.DocSets;

import java.io.IOException;
import java.util.*;

/**
 * A query that allows for a pluggable boost function / filter. If it matches the filter, it will
 * be boosted by the formula.
 */
public class FiltersFunctionScoreQuery extends Query {

    public static abstract class FilterItem {
    }

    public static class FilterScoreGroup extends FilterItem {
        public final ScoreMode scoreMode;
        public final FilterItem[] filterItems;

        public FilterScoreGroup(ScoreMode scoreMode, FilterItem[] filterItems) {
            this.scoreMode = scoreMode;
            this.filterItems = filterItems;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("filter score group (").append(scoreMode.toString()).append(", children: [");
            for (FilterItem filterItem : filterItems) {
                sb.append("{" + filterItem.toString() + "}");
            }
            sb.append("])");
            return sb.toString();
        }


        @Override
        public boolean equals(Object o) {
            if (getClass() != o.getClass()) return false;
            FilterScoreGroup other = (FilterScoreGroup) o;
            if (this.scoreMode != other.scoreMode)
                return false;
            return Arrays.equals(this.filterItems, other.filterItems);
        }

        @Override
        public int hashCode() {
            return scoreMode.hashCode() + 31 * Arrays.hashCode(filterItems);
        }
    }

    public static class FilterFunction extends FilterItem {
        public final Filter filter;
        public final ScoreFunction function;

        public FilterFunction(Filter filter, ScoreFunction function) {
            this.filter = filter;
            this.function = function;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FilterFunction that = (FilterFunction) o;

            if (filter != null ? !filter.equals(that.filter) : that.filter != null) return false;
            if (function != null ? !function.equals(that.function) : that.function != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = filter != null ? filter.hashCode() : 0;
            result = 31 * result + (function != null ? function.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("filter(").append(filter).append("), function [").append(function).append("]");
            return sb.toString();
        }
    }

    public static enum ScoreMode {First, Avg, Max, Total, Min, Multiply}

    Query subQuery;
    Map<FilterFunction, DocSet> docSets = new HashMap<FilterFunction, DocSet>();  // TODO: something better than a map?
    FilterScoreGroup filters;

    public FiltersFunctionScoreQuery(Query subQuery, ScoreMode scoreMode, FilterFunction[] filterFunctions) {
        this.subQuery = subQuery;
        this.filters = new FilterScoreGroup(scoreMode, filterFunctions);
    }

    public FiltersFunctionScoreQuery(Query subQuery, FilterScoreGroup rootFilterGroup) {
        this.subQuery = subQuery;
        this.filters = rootFilterGroup;
    }


    public Query getSubQuery() {
        return subQuery;
    }

    // 26-Dec-2011 NOTE:  breaking API change from getFilterFunctions() : FilterFunction -- which was unused method in
    // ES code base, but not necessarily unused for users of ES
    public FilterScoreGroup getFilterFunctions() {
        return filters;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query newQ = subQuery.rewrite(reader);
        if (newQ == subQuery) return this;
        FiltersFunctionScoreQuery bq = (FiltersFunctionScoreQuery) this.clone();
        bq.subQuery = newQ;
        return bq;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
        subQuery.extractTerms(terms);
    }

    @Override
    public Weight createWeight(Searcher searcher) throws IOException {
        return new CustomBoostFactorWeight(searcher);
    }

    class CustomBoostFactorWeight extends Weight {
        Searcher searcher;
        Weight subQueryWeight;

        public CustomBoostFactorWeight(Searcher searcher) throws IOException {
            this.searcher = searcher;
            this.subQueryWeight = subQuery.weight(searcher);
        }

        @Override
        public Query getQuery() {
            return FiltersFunctionScoreQuery.this;
        }

        @Override
        public float getValue() {
            return getBoost();
        }

        @Override
        public float sumOfSquaredWeights() throws IOException {
            float sum = subQueryWeight.sumOfSquaredWeights();
            sum *= getBoost() * getBoost();
            return sum;
        }

        @Override
        public void normalize(float norm) {
            norm *= getBoost();
            subQueryWeight.normalize(norm);
        }

        @Override
        public Scorer scorer(IndexReader reader, boolean scoreDocsInOrder, boolean topScorer) throws IOException {
            Scorer subQueryScorer = subQueryWeight.scorer(reader, scoreDocsInOrder, false);
            if (subQueryScorer == null) {
                return null;
            }
            initFilterFunctionDocSets(reader, filters);
            return new CustomBoostFactorScorer(getSimilarity(searcher), this, subQueryScorer, filters, docSets);
        }

        private void initFilterFunctionDocSets(IndexReader reader, FilterScoreGroup group) throws IOException {
            for (FilterItem item : group.filterItems) {
                if (item instanceof FilterScoreGroup) {
                    initFilterFunctionDocSets(reader, (FilterScoreGroup) item);
                } else if (item instanceof FilterFunction) {
                    FilterFunction filterFunction = (FilterFunction) item;
                    if (!docSets.containsKey(filterFunction)) {
                        filterFunction.function.setNextReader(reader);
                        docSets.put(filterFunction, DocSets.convert(reader, filterFunction.filter.getDocIdSet(reader)));
                    }
                }
            }
        }

        @Override
        public Explanation explain(IndexReader reader, int doc) throws IOException {
            Explanation subQueryExpl = subQueryWeight.explain(reader, doc);
            if (!subQueryExpl.isMatch()) {
                return subQueryExpl;
            }

            return explainGroup(reader, doc, subQueryExpl, filters, true);
        }

        private Explanation explainGroup(IndexReader reader, int doc, Explanation subQueryExpl, FilterScoreGroup group, boolean outerGroup) throws IOException {
            Float baseScore = (outerGroup ? getValue() : 1.0F);

            if (group.scoreMode == ScoreMode.First) {
                for (FilterItem item : group.filterItems) {
                    if (item instanceof FilterScoreGroup) {
                        Explanation subGroupExplain = explainGroup(reader, doc, subQueryExpl, (FilterScoreGroup) item, false);
                        if (!(subGroupExplain instanceof NoMatchExplanation)) {
                            float sc = baseScore * subGroupExplain.getValue();
                            Explanation res = new ComplexExplanation(true, sc, "custom score, product of:");
                            res.addDetail(new Explanation(1.0f, "score group: " + group.toString()));
                            res.addDetail(subGroupExplain);
                            res.addDetail(new Explanation(baseScore, "queryBoost"));
                            return res;
                        }
                    } else if (item instanceof FilterFunction) {
                        FilterFunction filterFunction = (FilterFunction) item;

                        DocSet docSet = DocSets.convert(reader, filterFunction.filter.getDocIdSet(reader));
                        if (docSet.get(doc)) {
                            filterFunction.function.setNextReader(reader);
                            Explanation functionExplanation = filterFunction.function.explain(doc, subQueryExpl);
                            float sc = baseScore * functionExplanation.getValue();
                            Explanation res = new ComplexExplanation(true, sc, "custom score, product of:");
                            res.addDetail(new Explanation(1.0f, "match filter: " + filterFunction.filter.toString()));
                            res.addDetail(functionExplanation);
                            res.addDetail(new Explanation(baseScore, "queryBoost"));
                            return res;
                        }
                    }
                }
            } else {
                int count = 0;
                float total = 0;
                float multiply = 1;
                float max = Float.NEGATIVE_INFINITY;
                float min = Float.POSITIVE_INFINITY;
                ArrayList<Explanation> filtersExplanations = new ArrayList<Explanation>();
                for (FilterItem item : group.filterItems) {
                    if (item instanceof FilterScoreGroup) {
                        Explanation subGroupExplain = explainGroup(reader, doc, subQueryExpl, (FilterScoreGroup) item, false);
                        if (!(subGroupExplain instanceof NoMatchExplanation)) {
                            float sc = subGroupExplain.getValue();
                            count++;
                            total += sc;
                            multiply *= sc;
                            max = Math.max(sc, max);
                            min = Math.min(sc, min);
                            Explanation res = new ComplexExplanation(true, sc, "custom score, product of:");
                            res.addDetail(new Explanation(1.0f, "score group: " + group.toString()));
                            res.addDetail(subGroupExplain);
                            res.addDetail(new Explanation(baseScore, "queryBoost"));
                            filtersExplanations.add(res);
                        }
                    } else if (item instanceof FilterFunction) {
                        FilterFunction filterFunction = (FilterFunction) item;
                        DocSet docSet = DocSets.convert(reader, filterFunction.filter.getDocIdSet(reader));
                        if (docSet.get(doc)) {
                            filterFunction.function.setNextReader(reader);
                            Explanation functionExplanation = filterFunction.function.explain(doc, subQueryExpl);
                            float sc = functionExplanation.getValue();
                            count++;
                            total += sc;
                            multiply *= sc;
                            max = Math.max(sc, max);
                            min = Math.min(sc, min);
                            Explanation res = new ComplexExplanation(true, sc, "custom score, product of:");
                            res.addDetail(new Explanation(1.0f, "match filter: " + filterFunction.filter.toString()));
                            res.addDetail(functionExplanation);
                            res.addDetail(new Explanation(baseScore, "queryBoost"));
                            filtersExplanations.add(res);
                        }
                    }
                }
                if (count > 0) {
                    float sc = 0;
                    switch (group.scoreMode) {
                        case Avg:
                            sc = total / count;
                            break;
                        case Max:
                            sc = max;
                            break;
                        case Min:
                            sc = min;
                            break;
                        case Total:
                            sc = total;
                            break;
                        case Multiply:
                            sc = multiply;
                            break;
                    }
                    sc *= baseScore;
                    Explanation res = new ComplexExplanation(true, sc, "custom score, score mode [" + group.scoreMode.toString().toLowerCase() + "]");
                    for (Explanation explanation : filtersExplanations) {
                        res.addDetail(explanation);
                    }
                    return res;
                }
            }

            float sc = baseScore * subQueryExpl.getValue();
            Explanation res = new NoMatchExplanation(sc);
            res.addDetail(subQueryExpl);
            res.addDetail(new Explanation(baseScore, "queryBoost"));
            return res;
        }

        public class NoMatchExplanation extends ComplexExplanation {
            public NoMatchExplanation(float score) {
                super(true, score, "custom score, no filter match, product of:");
            }
        }
    }


    static class CustomBoostFactorScorer extends Scorer {
        private final float subQueryWeight;
        private final Scorer scorer;
        private final FilterScoreGroup filters;
        private final Map<FilterFunction, DocSet> docSets;


        private CustomBoostFactorScorer(Similarity similarity, CustomBoostFactorWeight w, Scorer scorer,
                                        FilterScoreGroup filters, Map<FilterFunction, DocSet> docSets) throws IOException {
            super(similarity);
            this.subQueryWeight = w.getValue();
            this.scorer = scorer;
            this.filters = filters;
            this.docSets = docSets;
        }

        @Override
        public int docID() {
            return scorer.docID();
        }

        @Override
        public int advance(int target) throws IOException {
            return scorer.advance(target);
        }

        @Override
        public int nextDoc() throws IOException {
            return scorer.nextDoc();
        }

        @Override
        public float score() throws IOException {
            int docId = scorer.docID();
            float score = scorer.score();
            score = score(filters, docId, score);
            return subQueryWeight * score;
        }

        private float score(FilterScoreGroup group, int docId, float score) {
            float newScore = scoreOnlyMatchingFilters(group, docId, score);
            return newScore != Float.NEGATIVE_INFINITY ? newScore : score;
        }

        private float scoreOnlyMatchingFilters(FilterScoreGroup group, int docId, float subqueryScore) {
            if (group.scoreMode == ScoreMode.First) {
                for (FilterItem item : group.filterItems) {
                    if (item instanceof FilterScoreGroup) {
                        float tempScore = scoreOnlyMatchingFilters((FilterScoreGroup) item, docId, subqueryScore);
                        if (tempScore != Float.NEGATIVE_INFINITY) {
                            return tempScore;
                        }
                    } else {
                        FilterFunction filterFunction = (FilterFunction) item;
                        DocSet docSet = docSets.get(filterFunction);
                        if (docSet != null && docSet.get(docId)) {
                            return filterFunction.function.score(docId, subqueryScore);
                        }
                    }
                }
            } else if (group.scoreMode == ScoreMode.Max) {
                float maxScore = Float.NEGATIVE_INFINITY;
                for (FilterItem item : group.filterItems) {
                    if (item instanceof FilterScoreGroup) {
                        maxScore = Math.max(scoreOnlyMatchingFilters((FilterScoreGroup) item, docId, subqueryScore), maxScore);
                    } else {
                        FilterFunction filterFunction = (FilterFunction) item;
                        DocSet docSet = docSets.get(filterFunction);
                        if (docSet != null && docSet.get(docId)) {
                            maxScore = Math.max(filterFunction.function.score(docId, subqueryScore), maxScore);
                        }
                    }
                }
                if (maxScore != Float.NEGATIVE_INFINITY) {
                    return maxScore;
                }
            } else if (group.scoreMode == ScoreMode.Min) {
                float minScore = Float.POSITIVE_INFINITY;
                for (FilterItem item : group.filterItems) {
                    if (item instanceof FilterScoreGroup) {
                        float tempScore = scoreOnlyMatchingFilters((FilterScoreGroup) item, docId, subqueryScore);
                        if (tempScore != Float.NEGATIVE_INFINITY) {
                            minScore = Math.min(tempScore, minScore);
                        }
                    } else {
                        FilterFunction filterFunction = (FilterFunction) item;
                        DocSet docSet = docSets.get(filterFunction);
                        if (docSet != null && docSet.get(docId)) {
                            minScore = Math.min(filterFunction.function.score(docId, subqueryScore), minScore);
                        }
                    }
                }
                if (minScore != Float.POSITIVE_INFINITY) {
                    return minScore;
                }
            } else { // Avg / Total / multiply
                float totalScore = 0.0f;
                float multiplicativeScore = 1.0f;
                int count = 0;
                for (FilterItem item : group.filterItems) {
                    if (item instanceof FilterScoreGroup) {
                        float tempScore = scoreOnlyMatchingFilters((FilterScoreGroup) item, docId, subqueryScore);
                        if (tempScore != Float.NEGATIVE_INFINITY) {
                            totalScore += tempScore;
                            multiplicativeScore *= tempScore;
                            count++;
                        }
                    } else {
                        FilterFunction filterFunction = (FilterFunction) item;
                        DocSet docSet = docSets.get(filterFunction);
                        if (docSet != null && docSet.get(docId)) {
                            float tempScore = filterFunction.function.score(docId, subqueryScore);
                            totalScore += tempScore;
                            multiplicativeScore *= tempScore;
                            count++;
                        }
                    }
                }
                if (count != 0) {
                    if (group.scoreMode == ScoreMode.Avg) {
                        return totalScore / count;
                    } else if (group.scoreMode == ScoreMode.Multiply) {
                        return multiplicativeScore;
                    }
                    return totalScore;
                }
            }
            return Float.NEGATIVE_INFINITY;
        }
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("custom score (").append(subQuery.toString(field)).append(", functions: [");
        sb.append("{" + filters.toString() + "}");
        sb.append("])");
        sb.append(ToStringUtils.boost(getBoost()));
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (getClass() != o.getClass()) return false;
        FiltersFunctionScoreQuery other = (FiltersFunctionScoreQuery) o;
        if (this.getBoost() != other.getBoost())
            return false;
        if (!this.subQuery.equals(other.subQuery)) {
            return false;
        }
        return filters.equals(other.filters);
    }

    @Override
    public int hashCode() {
        return subQuery.hashCode() + 31 * filters.hashCode() ^ Float.floatToIntBits(getBoost());
    }
}

