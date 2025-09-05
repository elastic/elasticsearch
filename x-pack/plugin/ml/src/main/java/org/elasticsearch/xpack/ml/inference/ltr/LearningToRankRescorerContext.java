/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.ltr;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.Rescorer;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearningToRankConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.LearningToRankFeatureExtractorBuilder;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.QueryExtractorBuilder;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.function.Predicate.not;

public class LearningToRankRescorerContext extends RescoreContext {

    final SearchExecutionContext executionContext;
    final LocalModel regressionModelDefinition;
    final LearningToRankConfig learningToRankConfig;

    /**
     * @param windowSize how many documents to rescore
     * @param rescorer The rescorer to apply
     * @param learningToRankConfig The inference config containing updated and rewritten parameters
     * @param regressionModelDefinition The local model inference definition, may be null during certain search phases.
     * @param executionContext The local shard search context
     */
    public LearningToRankRescorerContext(
        int windowSize,
        Rescorer rescorer,
        LearningToRankConfig learningToRankConfig,
        LocalModel regressionModelDefinition,
        SearchExecutionContext executionContext
    ) {
        super(windowSize, rescorer);
        this.executionContext = executionContext;
        this.regressionModelDefinition = regressionModelDefinition;
        this.learningToRankConfig = learningToRankConfig;
    }

    List<FeatureExtractor> buildFeatureExtractors(IndexSearcher searcher) throws IOException {
        assert this.regressionModelDefinition != null && this.learningToRankConfig != null;

        List<FeatureExtractor> featureExtractors = new ArrayList<>();

        List<Weight> weights = new ArrayList<>();
        List<String> queryFeatureNames = new ArrayList<>();
        for (LearningToRankFeatureExtractorBuilder featureExtractorBuilder : learningToRankConfig.getFeatureExtractorBuilders()) {
            if (featureExtractorBuilder instanceof QueryExtractorBuilder queryExtractorBuilder) {
                Query query = executionContext.toQuery(queryExtractorBuilder.query().getParsedQuery()).query();
                Weight weight = searcher.rewrite(query).createWeight(searcher, ScoreMode.COMPLETE, 1f);
                weights.add(weight);
                queryFeatureNames.add(queryExtractorBuilder.featureName());
            }
        }
        if (weights.isEmpty() == false) {
            featureExtractors.add(new QueryFeatureExtractor(queryFeatureNames, weights));
        }

        List<String> fieldValueExtractorFields = this.regressionModelDefinition.inputFields()
            .stream()
            .filter(not(queryFeatureNames::contains))
            .toList();
        if (fieldValueExtractorFields.isEmpty() == false) {
            featureExtractors.add(new FieldValueFeatureExtractor(fieldValueExtractorFields, this.executionContext));
        }

        return featureExtractors;
    }

    @Override
    public List<ParsedQuery> getParsedQueries() {
        if (this.learningToRankConfig == null) {
            return List.of();
        }
        List<ParsedQuery> parsedQueries = new ArrayList<>();
        for (LearningToRankFeatureExtractorBuilder featureExtractorBuilder : learningToRankConfig.getFeatureExtractorBuilders()) {
            if (featureExtractorBuilder instanceof QueryExtractorBuilder queryExtractorBuilder) {
                parsedQueries.add(executionContext.toQuery(queryExtractorBuilder.query().getParsedQuery()));
            }
        }
        return parsedQueries;
    }
}
