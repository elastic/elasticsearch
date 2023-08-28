/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.rescorer;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.Rescorer;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearnToRankConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.LearnToRankFeatureExtractorBuilder;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.QueryExtractorBuilder;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InferenceRescorerContext extends RescoreContext {

    final SearchExecutionContext executionContext;
    final LocalModel inferenceDefinition;
    final LearnToRankConfig inferenceConfig;

    /**
     * @param windowSize how many documents to rescore
     * @param rescorer The rescorer to apply
     * @param inferenceConfig The inference config containing updated and rewritten parameters
     * @param inferenceDefinition The local model inference definition, may be null during certain search phases.
     * @param executionContext The local shard search context
     */
    public InferenceRescorerContext(
        int windowSize,
        Rescorer rescorer,
        LearnToRankConfig inferenceConfig,
        LocalModel inferenceDefinition,
        SearchExecutionContext executionContext
    ) {
        super(windowSize, rescorer);
        this.executionContext = executionContext;
        this.inferenceDefinition = inferenceDefinition;
        this.inferenceConfig = inferenceConfig;
    }

    List<FeatureExtractor> buildFeatureExtractors(IndexSearcher searcher) throws IOException {
        assert this.inferenceDefinition != null && this.inferenceConfig != null;
        List<FeatureExtractor> featureExtractors = new ArrayList<>();
        if (this.inferenceDefinition.inputFields().isEmpty() == false) {
            featureExtractors.add(
                new FieldValueFeatureExtractor(new ArrayList<>(this.inferenceDefinition.inputFields()), this.executionContext)
            );
        }
        List<Weight> weights = new ArrayList<>();
        List<String> queryFeatureNames = new ArrayList<>();
        for (LearnToRankFeatureExtractorBuilder featureExtractorBuilder : inferenceConfig.getFeatureExtractorBuilders()) {
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

        return featureExtractors;
    }

    @Override
    public List<ParsedQuery> getParsedQueries() {
        if (this.inferenceConfig == null) {
            return List.of();
        }
        List<ParsedQuery> parsedQueries = new ArrayList<>();
        for (LearnToRankFeatureExtractorBuilder featureExtractorBuilder : inferenceConfig.getFeatureExtractorBuilders()) {
            if (featureExtractorBuilder instanceof QueryExtractorBuilder queryExtractorBuilder) {
                parsedQueries.add(executionContext.toQuery(queryExtractorBuilder.query().getParsedQuery()));
            }
        }
        return parsedQueries;
    }
}
