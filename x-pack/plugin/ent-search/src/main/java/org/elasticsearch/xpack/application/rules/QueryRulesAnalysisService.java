/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;

public class QueryRulesAnalysisService {

    private static final TimeValue TIMEOUT_MS = TimeValue.timeValueMillis(1000);

    private static final Logger logger = LogManager.getLogger(QueryRulesAnalysisService.class);

    private final Client clientWithOrigin;

    public QueryRulesAnalysisService(Client client) {
        this.clientWithOrigin = new OriginSettingClient(client, ENT_SEARCH_ORIGIN);
    }

    private String analyze(String text, QueryRulesAnalysisConfig analysisConfig) {
        String analyzer = analysisConfig.analyzer();
        String tokenizer = analysisConfig.tokenizer();
        List<String> filters = analysisConfig.filters();
        AnalyzeAction.Request analyzeRequest = new AnalyzeAction.Request().text(text);
        if (analysisConfig.index() != null) {
            analyzeRequest.index(analysisConfig.index());
        }
        if (analyzer != null) {
            analyzeRequest.analyzer(analyzer);
        }
        if (tokenizer != null) {
            analyzeRequest.tokenizer(tokenizer);
        }
        if (filters != null) {
            analyzeRequest.addTokenFilters(filters);
        }
        // TODO - this was OK for a POC but needs a better implementation for prod
        AnalyzeAction.Response analyzeResponse = clientWithOrigin.execute(AnalyzeAction.INSTANCE, analyzeRequest).actionGet(TIMEOUT_MS);
        List<AnalyzeAction.AnalyzeToken> analyzeTokens = analyzeResponse.getTokens();
        return analyzeTokens.stream().map(AnalyzeAction.AnalyzeToken::getTerm).collect(Collectors.joining(" "));
    }

    public AnalyzedContent analyzeContent(List<Map<String, Object>> analysisChain, String input, String criteriaValue) {
        String analyzedInput = input;
        String analyzedCriteriaValue = criteriaValue;
        for (Map<String, Object> analysisConfig : analysisChain) {
            QueryRulesAnalysisConfig analysisConfigObj = QueryRulesAnalysisConfig.fromMap(analysisConfig);
            analyzedInput = analyze(analyzedInput, analysisConfigObj);
            analyzedCriteriaValue = analyze(analyzedCriteriaValue, analysisConfigObj);
            logger.info("analyzedInput: " + analyzedInput + "; analyzedCriteriaValue: " + analyzedCriteriaValue);
        }
        return new AnalyzedContent(analyzedInput, analyzedCriteriaValue);
    }

    public record AnalyzedContent(String analyzedInput, String analyzedCriteriaValue) {}

}
