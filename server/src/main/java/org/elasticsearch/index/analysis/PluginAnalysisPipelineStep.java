/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.plugins.analysis.AnalysisIteratorFactory;
import org.elasticsearch.plugins.analysis.AnalyzeState;
import org.elasticsearch.plugins.analysis.AnalyzeToken;
import org.elasticsearch.plugins.analysis.PortableAnalyzeIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PluginAnalysisPipelineStep implements AnalysisPipelineStep {

    private final AnalysisIteratorFactory iteratorFactory;

    public PluginAnalysisPipelineStep(AnalysisIteratorFactory iteratorFactory) {
        this.iteratorFactory = iteratorFactory;
    }

    @Override
    public List<AnalyzeAction.AnalyzeToken> process(String field, List<AnalyzeAction.AnalyzeToken> inTokens, int maxTokenCount) {
        return process(field, inTokens, maxTokenCount, null);
    }

    public List<AnalyzeAction.AnalyzeToken> process(
        String field, List<AnalyzeAction.AnalyzeToken> inTokens, int maxTokenCount, String[] attributes) {
        TokenCounter tc = new TokenCounter(maxTokenCount);
        List<AnalyzeAction.AnalyzeToken> tokens = new ArrayList<>();
        AnalyzeState state = new AnalyzeState(-1, 0);

        List<AnalyzeToken> convertedTokens = inTokens.stream().map(
            t -> new AnalyzeToken(t.getTerm(), t.getPosition(), t.getStartOffset(), t.getEndOffset(), t.getPositionLength(), t.getType())
        ).collect(Collectors.toList());

        try (PortableAnalyzeIterator iterator = iteratorFactory.newInstance(convertedTokens, state)) {
            AnalyzeToken token;
            iterator.reset();
            while ((token = iterator.next()) != null) {
                tokens.add(new AnalyzeAction.AnalyzeToken(
                    token.getTerm(),
                    token.getPosition(),
                    token.getStartOffset(),
                    token.getEndOffset(),
                    token.getPositionLength(),
                    token.getType(),
                    null
                ));

                tc.increment();
            }
            iterator.end();
        }


        return tokens;
    }

    @Override
    public List<AnalyzeAction.AnalyzeTokenList> detailedFilters(
        String field, List<AnalyzeAction.AnalyzeToken> inTokens, int maxTokenCount, String[] attributes) {
        List<AnalyzeAction.AnalyzeToken> tokens = process(field, inTokens, maxTokenCount, attributes);
        return List.of(new AnalyzeAction.AnalyzeTokenList(iteratorFactory.name(), tokens.toArray(new AnalyzeAction.AnalyzeToken[0])));
    }
}
