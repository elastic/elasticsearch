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
import org.elasticsearch.plugins.analysis.SimpleAnalyzeIterator;

import java.util.ArrayList;
import java.util.List;

public class PluginAnalysisPipelineStep implements AnalysisPipelineStep {

    private final AnalysisIteratorFactory iteratorFactory;

    public PluginAnalysisPipelineStep(AnalysisIteratorFactory iteratorFactory) {
        this.iteratorFactory = iteratorFactory;
    }

    @Override
    public List<AnalyzeAction.AnalyzeToken> process(String field, String[] texts, int maxTokenCount) {
        return process(field, texts, maxTokenCount, null);
    }

    public List<AnalyzeAction.AnalyzeToken> process(String field, String[] texts, int maxTokenCount, String[] attributes) {
        TokenCounter tc = new TokenCounter(maxTokenCount);
        List<AnalyzeAction.AnalyzeToken> tokens = new ArrayList<>();
        AnalyzeState state = new AnalyzeState(-1, 0);

        for (String text : texts) {
            try (SimpleAnalyzeIterator iterator = iteratorFactory.newInstance(text, state)) {
                AnalyzeToken token;
                iterator.start();
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
                state = iterator.state();
            }
        }

        return tokens;
    }

    @Override
    public DetailedPipelineAnalysisPackage details(String field, String[] texts, int maxTokenCount, String[] attributes) {
        return null;
    }

    @Override
    public List<AnalyzeAction.AnalyzeTokenList> detailedFilters(String field, String[] texts, int maxTokenCount, String[] attributes) {
        List<AnalyzeAction.AnalyzeToken> tokens = process(field, texts, maxTokenCount, attributes);
        return List.of(new AnalyzeAction.AnalyzeTokenList(iteratorFactory.name(), tokens.toArray(new AnalyzeAction.AnalyzeToken[0])));
    }
}
