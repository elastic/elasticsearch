/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class AnalysisPipeline {
    private final List<AnalysisPipelineStep> steps;

    public AnalysisPipeline(List<AnalysisPipelineStep> steps) {
        this.steps = steps;
    }

    private static class StepTransform {
        private final Map<Integer, Integer> mappings;
        private final String text;

        StepTransform(Map<Integer, Integer> mappings, String text) {
            this.mappings = mappings;
            this.text = text;
        }
    }

    private StepTransform nextInput(List<AnalyzeAction.AnalyzeToken> tokens, Map<Integer, Integer> prevMappings) {
        Map<Integer, Integer> nextIndexMappings = new HashMap<>();
        int tokenIndex = 0;
        StringBuilder textBuilder = new StringBuilder();

        for (AnalyzeAction.AnalyzeToken token : tokens) {
            Integer prevMapping = (prevMappings == null) ? token.getPosition() : prevMappings.get(token.getPosition());
            assert prevMapping != null : "We should have found the original index mapping";
            nextIndexMappings.put(tokenIndex++, prevMapping);
            textBuilder.append(token.getTerm()).append(' ');
        }
        textBuilder.setLength(textBuilder.length() - 1);

        return new StepTransform(nextIndexMappings, textBuilder.toString());
    }

    public List<AnalyzeAction.AnalyzeToken> process(String field, String[] texts, int maxTokenCount) {
        if (steps.isEmpty()) {
            return new ArrayList<>();
        }

        List<AnalyzeAction.AnalyzeToken> result;
        Iterator<AnalysisPipelineStep> stepIterator = steps.listIterator();

        result = stepIterator.next().process(field, texts, maxTokenCount);

        if (stepIterator.hasNext()) {
            Map<Integer, AnalyzeAction.AnalyzeToken> firstTokens = new HashMap<>();

            for (AnalyzeAction.AnalyzeToken token : result) {
                firstTokens.put(token.getPosition(), token);
            }

            StepTransform nextInput = nextInput(result, null);

            while (stepIterator.hasNext()) {
                result = stepIterator.next().process(field, new String[]{ nextInput.text }, maxTokenCount);

                nextInput = nextInput(result, nextInput.mappings);
            }

            List<AnalyzeAction.AnalyzeToken> adjusted = new ArrayList<>();

            for (AnalyzeAction.AnalyzeToken token : result) {
                Integer mappingIndex = nextInput.mappings.get(token.getPosition());
                if (mappingIndex != null) {
                    AnalyzeAction.AnalyzeToken original = firstTokens.get(mappingIndex);
                    if (original != null) {
                        AnalyzeAction.AnalyzeToken mappedToken = new AnalyzeAction.AnalyzeToken(
                            token.getTerm(),
                            original.getPosition(),
                            original.getStartOffset(),
                            original.getEndOffset(),
                            token.getPositionLength(),
                            token.getType(),
                            token.getAttributes()
                        );
                        adjusted.add(mappedToken);
                        continue;
                    } else {
                        assert false : "We should have a token in our map based on the original index";
                    }
                } else {
                    assert false : "We should have found the original index mapping";
                }
                adjusted.add(token);
            }

            return adjusted;
        }
        return result;
    }
}
