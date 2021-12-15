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

    private Map<Integer, Integer> nextStepMappings(List<AnalyzeAction.AnalyzeToken> tokens, Map<Integer, Integer> prevMappings) {
        Map<Integer, Integer> nextIndexMappings = new HashMap<>();
        int tokenIndex = 0;

        for (AnalyzeAction.AnalyzeToken token : tokens) {
            Integer prevMapping = (prevMappings == null) ? token.getPosition() : prevMappings.get(token.getPosition());
            assert prevMapping != null : "We should have found the original index mapping";
            nextIndexMappings.put(tokenIndex++, prevMapping);
        }

        return nextIndexMappings;
    }

    private List<AnalyzeAction.AnalyzeToken> remap(
        Map<Integer, Integer> mappings,
        Map<Integer, AnalyzeAction.AnalyzeToken> firstTokens,
        List<AnalyzeAction.AnalyzeToken> in) {
        List<AnalyzeAction.AnalyzeToken> adjusted = new ArrayList<>();

        for (AnalyzeAction.AnalyzeToken token : in) {
            Integer mappingIndex = mappings.get(token.getPosition());
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

    public List<AnalyzeAction.AnalyzeToken> process(String field, String[] texts, int maxTokenCount) {
        if (steps.isEmpty()) {
            return new ArrayList<>();
        }

        List<AnalyzeAction.AnalyzeToken> result;
        Iterator<AnalysisPipelineStep> stepIterator = steps.listIterator();

        AnalysisPipelineStep firstStep = stepIterator.next();

        if (firstStep instanceof AnalysisPipelineFirstStep) {
            result = ((AnalysisPipelineFirstStep)firstStep).process(field, texts, maxTokenCount);
        } else {
            throw new IllegalStateException("The first step of the analysis pipeline must be of AnalysisPipelineFirstStep type");
        }

        if (stepIterator.hasNext()) {
            Map<Integer, AnalyzeAction.AnalyzeToken> firstTokens = new HashMap<>();

            for (AnalyzeAction.AnalyzeToken token : result) {
                firstTokens.put(token.getPosition(), token);
            }

            Map<Integer, Integer> mappings = nextStepMappings(result, null);

            while (stepIterator.hasNext()) {
                result = stepIterator.next().process(field, result, maxTokenCount);

                mappings = nextStepMappings(result, mappings);
            }

            List<AnalyzeAction.AnalyzeToken> adjusted = remap(mappings, firstTokens, result);

            return adjusted;
        }

        return result;
    }

    public DetailedPipelineAnalysisPackage details(String field, String[] texts, int maxTokenCount, String[] attributes) {
        if (steps.isEmpty()) {
            return null;
        }

        Iterator<AnalysisPipelineStep> stepIterator = steps.listIterator();
        AnalysisPipelineStep firstStep = stepIterator.next();
        DetailedPipelineAnalysisPackage result;

        if (firstStep instanceof AnalysisPipelineFirstStep) {
            result = ((AnalysisPipelineFirstStep)firstStep).details(field, texts, maxTokenCount, attributes);
        } else {
            throw new IllegalStateException("The first step of the analysis pipeline must be of AnalysisPipelineFirstStep type");
        }

        if (stepIterator.hasNext()) {
            Map<Integer, AnalyzeAction.AnalyzeToken> firstTokens = new HashMap<>();
            AnalyzeAction.AnalyzeTokenList lastFilterResult = result.getTokenizer();

            if (result.getTokenFilters().isEmpty() == false) {
                lastFilterResult = result.getTokenFilters().get(result.getTokenFilters().size() - 1);
            }

            for (AnalyzeAction.AnalyzeToken token : lastFilterResult.getTokens()) {
                firstTokens.put(token.getPosition(), token);
            }

            Map<Integer, Integer> mappings = nextStepMappings(List.of(lastFilterResult.getTokens()), null);

            while (stepIterator.hasNext()) {
                List<AnalyzeAction.AnalyzeTokenList> stepList = stepIterator.next().detailedFilters(
                    field, List.of(lastFilterResult.getTokens()), maxTokenCount, attributes);

                for (AnalyzeAction.AnalyzeTokenList filterList : stepList) {
                    List<AnalyzeAction.AnalyzeToken> adjusted = remap(mappings, firstTokens, List.of(filterList.getTokens()));
                    result.getTokenFilters().add(new AnalyzeAction.AnalyzeTokenList(
                        filterList.getName(),
                        adjusted.toArray(new AnalyzeAction.AnalyzeToken[0])
                    ));
                }

                AnalyzeAction.AnalyzeTokenList last = stepList.get(stepList.size() - 1);

                mappings = nextStepMappings(List.of(last.getTokens()), mappings);

                lastFilterResult = result.getTokenFilters().get(result.getTokenFilters().size() - 1);
            }
        }

        return result;
    }
}
