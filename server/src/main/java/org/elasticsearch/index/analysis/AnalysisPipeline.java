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
    private final AnalysisPipelineFirstStep firstStep;
    private final List<AnalysisPipelineStep> steps;

    public AnalysisPipeline(AnalysisPipelineFirstStep firstStep, List<AnalysisPipelineStep> steps) {
        this.firstStep = firstStep;
        this.steps = steps;
    }

    private Map<Integer, Integer> nextInput(List<AnalyzeAction.AnalyzeToken> tokens, Map<Integer, Integer> prevMappings) {
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
        if (firstStep == null) {
            return new ArrayList<>();
        }

        List<AnalyzeAction.AnalyzeToken> result;
        Iterator<AnalysisPipelineStep> stepIterator = steps.listIterator();

        result = firstStep.process(field, texts, maxTokenCount);

        if (stepIterator.hasNext()) {
            Map<Integer, AnalyzeAction.AnalyzeToken> firstTokens = new HashMap<>();

            for (AnalyzeAction.AnalyzeToken token : result) {
                firstTokens.put(token.getPosition(), token);
            }

            Map<Integer, Integer> mappings = nextInput(result, null);

            while (stepIterator.hasNext()) {
                result = stepIterator.next().process(field, result, maxTokenCount);

                mappings = nextInput(result, mappings);
            }

            List<AnalyzeAction.AnalyzeToken> adjusted = remap(mappings, firstTokens, result);

            return adjusted;
        }

        return result;
    }

    public DetailedPipelineAnalysisPackage details(String field, String[] texts, int maxTokenCount, String[] attributes) {
        if (firstStep == null) {
            return null;
        }

        Iterator<AnalysisPipelineStep> stepIterator = steps.listIterator();

        DetailedPipelineAnalysisPackage result = firstStep.details(field, texts, maxTokenCount, attributes);

        if (stepIterator.hasNext()) {
            Map<Integer, AnalyzeAction.AnalyzeToken> firstTokens = new HashMap<>();
            AnalyzeAction.AnalyzeTokenList lastFilterResult = result.getTokenizer();

            if (result.getTokenFilters().isEmpty() == false) {
                lastFilterResult = result.getTokenFilters().get(result.getTokenFilters().size() - 1);
            }


            for (AnalyzeAction.AnalyzeToken token : lastFilterResult.getTokens()) {
                firstTokens.put(token.getPosition(), token);
            }

            Map<Integer, Integer> mappings = nextInput(List.of(lastFilterResult.getTokens()), null);

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

                mappings = nextInput(List.of(last.getTokens()), mappings);

                lastFilterResult = result.getTokenFilters().get(result.getTokenFilters().size() - 1);
            }
        }
        return result;
    }
}
