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

    public List<AnalyzeAction.AnalyzeToken> process(String field, String[] texts, int maxTokenCount) {
        if (steps.isEmpty()) {
            return new ArrayList<>();
        }

        List<AnalyzeAction.AnalyzeToken> result;
        Iterator<AnalysisPipelineStep> stepIterator = steps.listIterator();

        result = stepIterator.next().process(field, texts, maxTokenCount);

        if (stepIterator.hasNext()) {
            Map<Integer, AnalyzeAction.AnalyzeToken> firstTokens = new HashMap<>();
            Map<Integer, Integer> indexMappings = new HashMap<>();
            StringBuilder stringBuilder = new StringBuilder();

            int index = 0;
            for (AnalyzeAction.AnalyzeToken token : result) {
                firstTokens.put(token.getPosition(), token);

                indexMappings.put(index++, token.getPosition());
                stringBuilder.append(token.getTerm()).append(' ');
            }
            stringBuilder.setLength(stringBuilder.length() - 1);

            while (stepIterator.hasNext()) {
                result = stepIterator.next().process(field, new String[]{ stringBuilder.toString() }, maxTokenCount);

                Map<Integer, Integer> nextIndexMappings = new HashMap<>();
                index = 0;
                stringBuilder.setLength(0);
                for (AnalyzeAction.AnalyzeToken token : result) {
                    Integer prevMapping = indexMappings.get(token.getPosition());
                    nextIndexMappings.put(index++, prevMapping);
                    stringBuilder.append(token.getTerm()).append(' ');
                }
                stringBuilder.setLength(stringBuilder.length() - 1);
                indexMappings = nextIndexMappings;
            }

            List<AnalyzeAction.AnalyzeToken> mapped = new ArrayList<>();

            for (AnalyzeAction.AnalyzeToken token : result) {
                Integer mappingIndex = indexMappings.get(token.getPosition());
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
                        mapped.add(mappedToken);
                        continue;
                    }
                }
                mapped.add(token);
            }

            return mapped;
        }
        return result;
    }
}
