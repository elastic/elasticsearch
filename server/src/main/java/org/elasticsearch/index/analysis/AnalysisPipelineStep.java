/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;

import java.util.List;

public interface AnalysisPipelineStep {
    class TokenCounter {
        private int tokenCount = 0;
        private int maxTokenCount;

        TokenCounter(int maxTokenCount) {
            this.maxTokenCount = maxTokenCount;
        }

        void increment() {
            tokenCount++;
            if (tokenCount > maxTokenCount) {
                throw new IllegalStateException(
                    "The number of tokens produced by calling _analyze has exceeded the allowed maximum of ["
                        + maxTokenCount
                        + "]."
                        + " This limit can be set by changing the [index.analyze.max_token_count] index level setting."
                );
            }
        }
    }

    List<AnalyzeAction.AnalyzeToken> process(String field, List<AnalyzeAction.AnalyzeToken> tokens, int maxTokenCount);
    List<AnalyzeAction.AnalyzeTokenList> detailedFilters(String field, List<AnalyzeAction.AnalyzeToken> tokens, int maxTokenCount, String[] attributes);
}
