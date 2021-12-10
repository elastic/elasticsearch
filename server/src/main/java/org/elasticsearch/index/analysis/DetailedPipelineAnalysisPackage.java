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

public class DetailedPipelineAnalysisPackage {
    private final List<AnalyzeAction.CharFilteredText> charFilters;
    private final AnalyzeAction.AnalyzeTokenList tokenizer;
    private final List<AnalyzeAction.AnalyzeTokenList> tokenFilters;

    DetailedPipelineAnalysisPackage(
        AnalyzeAction.AnalyzeTokenList tokenizer,
        List<AnalyzeAction.CharFilteredText> charFilters,
        List<AnalyzeAction.AnalyzeTokenList> tokenFilters
    ) {
        this.charFilters = charFilters;
        this.tokenizer = tokenizer;
        this.tokenFilters = tokenFilters;
    }

    public List<AnalyzeAction.CharFilteredText> getCharFilters() {
        return charFilters;
    }

    public AnalyzeAction.AnalyzeTokenList getTokenizer() {
        return tokenizer;
    }

    public List<AnalyzeAction.AnalyzeTokenList> getTokenFilters() {
        return tokenFilters;
    }
}
