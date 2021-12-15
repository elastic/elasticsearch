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

public interface AnalysisPipelineFirstStep {
    DetailedPipelineAnalysisPackage details(String field, String[] texts, int maxTokenCount, String[] attributes);
    List<AnalyzeAction.AnalyzeToken> process(String field, String[] texts, int maxTokenCount);
}
