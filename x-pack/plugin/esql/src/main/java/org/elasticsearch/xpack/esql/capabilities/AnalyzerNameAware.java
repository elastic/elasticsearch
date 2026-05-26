/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.capabilities;

import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.xpack.esql.common.Failures;

/**
 * Interface implemented by expressions that reference one or more named analyzers from the
 * cluster-side {@link AnalysisRegistry}, so the names can be resolved during query verification
 * on the coordinator rather than at evaluator construction on the data node.
 * <p>
 * Implementers validate their own analyzer names so that the failure can be reported against
 * the expression's own {@code Source} for accurate error positioning, with implementation-specific
 * error wording.
 */
public interface AnalyzerNameAware {

    /**
     * Validate the analyzer names this expression references against the given registry.
     * Discovered failures are appended to {@code failures}.
     */
    void validateAnalyzers(AnalysisRegistry registry, Failures failures);
}
