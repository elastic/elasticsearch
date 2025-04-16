/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.inference.InferenceResolution;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.Map;

public record AnalyzerContext(
    Configuration configuration,
    EsqlFunctionRegistry functionRegistry,
    IndexResolution indexResolution,
    Map<String, IndexResolution> lookupResolution,
    EnrichResolution enrichResolution,
    InferenceResolution inferenceResolution
) {
    // Currently for tests only, since most do not test lookups
    // TODO: make this even simpler, remove the enrichResolution for tests that do not require it (most tests)
    public AnalyzerContext(
        Configuration configuration,
        EsqlFunctionRegistry functionRegistry,
        IndexResolution indexResolution,
        EnrichResolution enrichResolution,
        InferenceResolution inferenceResolution
    ) {
        this(configuration, functionRegistry, indexResolution, Map.of(), enrichResolution, inferenceResolution);
    }
}
