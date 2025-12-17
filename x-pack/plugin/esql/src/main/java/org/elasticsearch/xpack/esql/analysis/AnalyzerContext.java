/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.inference.InferenceResolution;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.EsqlSession;

import java.util.Map;

public class AnalyzerContext {
    private final Configuration configuration;
    private final EsqlFunctionRegistry functionRegistry;
    private final Map<IndexPattern, IndexResolution> indexResolution;
    private final Map<String, IndexResolution> lookupResolution;
    private final EnrichResolution enrichResolution;
    private final InferenceResolution inferenceResolution;
    private final TransportVersion minimumVersion;

    public AnalyzerContext(
        Configuration configuration,
        EsqlFunctionRegistry functionRegistry,
        Map<IndexPattern, IndexResolution> indexResolution,
        Map<String, IndexResolution> lookupResolution,
        EnrichResolution enrichResolution,
        InferenceResolution inferenceResolution,
        TransportVersion minimumVersion
    ) {
        this.configuration = configuration;
        this.functionRegistry = functionRegistry;
        this.indexResolution = indexResolution;
        this.lookupResolution = lookupResolution;
        this.enrichResolution = enrichResolution;
        this.inferenceResolution = inferenceResolution;
        this.minimumVersion = minimumVersion;

        assert minimumVersion != null : "AnalyzerContext must have a minimum transport version";
        assert minimumVersion.onOrBefore(TransportVersion.current())
            : "AnalyzerContext [" + minimumVersion + "] is not on or before current transport version [" + TransportVersion.current() + "]";
    }

    public Configuration configuration() {
        return configuration;
    }

    public EsqlFunctionRegistry functionRegistry() {
        return functionRegistry;
    }

    public Map<IndexPattern, IndexResolution> indexResolution() {
        return indexResolution;
    }

    public Map<String, IndexResolution> lookupResolution() {
        return lookupResolution;
    }

    public EnrichResolution enrichResolution() {
        return enrichResolution;
    }

    public InferenceResolution inferenceResolution() {
        return inferenceResolution;
    }

    public TransportVersion minimumVersion() {
        return minimumVersion;
    }

    public AnalyzerContext(Configuration configuration, EsqlFunctionRegistry functionRegistry, EsqlSession.PreAnalysisResult result) {
        this(
            configuration,
            functionRegistry,
            result.indexResolution,
            result.lookupIndices,
            result.enrichResolution,
            result.inferenceResolution,
            result.minimumTransportVersion
        );
    }
}
