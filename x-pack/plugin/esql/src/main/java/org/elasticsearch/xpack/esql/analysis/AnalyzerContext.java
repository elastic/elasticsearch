/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.esql.version.BehaviorFlag;
import org.elasticsearch.xpack.esql.version.EsqlVersion;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.index.IndexResolution;

import java.util.Set;

public record AnalyzerContext(
    EsqlConfiguration configuration,
    FunctionRegistry functionRegistry,
    IndexResolution indexResolution,
    EnrichResolution enrichResolution,
    Set<BehaviorFlag> behaviorFlags
) {
    public AnalyzerContext(
        EsqlConfiguration configuration,
        FunctionRegistry functionRegistry,
        IndexResolution indexResolution,
        EnrichResolution enrichResolution
    ) {
        this(configuration, functionRegistry, indexResolution, enrichResolution, EsqlVersion.SNAPSHOT.behaviorTags());
    }
}
