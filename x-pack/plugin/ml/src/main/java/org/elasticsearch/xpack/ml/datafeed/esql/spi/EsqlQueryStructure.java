/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.esql.spi;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Syntax facts of one ES|QL query.
 *
 * @param settings       {@code SET name=value;} statements before the query, in order
 * @param settingsText   the exact query text of those statements, empty when there are none
 * @param sourceCommand  the leading source command as written ({@code FROM ...} or {@code TS ...}). When the parser collapses a
 *                       single subquery such as {@code FROM (FROM a | WHERE x)}, this is the inner source command ({@code FROM a}).
 *                       For a FROM with several subqueries it omits any {@code METADATA} clause
 * @param sourceKind     the kind of the leading source command
 * @param sourcePatterns every index pattern the query reads, including subqueries, one pattern per entry; lookup indices excluded
 * @param lookupIndices  indices read by {@code LOOKUP JOIN}
 * @param outerPipeline  the processing commands of the top-level pipeline, last command first, including those of a collapsed
 *                       single subquery
 */
public record EsqlQueryStructure(
    Map<String, String> settings,
    String settingsText,
    String sourceCommand,
    SourceKind sourceKind,
    List<String> sourcePatterns,
    List<String> lookupIndices,
    List<PipelineStep> outerPipeline
) {

    public enum SourceKind {
        FROM,
        TS,
        /** The query has no index source (for example {@code ROW}); {@code sourceCommand} is empty. */
        OTHER
    }

    public EsqlQueryStructure {
        settings = Map.copyOf(settings);
        Objects.requireNonNull(settingsText);
        Objects.requireNonNull(sourceCommand);
        Objects.requireNonNull(sourceKind);
        sourcePatterns = List.copyOf(sourcePatterns);
        lookupIndices = List.copyOf(lookupIndices);
        outerPipeline = List.copyOf(outerPipeline);
    }
}
