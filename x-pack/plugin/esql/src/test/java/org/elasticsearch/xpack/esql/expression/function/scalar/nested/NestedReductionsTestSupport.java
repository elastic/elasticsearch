/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.inference.InferenceResolution;
import org.elasticsearch.xpack.esql.parser.EsqlConfig;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.telemetry.Metrics;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/** Runs the real analyzer over field caps built by the tests. */
final class NestedReductionsTestSupport {
    private NestedReductionsTestSupport() {}

    static LogicalPlan analyze(String query, EsIndex index, Map<String, IndexResolution> lookups) {
        return analyze(query, Map.of(index.name(), index), lookups);
    }

    static LogicalPlan analyze(String query, Map<String, EsIndex> indices, Map<String, IndexResolution> lookups) {
        var registry = new EsqlFunctionRegistry().snapshotRegistry();
        var statement = new EsqlParser(new EsqlConfig(registry)).createStatement(query);
        var config = configuration(query);
        Map<IndexPattern, IndexResolution> resolutions = new LinkedHashMap<>();
        indices.forEach((name, index) -> resolutions.put(new IndexPattern(Source.EMPTY, name), IndexResolution.valid(index)));
        var context = new AnalyzerContext(
            config,
            registry,
            PromqlFunctionRegistry.INSTANCE,
            null,
            resolutions,
            lookups,
            new EnrichResolution(),
            InferenceResolution.EMPTY,
            TransportVersion.current(),
            statement.setting(QuerySettings.UNMAPPED_FIELDS)
        );
        return new Analyzer(context, new Verifier(new Metrics(registry, true, false), new XPackLicenseState(() -> 0L))).analyze(
            statement.plan()
        );
    }

    private static Configuration configuration(String query) {
        return new Configuration(
            Instant.EPOCH,
            Locale.ROOT,
            "test",
            "test",
            QueryPragmas.EMPTY,
            10000,
            1000,
            query,
            false,
            Map.of(),
            0,
            false,
            10000,
            1000,
            null,
            Map.of()
        );
    }
}
