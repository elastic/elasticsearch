/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.enrich.ResolvedEnrichPolicy;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.enrich.EnrichPolicy.GEO_MATCH_TYPE;
import static org.elasticsearch.xpack.core.enrich.EnrichPolicy.MATCH_TYPE;
import static org.elasticsearch.xpack.core.enrich.EnrichPolicy.RANGE_TYPE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;

public final class AnalyzerTestUtils {

    private AnalyzerTestUtils() {}

    public static Analyzer defaultAnalyzer() {
        return analyzer(analyzerDefaultMapping());
    }

    public static Analyzer expandedDefaultAnalyzer() {
        return analyzer(expandedDefaultIndexResolution());
    }

    public static Analyzer analyzer(IndexResolution indexResolution) {
        return analyzer(indexResolution, TEST_VERIFIER);
    }

    public static Analyzer analyzer(IndexResolution indexResolution, Map<String, IndexResolution> lookupResolution) {
        return analyzer(indexResolution, lookupResolution, TEST_VERIFIER);
    }

    public static Analyzer analyzer(IndexResolution indexResolution, Verifier verifier) {
        return new Analyzer(
            new AnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                indexResolution,
                defaultLookupResolution(),
                defaultEnrichResolution()
            ),
            verifier
        );
    }

    public static Analyzer analyzer(IndexResolution indexResolution, Map<String, IndexResolution> lookupResolution, Verifier verifier) {
        return new Analyzer(
            new AnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                indexResolution,
                lookupResolution,
                defaultEnrichResolution()
            ),
            verifier
        );
    }

    public static Analyzer analyzer(IndexResolution indexResolution, Verifier verifier, Configuration config) {
        return new Analyzer(
            new AnalyzerContext(config, new EsqlFunctionRegistry(), indexResolution, defaultLookupResolution(), defaultEnrichResolution()),
            verifier
        );
    }

    public static Analyzer analyzer(Verifier verifier) {
        return new Analyzer(
            new AnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                analyzerDefaultMapping(),
                defaultLookupResolution(),
                defaultEnrichResolution()
            ),
            verifier
        );
    }

    public static LogicalPlan analyze(String query) {
        return analyze(query, "mapping-basic.json");
    }

    public static LogicalPlan analyze(String query, String mapping) {
        return analyze(query, "test", mapping);
    }

    public static LogicalPlan analyze(String query, String index, String mapping) {
        return analyze(query, analyzer(loadMapping(mapping, index), TEST_VERIFIER, configuration(query)));
    }

    public static LogicalPlan analyze(String query, Analyzer analyzer) {
        var plan = new EsqlParser().createStatement(query);
        // System.out.println(plan);
        var analyzed = analyzer.analyze(plan);
        // System.out.println(analyzed);
        return analyzed;
    }

    public static LogicalPlan analyze(String query, String mapping, QueryParams params) {
        var plan = new EsqlParser().createStatement(query, params);
        var analyzer = analyzer(loadMapping(mapping, "test"), TEST_VERIFIER, configuration(query));
        return analyzer.analyze(plan);
    }

    public static IndexResolution loadMapping(String resource, String indexName, IndexMode indexMode) {
        EsIndex test = new EsIndex(indexName, EsqlTestUtils.loadMapping(resource), Map.of(indexName, indexMode));
        return IndexResolution.valid(test);
    }

    public static IndexResolution loadMapping(String resource, String indexName) {
        EsIndex test = new EsIndex(indexName, EsqlTestUtils.loadMapping(resource), Map.of(indexName, IndexMode.STANDARD));
        return IndexResolution.valid(test);
    }

    public static IndexResolution analyzerDefaultMapping() {
        return loadMapping("mapping-basic.json", "test");
    }

    public static IndexResolution expandedDefaultIndexResolution() {
        return loadMapping("mapping-default.json", "test");
    }

    public static Map<String, IndexResolution> defaultLookupResolution() {
        return Map.of("languages_lookup", loadMapping("mapping-languages.json", "languages_lookup", IndexMode.LOOKUP));
    }

    public static EnrichResolution defaultEnrichResolution() {
        EnrichResolution enrichResolution = new EnrichResolution();
        loadEnrichPolicyResolution(enrichResolution, MATCH_TYPE, "languages", "language_code", "languages_idx", "mapping-languages.json");
        loadEnrichPolicyResolution(enrichResolution, RANGE_TYPE, "client_cidr", "client_cidr", "client_cidr", "mapping-client_cidr.json");
        loadEnrichPolicyResolution(enrichResolution, RANGE_TYPE, "ages_policy", "age_range", "ages", "mapping-ages.json");
        loadEnrichPolicyResolution(enrichResolution, RANGE_TYPE, "heights_policy", "height_range", "heights", "mapping-heights.json");
        loadEnrichPolicyResolution(enrichResolution, RANGE_TYPE, "decades_policy", "date_range", "decades", "mapping-decades.json");
        loadEnrichPolicyResolution(
            enrichResolution,
            GEO_MATCH_TYPE,
            "city_boundaries",
            "city_boundary",
            "airport_city_boundaries",
            "mapping-airport_city_boundaries.json"
        );
        return enrichResolution;
    }

    public static void loadEnrichPolicyResolution(
        EnrichResolution enrich,
        String policyType,
        String policy,
        String field,
        String index,
        String mapping
    ) {
        IndexResolution indexResolution = loadMapping(mapping, index);
        List<String> enrichFields = new ArrayList<>(indexResolution.get().mapping().keySet());
        enrichFields.remove(field);
        enrich.addResolvedPolicy(
            policy,
            Enrich.Mode.ANY,
            new ResolvedEnrichPolicy(field, policyType, enrichFields, Map.of("", index), indexResolution.get().mapping())
        );
    }

    public static void loadEnrichPolicyResolution(EnrichResolution enrich, String policy, String field, String index, String mapping) {
        loadEnrichPolicyResolution(enrich, EnrichPolicy.MATCH_TYPE, policy, field, index, mapping);
    }

    public static IndexResolution tsdbIndexResolution() {
        return loadMapping("tsdb-mapping.json", "test");
    }
}
