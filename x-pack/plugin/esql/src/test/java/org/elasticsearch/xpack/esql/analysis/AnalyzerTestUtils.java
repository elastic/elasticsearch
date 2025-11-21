/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.enrich.ResolvedEnrichPolicy;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.inference.InferenceResolution;
import org.elasticsearch.xpack.esql.inference.ResolvedInference;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.core.enrich.EnrichPolicy.GEO_MATCH_TYPE;
import static org.elasticsearch.xpack.core.enrich.EnrichPolicy.MATCH_TYPE;
import static org.elasticsearch.xpack.core.enrich.EnrichPolicy.RANGE_TYPE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.testAnalyzerContext;

public final class AnalyzerTestUtils {

    private AnalyzerTestUtils() {}

    public static Analyzer defaultAnalyzer() {
        return analyzer(analyzerDefaultMapping());
    }

    public static Analyzer expandedDefaultAnalyzer() {
        return analyzer(expandedDefaultIndexResolution());
    }

    /** Simplest analyzer with a single index, which must be valid */
    public static Analyzer analyzer(IndexResolution indexResolution) {
        return analyzer(indexResolution, TEST_VERIFIER);
    }

    /** Simple analyzer with multiple indexes, which may also be invalid */
    public static Analyzer analyzer(Map<IndexPattern, IndexResolution> indexResolutions) {
        return analyzer(indexResolutions, defaultLookupResolution(), defaultEnrichResolution(), TEST_VERIFIER, TEST_CFG);
    }

    public static Analyzer analyzer(IndexResolution indexResolution, Map<String, IndexResolution> lookupResolution) {
        return analyzer(indexResolution, lookupResolution, TEST_VERIFIER);
    }

    public static Analyzer analyzer(IndexResolution indexResolution, Verifier verifier) {
        return analyzer(indexResolution, defaultLookupResolution(), verifier);
    }

    public static Analyzer analyzer(IndexResolution indexResolution, Map<String, IndexResolution> lookupResolution, Verifier verifier) {
        return analyzer(indexResolution, lookupResolution, defaultEnrichResolution(), verifier);
    }

    public static Analyzer analyzer(
        IndexResolution indexResolution,
        Map<String, IndexResolution> lookupResolution,
        EnrichResolution enrichResolution,
        Verifier verifier
    ) {
        return analyzer(indexResolutions(indexResolution), lookupResolution, enrichResolution, verifier, TEST_CFG);
    }

    public static Analyzer analyzer(
        Map<IndexPattern, IndexResolution> indexResolutions,
        Map<String, IndexResolution> lookupResolution,
        EnrichResolution enrichResolution,
        Verifier verifier,
        Configuration config
    ) {
        return new Analyzer(
            testAnalyzerContext(
                config,
                new EsqlFunctionRegistry(),
                mergeIndexResolutions(indexResolutions, defaultSubqueryResolution()),
                lookupResolution,
                enrichResolution,
                defaultInferenceResolution()
            ),
            verifier
        );
    }

    public static Map<IndexPattern, IndexResolution> mergeIndexResolutions(
        Map<IndexPattern, IndexResolution> indexResolutions,
        Map<IndexPattern, IndexResolution> more
    ) {
        Map<IndexPattern, IndexResolution> combined = new HashMap<>(indexResolutions);
        combined.putAll(more);
        return combined;
    }

    public static Analyzer analyzer(Map<IndexPattern, IndexResolution> indexResolutions, Verifier verifier, Configuration config) {
        return analyzer(indexResolutions, defaultLookupResolution(), defaultEnrichResolution(), verifier, config);
    }

    public static Analyzer analyzer(Verifier verifier) {
        return analyzer(analyzerDefaultMapping(), defaultLookupResolution(), defaultEnrichResolution(), verifier, EsqlTestUtils.TEST_CFG);
    }

    public static Analyzer analyzer(Map<IndexPattern, IndexResolution> indexResolutions, Verifier verifier) {
        return analyzer(indexResolutions, defaultLookupResolution(), defaultEnrichResolution(), verifier, EsqlTestUtils.TEST_CFG);
    }

    public static LogicalPlan analyze(String query) {
        return analyze(query, "mapping-basic.json");
    }

    public static LogicalPlan analyze(String query, String mapping) {
        return analyze(query, indexFromQuery(query), mapping);
    }

    public static LogicalPlan analyze(String query, String index, String mapping) {
        Map<IndexPattern, IndexResolution> indexResolutions = index == null
            ? Map.of()
            : Map.of(new IndexPattern(Source.EMPTY, index), loadMapping(mapping, index));
        return analyze(query, analyzer(indexResolutions, TEST_VERIFIER, configuration(query)));
    }

    public static LogicalPlan analyze(String query, Analyzer analyzer) {
        var plan = new EsqlParser().createStatement(query);
        // System.out.println(plan);
        var analyzed = analyzer.analyze(plan);
        // System.out.println(analyzed);
        return analyzed;
    }

    public static LogicalPlan analyze(String query, TransportVersion transportVersion) {
        Analyzer baseAnalyzer = expandedDefaultAnalyzer();
        if (baseAnalyzer.context() instanceof MutableAnalyzerContext mutableContext) {
            try (var restore = mutableContext.setTemporaryTransportVersionOnOrAfter(transportVersion)) {
                return analyze(query, baseAnalyzer);
            }
        } else {
            throw new UnsupportedOperationException("Analyzer Context is not mutable");
        }
    }

    private static final Pattern indexFromPattern = Pattern.compile("(?i)FROM\\s+([\\w-]+)");

    private static String indexFromQuery(String query) {
        // Extract the index name from the FROM clause of the query using regexp
        Matcher matcher = indexFromPattern.matcher(query);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    public static LogicalPlan analyze(String query, String mapping, QueryParams params) {
        return analyze(query, indexFromQuery(query), mapping, params);
    }

    public static LogicalPlan analyze(String query, String index, String mapping, QueryParams params) {
        var plan = new EsqlParser().createStatement(query, params);
        var indexResolutions = Map.of(new IndexPattern(Source.EMPTY, index), loadMapping(mapping, index));
        var analyzer = analyzer(indexResolutions, TEST_VERIFIER, configuration(query));
        return analyzer.analyze(plan);
    }

    public static UnresolvedRelation unresolvedRelation(String index) {
        return new UnresolvedRelation(
            Source.EMPTY,
            new IndexPattern(Source.EMPTY, index),
            false,
            List.of(),
            IndexMode.STANDARD,
            null,
            "FROM"
        );
    }

    public static IndexResolution loadMapping(String resource, String indexName, IndexMode indexMode) {
        return IndexResolution.valid(new EsIndex(indexName, EsqlTestUtils.loadMapping(resource), Map.of(indexName, indexMode), Set.of()));
    }

    public static IndexResolution loadMapping(String resource, String indexName) {
        return loadMapping(resource, indexName, IndexMode.STANDARD);
    }

    public static Map<IndexPattern, IndexResolution> analyzerDefaultMapping() {
        // Most tests use either "test" or "employees" as the index name, but for the same mapping
        return Map.of(
            new IndexPattern(Source.EMPTY, "test"),
            loadMapping("mapping-basic.json", "test"),
            new IndexPattern(Source.EMPTY, "employees"),
            loadMapping("mapping-basic.json", "employees")
        );
    }

    public static Map<IndexPattern, IndexResolution> indexResolutions(EsIndex... indexes) {
        Map<IndexPattern, IndexResolution> map = new HashMap<>();
        for (EsIndex index : indexes) {
            map.put(new IndexPattern(Source.EMPTY, index.name()), IndexResolution.valid(index));
        }
        return map;
    }

    public static Map<IndexPattern, IndexResolution> indexResolutions(IndexResolution... indexes) {
        Map<IndexPattern, IndexResolution> map = new HashMap<>();
        for (IndexResolution index : indexes) {
            map.put(new IndexPattern(Source.EMPTY, index.get().name()), index);
        }
        return map;
    }

    public static IndexResolution expandedDefaultIndexResolution() {
        return loadMapping("mapping-default.json", "test");
    }

    public static Map<String, IndexResolution> defaultLookupResolution() {
        return Map.of(
            "languages_lookup",
            loadMapping("mapping-languages.json", "languages_lookup", IndexMode.LOOKUP),
            "test_lookup",
            loadMapping("mapping-basic.json", "test_lookup", IndexMode.LOOKUP),
            "spatial_lookup",
            loadMapping("mapping-multivalue_geometries.json", "spatial_lookup", IndexMode.LOOKUP)
        );
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
        loadEnrichPolicyResolution(
            enrichResolution,
            Enrich.Mode.COORDINATOR,
            MATCH_TYPE,
            "languages_coord",
            "language_code",
            "languages_idx",
            "mapping-languages.json"
        );
        loadEnrichPolicyResolution(
            enrichResolution,
            Enrich.Mode.REMOTE,
            MATCH_TYPE,
            "languages_remote",
            "language_code",
            "languages_idx",
            "mapping-languages.json"
        );
        return enrichResolution;
    }

    public static final String RERANKING_INFERENCE_ID = "reranking-inference-id";
    public static final String COMPLETION_INFERENCE_ID = "completion-inference-id";
    public static final String TEXT_EMBEDDING_INFERENCE_ID = "text-embedding-inference-id";
    public static final String CHAT_COMPLETION_INFERENCE_ID = "chat-completion-inference-id";
    public static final String SPARSE_EMBEDDING_INFERENCE_ID = "sparse-embedding-inference-id";
    public static final List<String> VALID_INFERENCE_IDS = List.of(
        RERANKING_INFERENCE_ID,
        COMPLETION_INFERENCE_ID,
        TEXT_EMBEDDING_INFERENCE_ID,
        CHAT_COMPLETION_INFERENCE_ID,
        SPARSE_EMBEDDING_INFERENCE_ID
    );
    public static final String ERROR_INFERENCE_ID = "error-inference-id";

    public static InferenceResolution defaultInferenceResolution() {
        return InferenceResolution.builder()
            .withResolvedInference(new ResolvedInference(RERANKING_INFERENCE_ID, TaskType.RERANK))
            .withResolvedInference(new ResolvedInference(COMPLETION_INFERENCE_ID, TaskType.COMPLETION))
            .withResolvedInference(new ResolvedInference(TEXT_EMBEDDING_INFERENCE_ID, TaskType.TEXT_EMBEDDING))
            .withResolvedInference(new ResolvedInference(CHAT_COMPLETION_INFERENCE_ID, TaskType.CHAT_COMPLETION))
            .withResolvedInference(new ResolvedInference(SPARSE_EMBEDDING_INFERENCE_ID, TaskType.SPARSE_EMBEDDING))
            .withError(ERROR_INFERENCE_ID, "error with inference resolution")
            .build();
    }

    public static Map<IndexPattern, IndexResolution> defaultSubqueryResolution() {
        return Map.of(
            new IndexPattern(Source.EMPTY, "languages"),
            loadMapping("mapping-languages.json", "languages"),
            new IndexPattern(Source.EMPTY, "sample_data"),
            loadMapping("mapping-sample_data.json", "sample_data"),
            new IndexPattern(Source.EMPTY, "test_mixed_types"),
            loadMapping("mapping-default-incompatible.json", "test_mixed_types"),
            new IndexPattern(Source.EMPTY, "k8s"),
            loadMapping("k8s-downsampled-mappings.json", "k8s", IndexMode.TIME_SERIES)
        );
    }

    public static String randomInferenceId() {
        return ESTestCase.randomFrom(VALID_INFERENCE_IDS);
    }

    public static String randomInferenceIdOtherThan(String... excludes) {
        return ESTestCase.randomValueOtherThanMany(Arrays.asList(excludes)::contains, AnalyzerTestUtils::randomInferenceId);
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

    public static void loadEnrichPolicyResolution(
        EnrichResolution enrich,
        Enrich.Mode mode,
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
            mode,
            new ResolvedEnrichPolicy(field, policyType, enrichFields, Map.of("", index), indexResolution.get().mapping())
        );
    }

    public static void loadEnrichPolicyResolution(EnrichResolution enrich, String policy, String field, String index, String mapping) {
        loadEnrichPolicyResolution(enrich, EnrichPolicy.MATCH_TYPE, policy, field, index, mapping);
    }

    public static IndexResolution tsdbIndexResolution() {
        return loadMapping("tsdb-mapping.json", "test");
    }

    public static IndexResolution k8sIndexResolution() {
        return loadMapping("k8s-mappings.json", "k8s");
    }

    public static <E> E randomValueOtherThanTest(Predicate<E> exclude, Supplier<E> supplier) {
        while (true) {
            E value = supplier.get();
            if (exclude.test(value) == false) {
                return value;
            }
        }
    }

    public static IndexResolution indexWithDateDateNanosUnionType() {
        // this method is shared by AnalyzerTest, QueryTranslatorTests and LocalPhysicalPlanOptimizerTests
        String dateDateNanos = "date_and_date_nanos"; // mixed date and date_nanos
        String dateDateNanosLong = "date_and_date_nanos_and_long"; // mixed date, date_nanos and long
        LinkedHashMap<String, Set<String>> typesToIndices1 = new LinkedHashMap<>();
        typesToIndices1.put("date", Set.of("index1", "index2"));
        typesToIndices1.put("date_nanos", Set.of("index3"));
        LinkedHashMap<String, Set<String>> typesToIndices2 = new LinkedHashMap<>();
        typesToIndices2.put("date", Set.of("index1"));
        typesToIndices2.put("date_nanos", Set.of("index2"));
        typesToIndices2.put("long", Set.of("index3"));
        EsField dateDateNanosField = new InvalidMappedField(dateDateNanos, typesToIndices1);
        EsField dateDateNanosLongField = new InvalidMappedField(dateDateNanosLong, typesToIndices2);
        EsIndex index = new EsIndex(
            "index*",
            Map.of(dateDateNanos, dateDateNanosField, dateDateNanosLong, dateDateNanosLongField),
            Map.of("index1", IndexMode.STANDARD, "index2", IndexMode.STANDARD, "index3", IndexMode.STANDARD),
            Set.of()
        );
        return IndexResolution.valid(index);
    }
}
