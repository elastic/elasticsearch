/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.MutableAnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor.TimestampBounds;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.enrich.ResolvedEnrichPolicy;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.inference.InferenceResolution;
import org.elasticsearch.xpack.esql.inference.ResolvedInference;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static junit.framework.Assert.assertTrue;
import static org.elasticsearch.test.ESTestCase.expectThrows;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.toQueryParams;
import static org.elasticsearch.xpack.esql.plan.QuerySettings.UNMAPPED_FIELDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Helper for testing all things related to {@link Analyzer#analyze}.
 * <ul>
 *     <li>{@link #query} for successful analysis
 *     <li>{@link #error} for analysis errors
 *     <li>{@link #buildAnalyzer} for building {@link Analyzer} for advanced usage
 *     <li>{@link #buildContext} for building a {@link MutableAnalyzerContext} for
 *         even more advanced usage
 * </ul>
 */
public class TestAnalyzer {
    private Configuration configuration = EsqlTestUtils.TEST_CFG;
    private EsqlFunctionRegistry functionRegistry = EsqlTestUtils.TEST_FUNCTION_REGISTRY;
    private final Map<IndexPattern, IndexResolution> indexResolutions = new HashMap<>();
    private final Map<String, IndexResolution> lookupResolution = new HashMap<>();
    private EnrichResolution enrichResolution = new EnrichResolution();
    private InferenceResolution inferenceResolution = InferenceResolution.EMPTY;
    private UnmappedResolution unmappedResolution = UNMAPPED_FIELDS.defaultValue();
    private TimestampBounds timestampBounds;
    private Supplier<TransportVersion> minimumTransportVersion = TransportVersionUtils::randomCompatibleVersion;
    private boolean stripErrorPrefix;

    TestAnalyzer() {}

    public TestAnalyzer configuration(Configuration configuration) {
        this.configuration = configuration;
        return this;
    }

    public TestAnalyzer functionRegistry(EsqlFunctionRegistry functionRegistry) {
        this.functionRegistry = functionRegistry;
        return this;
    }

    public TestAnalyzer addIndex(String pattern, IndexResolution resolution) {
        this.indexResolutions.put(new IndexPattern(Source.EMPTY, pattern), resolution);
        return this;
    }

    public TestAnalyzer addIndex(EsIndex index) {
        return addIndex(IndexResolution.valid(index));
    }

    public TestAnalyzer addIndex(IndexResolution resolution) {
        return addIndex(resolution.get().name(), resolution);
    }

    /**
     * Adds the standard set of subquery index resolutions used by many analyzer tests.
     */
    public TestAnalyzer addAnalysisTestsIndexResolutions() {
        String noFieldsIndexName = "no_fields_index";
        EsIndex noFieldsIndex = new EsIndex(
            noFieldsIndexName,
            Map.of(),
            Map.of(noFieldsIndexName, IndexMode.STANDARD),
            Map.of("", List.of(noFieldsIndexName)),
            Map.of("", List.of(noFieldsIndexName)),
            Set.of()
        );
        addIndex("languages", "mapping-languages.json");
        addIndex("sample_data", "mapping-sample_data.json");
        addIndex("test_mixed_types", "mapping-default-incompatible.json");
        addIndex("colors", "mapping-colors.json");
        addIndex("k8s", "k8s-downsampled-mappings.json", IndexMode.TIME_SERIES);
        addIndex("remote:missingIndex", IndexResolution.EMPTY_SUBQUERY);
        addIndex("empty_index", IndexResolution.empty("empty_index"));
        addIndex(noFieldsIndexName, IndexResolution.valid(noFieldsIndex));
        return this;
    }

    /**
     * Adds an index resolution by loading the mapping from a resource file.
     */
    public TestAnalyzer addIndex(String name, String mappingFile) {
        return addIndex(name, mappingFile, IndexMode.STANDARD);
    }

    public TestAnalyzer addIndex(String name, String mappingFile, IndexMode indexMode) {
        return addIndex(name, loadMapping(mappingFile, name, indexMode));
    }

    /**
     * Adds our traditional "employees" index.
     */
    public TestAnalyzer addEmployees() {
        return addEmployees("employees");
    }

    /**
     * Add our traditional "employees" index with a custom name.
     */
    public TestAnalyzer addEmployees(String name) {
        return addIndex(name, "mapping-basic.json");
    }

    /**
     * Adds a lookup index.
     */
    public TestAnalyzer addLookupIndex(IndexResolution resolution) {
        this.lookupResolution.put(resolution.get().name(), resolution);
        return this;
    }

    /**
     * Adds a lookup index, reading its mapping from a file.
     */
    public TestAnalyzer addLookupIndex(String name, String mappingLocation) {
        return addLookupIndex(loadMapping(mappingLocation, name, IndexMode.LOOKUP));
    }

    /**
     * Adds the standard set of lookup resolutions used by many analyzer tests.
     */
    public TestAnalyzer addAnalysisTestsLookupResolutions() {
        addLookupIndex("languages_lookup", "mapping-languages.json");
        addLookupIndex("test_lookup", "mapping-basic.json");
        return addLookupIndex("spatial_lookup", "mapping-multivalue_geometries.json");
    }

    public TestAnalyzer addEnrichError(String policyName, Enrich.Mode mode, String reason) {
        enrichResolution.addError(policyName, mode, reason);
        return this;
    }

    /**
     * Adds the standard set of enrich policy resolutions used by many analyzer tests.
     */
    public TestAnalyzer addAnalysisTestsEnrichResolution() {
        addEnrichPolicy(EnrichPolicy.MATCH_TYPE, "languages", "language_code", "languages_idx", "mapping-languages.json");
        addEnrichPolicy(EnrichPolicy.RANGE_TYPE, "client_cidr", "client_cidr", "client_cidr", "mapping-client_cidr.json");
        addEnrichPolicy(EnrichPolicy.RANGE_TYPE, "ages_policy", "age_range", "ages", "mapping-ages.json");
        addEnrichPolicy(EnrichPolicy.RANGE_TYPE, "heights_policy", "height_range", "heights", "mapping-heights.json");
        addEnrichPolicy(EnrichPolicy.RANGE_TYPE, "decades_policy", "date_range", "decades", "mapping-decades.json");
        addEnrichPolicy(
            EnrichPolicy.GEO_MATCH_TYPE,
            "city_boundaries",
            "city_boundary",
            "airport_city_boundaries",
            "mapping-airport_city_boundaries.json"
        );
        addEnrichPolicy(
            Enrich.Mode.COORDINATOR,
            EnrichPolicy.MATCH_TYPE,
            "languages_coord",
            "language_code",
            "languages_idx",
            "mapping-languages.json"
        );
        addEnrichPolicy(
            Enrich.Mode.REMOTE,
            EnrichPolicy.MATCH_TYPE,
            "languages_remote",
            "language_code",
            "languages_idx",
            "mapping-languages.json"
        );
        return this;
    }

    /**
     * Adds an enrich policy resolution by loading the mapping from a resource file.
     */
    public TestAnalyzer addEnrichPolicy(String policyType, String policy, String field, String index, String mapping) {
        return addEnrichPolicy(Enrich.Mode.ANY, policyType, policy, field, index, mapping);
    }

    /**
     * Adds an enrich policy resolution with a specific mode by loading the mapping from a resource file.
     */
    public TestAnalyzer addEnrichPolicy(Enrich.Mode mode, String policyType, String policy, String field, String index, String mapping) {
        IndexResolution indexResolution = loadMapping(mapping, index, IndexMode.STANDARD);
        List<String> enrichFields = new ArrayList<>(indexResolution.get().mapping().keySet());
        enrichFields.remove(field);
        return addEnrichPolicy(
            mode,
            policy,
            new ResolvedEnrichPolicy(field, policyType, enrichFields, Map.of("", index), indexResolution.get().mapping())
        );
    }

    /**
     * Adds an enrich policy resolution with a specific mode by loading the mapping from a resource file.
     */
    public TestAnalyzer addEnrichPolicy(Enrich.Mode mode, String policy, ResolvedEnrichPolicy resolved) {
        enrichResolution.addResolvedPolicy(policy, mode, resolved);
        return this;
    }

    public TestAnalyzer inferenceResolution(InferenceResolution inferenceResolution) {
        this.inferenceResolution = inferenceResolution;
        return this;
    }

    private static final String RERANKING_INFERENCE_ID = "reranking-inference-id";
    private static final String COMPLETION_INFERENCE_ID = "completion-inference-id";
    private static final String TEXT_EMBEDDING_INFERENCE_ID = "text-embedding-inference-id";
    private static final String CHAT_COMPLETION_INFERENCE_ID = "chat-completion-inference-id";
    private static final String SPARSE_EMBEDDING_INFERENCE_ID = "sparse-embedding-inference-id";
    private static final String ERROR_INFERENCE_ID = "error-inference-id";

    private static InferenceResolution defaultInferenceResolution() {
        return InferenceResolution.builder()
            .withResolvedInference(new ResolvedInference(RERANKING_INFERENCE_ID, TaskType.RERANK))
            .withResolvedInference(new ResolvedInference(COMPLETION_INFERENCE_ID, TaskType.COMPLETION))
            .withResolvedInference(new ResolvedInference(TEXT_EMBEDDING_INFERENCE_ID, TaskType.TEXT_EMBEDDING))
            .withResolvedInference(new ResolvedInference(CHAT_COMPLETION_INFERENCE_ID, TaskType.CHAT_COMPLETION))
            .withResolvedInference(new ResolvedInference(SPARSE_EMBEDDING_INFERENCE_ID, TaskType.SPARSE_EMBEDDING))
            .withError(ERROR_INFERENCE_ID, "error with inference resolution")
            .build();
    }

    /**
     * Adds the standard set of inference resolutions used by many analyzer tests.
     */
    public TestAnalyzer addAnalysisTestsInferenceResolution() {
        return inferenceResolution(defaultInferenceResolution());
    }

    public TestAnalyzer unmappedResolution(UnmappedResolution unmappedResolution) {
        this.unmappedResolution = unmappedResolution;
        return this;
    }

    /**
     * Adds {@code @timestamp} lower and upper bounds extracted from a query DSL filter.
     */
    public TestAnalyzer timestampBounds(TimestampBounds timestampBounds) {
        this.timestampBounds = timestampBounds;
        return this;
    }

    /**
     * Set the minimum transport version used by the analyzer. Some behaviors are
     * disabled on older transport versions because the remote nodes won't
     * understand them.
     */
    public TestAnalyzer minimumTransportVersion(TransportVersion minimumTransportVersion) {
        this.minimumTransportVersion = () -> minimumTransportVersion;
        return this;
    }

    /**
     * Build the analyzer, parse the query, and analyze it.
     */
    public LogicalPlan query(String query) {
        return buildAnalyzer().analyze(EsqlTestUtils.TEST_PARSER.parseQuery(query));
    }

    /**
     * Build the analyzer, parse the query, and analyze it.
     */
    public LogicalPlan query(String query, Object... params) {
        return buildAnalyzer().analyze(EsqlTestUtils.TEST_PARSER.parseQuery(query, toQueryParams(params)));
    }

    /**
     * If {@code true}, {@link #error} strips the {@code "Found N problem(s)\nline "} prefix
     * from the exception message, returning only the per-line diagnostic. Defaults to {@code false}.
     */
    public TestAnalyzer stripErrorPrefix(boolean stripErrorPrefix) {
        this.stripErrorPrefix = stripErrorPrefix;
        return this;
    }

    /**
     * Build the analyzer, parse the query, analyze it, and assert that it throws.
     * If {@link #stripErrorPrefix} is set, strips the "Found N problem(s)" prefix.
     */
    public String error(String query, Object... params) {
        return error(query, VerificationException.class, params);
    }

    /**
     * Build the analyzer, parse the query, analyze it, and assert that it throws the given exception.
     * If {@link #stripErrorPrefix} is set, strips the "Found N problem(s)" prefix.
     */
    public String error(String query, Class<? extends Exception> exception, Object... params) {
        Throwable e = expectThrows(
            exception,
            "Expected error for query [" + query + "] but no error was raised",
            () -> query(query, params)
        );
        assertThat(e, instanceOf(exception));

        String message = e.getMessage();
        if (stripErrorPrefix == false) {
            return message;
        }
        if (e instanceof VerificationException) {
            assertTrue(message.startsWith("Found "));
        }
        String pattern = "\nline ";
        int index = message.indexOf(pattern);
        return message.substring(index + pattern.length());
    }

    /**
     * Build an {@link Analyzer} for advanced usage.
     * Prefer {@link #query} or {@link #error} if possible.
     */
    public Analyzer buildAnalyzer() {
        return buildAnalyzer(EsqlTestUtils.TEST_VERIFIER);
    }

    /**
     * Build an {@link Analyzer} for advanced usage.
     * Prefer {@link #query} or {@link #error} if possible.
     */
    public Analyzer buildAnalyzer(Verifier verifier) {
        return new Analyzer(buildContext(), verifier);
    }

    /**
     * Build an {@link AnalyzerContext} for advanced usage.
     * Prefer {@link #query} or {@link #error} if possible.
     */
    public AnalyzerContext buildContext() {
        return new AnalyzerContext(
            configuration,
            functionRegistry,
            null,
            indexResolutions,
            lookupResolution,
            enrichResolution,
            inferenceResolution,
            ExternalSourceResolution.EMPTY,
            minimumTransportVersion.get(),
            unmappedResolution,
            timestampBounds
        );
    }

    private static IndexResolution loadMapping(String resource, String indexName, IndexMode indexMode) {
        return IndexResolution.valid(
            new EsIndex(indexName, EsqlTestUtils.loadMapping(resource), Map.of(indexName, indexMode), Map.of(), Map.of(), Set.of())
        );
    }
}
