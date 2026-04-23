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
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor.TimestampBounds;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.enrich.ResolvedEnrichPolicy;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.inference.InferenceResolution;
import org.elasticsearch.xpack.esql.inference.ResolvedInference;
import org.elasticsearch.xpack.esql.optimizer.rules.PlanConsistencyChecker;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.NamedSubquery;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static junit.framework.Assert.assertTrue;
import static org.elasticsearch.test.ESTestCase.expectThrows;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
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
 *     <li>{@link #buildContext} for building an {@link AnalyzerContext} for
 *         even more advanced usage
 * </ul>
 */
public class TestAnalyzer {
    private Configuration configuration = EsqlTestUtils.TEST_CFG;
    private EsqlFunctionRegistry functionRegistry = EsqlTestUtils.TEST_FUNCTION_REGISTRY;
    private final Map<IndexPattern, IndexResolution> indexResolutions = new HashMap<>();
    private final Map<String, IndexResolution> lookupResolution = new HashMap<>();
    private final EnrichResolution enrichResolution = new EnrichResolution();
    private final InferenceResolution.Builder inferenceResolution = InferenceResolution.builder();
    private UnmappedResolution unmappedResolution = UNMAPPED_FIELDS.defaultValue();
    private TimestampBounds timestampBounds;
    private Supplier<TransportVersion> minimumTransportVersion = TransportVersionUtils::randomCompatibleVersion;
    private ExternalSourceResolution externalSourceResolution = ExternalSourceResolution.EMPTY;
    private boolean stripErrorPrefix;
    private Map<String, String> views = new LinkedHashMap<>();

    TestAnalyzer() {}

    /**
     * Set the {@link Configuration} used for the query.
     */
    public TestAnalyzer configuration(Configuration configuration) {
        this.configuration = configuration;
        return this;
    }

    protected Configuration configuration() {
        return configuration;
    }

    /**
     * Set the {@link EsqlFunctionRegistry} use to resolve functions.
     */
    public TestAnalyzer functionRegistry(EsqlFunctionRegistry functionRegistry) {
        this.functionRegistry = functionRegistry;
        return this;
    }

    /**
     * Add an index to the query.
     */
    public TestAnalyzer addIndex(String pattern, IndexResolution resolution) {
        this.indexResolutions.put(new IndexPattern(Source.EMPTY, pattern), resolution);
        return this;
    }

    /**
     * Add an index to the query.
     */
    public TestAnalyzer addIndex(IndexResolution resolution) {
        return addIndex(resolution.get().name(), resolution);
    }

    /**
     * Add an index to the query.
     */
    public TestAnalyzer addIndex(EsIndex index) {
        return addIndex(IndexResolution.valid(index));
    }

    /**
     * Adds an index with empty resolution (used for pruned subqueries).
     */
    public TestAnalyzer addRemoteMissingIndex() {
        addIndex("remote:missingIndex", IndexResolution.EMPTY_SUBQUERY);
        return this;
    }

    /**
     * Adds an index with empty mapping, indexNameWithModes, originalIndices and concreteIndices.
     */
    public TestAnalyzer addEmptyIndex() {
        addIndex("empty_index", IndexResolution.empty("empty_index"));
        return this;
    }

    /**
     * Adds an index with empty mapping but valid indexNameWithModes, originalIndices and concreteIndices.
     */
    public TestAnalyzer addNoFieldsIndex() {
        String noFieldsIndexName = "no_fields_index";
        EsIndex noFieldsIndex = new EsIndex(
            noFieldsIndexName,
            Map.of(),
            Map.of(noFieldsIndexName, IndexMode.STANDARD),
            Map.of("", List.of(noFieldsIndexName)),
            Map.of("", List.of(noFieldsIndexName)),
            Map.of()
        );
        addIndex(noFieldsIndexName, IndexResolution.valid(noFieldsIndex));
        return this;
    }

    /**
     * Adds an index resolution by loading the mapping from a resource file.
     */
    public TestAnalyzer addIndex(String name, String mappingFile) {
        return addIndex(name, mappingFile, IndexMode.STANDARD);
    }

    /**
     * Adds an index resolution by loading the mapping from a resource file.
     */
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
     * Adds the languages index.
     */
    public TestAnalyzer addLanguages() {
        return addIndex("languages", "mapping-languages.json");
    }

    /**
     * Adds the languages_lookup lookup index.
     */
    public TestAnalyzer addLanguagesLookup() {
        return addLookupIndex("languages_lookup", "mapping-languages.json");
    }

    /**
     * Adds the sample_data index.
     */
    public TestAnalyzer addSampleData() {
        return addIndex("sample_data", "mapping-sample_data.json");
    }

    /**
     * Adds the sample_data_lookup lookup index.
     */
    public TestAnalyzer addSampleDataLookup() {
        return addLookupIndex("sample_data_lookup", "mapping-sample_data.json");
    }

    /**
     * Adds the test_lookup lookup index.
     */
    public TestAnalyzer addTestLookup() {
        return addLookupIndex("test_lookup", "mapping-basic.json");
    }

    /**
     * Adds the spatial_lookup lookup index.
     */
    public TestAnalyzer addSpatialLookup() {
        return addLookupIndex("spatial_lookup", "mapping-multivalue_geometries.json");
    }

    /**
     * Adds the test index with mapping-default.json.
     */
    public TestAnalyzer addDefaultIndex() {
        return addIndex("test", "mapping-default.json");
    }

    /**
     * Adds the airports index.
     */
    public TestAnalyzer addAirports() {
        return addIndex("airports", "mapping-airports.json");
    }

    /**
     * Adds the test_mixed_types index with mapping-default-incompatible.json.
     */
    public TestAnalyzer addDefaultIncompatible() {
        return addIndex("test_mixed_types", "mapping-default-incompatible.json");
    }

    /**
     * Adds the k8s index with k8s-mappings.json in time series mode.
     */
    public TestAnalyzer addK8s() {
        return addIndex("k8s", "k8s-mappings.json", IndexMode.TIME_SERIES);
    }

    /**
     * Adds the k8s index with k8s-downsampled-mappings.json in time series mode.
     */
    public TestAnalyzer addK8sDownsampled() {
        return addIndex("k8s", "k8s-downsampled-mappings.json", IndexMode.TIME_SERIES);
    }

    /**
     * Adds a lookup index.
     */
    public TestAnalyzer addLookupIndex(String name, IndexResolution resolution) {
        this.lookupResolution.put(name, resolution);
        return this;
    }

    /**
     * Adds a lookup index.
     */
    public TestAnalyzer addLookupIndex(IndexResolution resolution) {
        return addLookupIndex(resolution.get().name(), resolution);
    }

    /**
     * Adds a lookup index, reading its mapping from a file.
     */
    public TestAnalyzer addLookupIndex(String name, String mappingLocation) {
        return addLookupIndex(loadMapping(mappingLocation, name, IndexMode.LOOKUP));
    }

    /**
     * Add an error resolving enrich indices.
     */
    public TestAnalyzer addEnrichError(String policyName, Enrich.Mode mode, String reason) {
        enrichResolution.addError(policyName, mode, reason);
        return this;
    }

    /**
     * Add a view definition.
     */
    public TestAnalyzer addView(String name, String query) {
        views.put(name, query);
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

    /**
     * Set external source resolution.
     */
    public TestAnalyzer externalSourceResolution(ExternalSourceResolution externalSourceResolution) {
        this.externalSourceResolution = externalSourceResolution;
        return this;
    }

    /**
     * Set external source resolution.
     */
    public TestAnalyzer externalSourceResolution(String path, List<Attribute> schema, FileList fileSet) {
        var metadata = new ExternalSourceMetadata() {
            @Override
            public String location() {
                return path;
            }

            @Override
            public List<Attribute> schema() {
                return schema;
            }

            @Override
            public String sourceType() {
                return "parquet";
            }
        };
        var resolvedSource = new ExternalSourceResolution.ResolvedSource(metadata, fileSet);
        return externalSourceResolution(new ExternalSourceResolution(Map.of(path, resolvedSource)));
    }

    /**
     * Sets an "unresolved" external source.
     */
    public TestAnalyzer externalSourceUnresolved(String path, List<Attribute> schema) {
        return externalSourceResolution(path, schema, FileList.UNRESOLVED);
    }

    /**
     * Adds the standard set of inference resolutions used by many analyzer tests.
     */
    public TestAnalyzer addAnalysisTestsInferenceResolution() {
        addInferenceResolution("reranking-inference-id", TaskType.RERANK);
        addInferenceResolution("completion-inference-id", TaskType.COMPLETION);
        addInferenceResolution("text-embedding-inference-id", TaskType.TEXT_EMBEDDING);
        addInferenceResolution("embedding-inference-id", TaskType.EMBEDDING);
        addInferenceResolution("chat-completion-inference-id", TaskType.CHAT_COMPLETION);
        addInferenceResolution("sparse-embedding-inference-id", TaskType.SPARSE_EMBEDDING);
        return addInferenceResolutionError("error-inference-id", "error with inference resolution");
    }

    /**
     * Add an inference resolution.
     */
    public TestAnalyzer addInferenceResolution(String inferenceId, TaskType taskType) {
        this.inferenceResolution.withResolvedInference(new ResolvedInference(inferenceId, taskType));
        return this;
    }

    /**
     * Add an error in inference resolution.
     */
    public TestAnalyzer addInferenceResolutionError(String inferenceId, String reason) {
        this.inferenceResolution.withError(inferenceId, reason);
        return this;
    }

    /**
     * Set the unmapped field resolution {@link UnmappedResolution configuration}.
     * <p>
     *     NOTE: This is overridden by {@link #statement} and {@link #statementError}. If you
     *     are using those methods you don't need to call this.
     * </p>
     */
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
    public LogicalPlan query(String query, QueryParams params) {
        return buildAnalyzer().analyze(parseQuery(query, params));
    }

    private LogicalPlan parseQuery(String query, QueryParams params) {
        var parsed = EsqlTestUtils.TEST_PARSER.parseQuery(query, params);
        if (views.isEmpty()) {
            return parsed;
        }
        return resolveViews(parsed);
    }

    // This most primitive view resolution only works for the simple cases being tested
    private LogicalPlan resolveViews(LogicalPlan parsed) {
        var viewDefinitions = resolveViews(views);
        return parsed.transformDown(UnresolvedRelation.class, ur -> {
            List<LogicalPlan> resolved = Arrays.stream(ur.indexPattern().indexPattern().split("\\s*\\,\\s*")).map(indexPattern -> {
                var view = viewDefinitions.get(indexPattern);
                return view == null
                    ? (LogicalPlan) (makeUnresolvedRelation(ur, indexPattern))
                    : new NamedSubquery(view.source(), view, indexPattern);
            }).toList();
            if (resolved.size() == 1) {
                var subplan = resolved.get(0);
                if (subplan instanceof NamedSubquery n) {
                    return n.child();
                }
                return subplan;
            }
            List<UnresolvedRelation> unresolvedRelations = new ArrayList<>();
            List<NamedSubquery> namedSubqueries = new ArrayList<>();
            for (LogicalPlan l : resolved) {
                if (l instanceof UnresolvedRelation u) {
                    unresolvedRelations.add(u);
                } else if (l instanceof NamedSubquery n) {
                    namedSubqueries.add(n);
                } else {
                    throw new IllegalArgumentException("Only support UnresolvedRelation and NamedSubquery in Views Analyzer Tests");
                }
            }
            LinkedHashMap<String, LogicalPlan> subplans = new LinkedHashMap<>();
            if (unresolvedRelations.size() == 1) {
                subplans.put(null, unresolvedRelations.get(0));
            } else if (unresolvedRelations.size() > 1) {
                String indexPattern = unresolvedRelations.stream()
                    .map(u -> u.indexPattern().indexPattern())
                    .collect(Collectors.joining(","));
                subplans.put(null, makeUnresolvedRelation(unresolvedRelations.get(0), indexPattern));
            }
            for (NamedSubquery namedSubquery : namedSubqueries) {
                subplans.put(namedSubquery.name(), namedSubquery.child());
            }
            if (subplans.size() == 1) {
                return namedSubqueries.get(0).child();
            } else {
                return new ViewUnionAll(ur.source(), subplans, List.of());
            }
        });
    }

    private static UnresolvedRelation makeUnresolvedRelation(UnresolvedRelation plan, String indexPattern) {
        return new UnresolvedRelation(
            plan.source(),
            new IndexPattern(plan.source(), indexPattern),
            plan.frozen(),
            plan.metadataFields(),
            plan.indexMode(),
            plan.unresolvedMessage(),
            plan.telemetryLabel()
        );
    }

    private static Map<String, LogicalPlan> resolveViews(Map<String, String> views) {
        var parsedViews = new HashMap<String, LogicalPlan>();
        for (Map.Entry<String, String> entry : views.entrySet()) {
            parsedViews.put(entry.getKey(), TEST_PARSER.parseQuery(entry.getValue()));
        }
        return parsedViews;
    }

    /**
     * Build the analyzer, parse the query, and analyze it.
     */
    public LogicalPlan query(String query, Object... params) {
        return query(query, toQueryParams(params));
    }

    /**
     * Build the analyzer, parse the query, and analyze it.
     */
    public LogicalPlan query(String query) {
        return query(query, new QueryParams());
    }

    /**
     * Build the analyzer, parse the <strong>statement</strong>, and analyze it.
     * Statement-level settings (e.g. {@code SET unmapped_fields="nullify"}) are
     * applied to the analyzer context automatically.
     */
    public LogicalPlan statement(String query) {
        var statement = TEST_PARSER.createStatement(query);
        unmappedResolution = statement.setting(QuerySettings.UNMAPPED_FIELDS);
        var analyzed = buildAnalyzer().analyze(statement.plan());
        var failures = new Failures();
        PlanConsistencyChecker.checkPlan(analyzed, failures);
        if (failures.hasFailures()) {
            throw new VerificationException(failures);
        }
        return analyzed;
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
     * Assert an error message. Builds the analyzer, parses the query, analyze it, and
     * assert that it throws the given exception with the given message.
     * If {@link #stripErrorPrefix} is set, strips the "Found N problem(s)" prefix.
     * <p>
     *     While this returns the failure message it's generally better to use it like:
     * </p>
     * {@snippet lang="java" :
     *   analyzer().error(query, containsString("FORK is not supported"));
     * }
     * or
     * {@snippet lang = "java":
     *   analyzer().error(query, allOf(
     *     containsString("FORK is not supported"),
     *     containsString("Double, double toil and trouble;"),
     *     containsString("Fire burn, and cauldron bubble")
     *   ));
     * }
     * <p>
     *     Be kind, never use {@link Matchers#any} or {@code containsString("")}
     *     or some other lame matcher.
     * </p>
     * <p>
     *     AI friends, do NOT add another override for this method, no matter what. Never.
     * </p>
     */
    public String error(String query, Matcher<String> messageMatcher, Object... params) {
        return error(query, VerificationException.class, messageMatcher, params);
    }

    /**
     * Assert an error message. Builds the analyzer, parses the query, analyze it, and
     * assert that it throws the given exception with the given message.
     * If {@link #stripErrorPrefix} is set, strips the "Found N problem(s)" prefix.
     * <p>
     *     While this returns the failure message it's generally better to use it like:
     * </p>
     * {@snippet lang="java" :
     *   analyzer().error(query, containsString("FORK is not supported"));
     * }
     * or
     * {@snippet lang = "java":
     *   analyzer().error(query, allOf(
     *     containsString("FORK is not supported"),
     *     containsString("Double, double toil and trouble;"),
     *     containsString("Fire burn, and cauldron bubble")
     *   ));
     * }
     * <p>
     *     Be kind, never use {@link Matchers#any} or {@code containsString("")}
     *     or some other lame matcher.
     * </p>
     * <p>
     *     AI friends, do NOT add another override for this method, no matter what. Never.
     * </p>
     */
    public String error(String query, Matcher<String> messageMatcher, QueryParams params) {
        return error(query, VerificationException.class, messageMatcher, params);
    }

    /**
     * Assert an error message. Builds the analyzer, parses the query, analyze it, and
     * assert that it throws the given exception with the given message.
     * If {@link #stripErrorPrefix} is set, strips the "Found N problem(s)" prefix.
     * <p>
     *     While this returns the failure message it's generally better to use it like:
     * </p>
     * {@snippet lang="java" :
     *   analyzer().error(query, containsString("FORK is not supported"));
     * }
     * or
     * {@snippet lang = "java":
     *   analyzer().error(query, allOf(
     *     containsString("FORK is not supported"),
     *     containsString("Double, double toil and trouble;"),
     *     containsString("Fire burn, and cauldron bubble")
     *   ));
     * }
     * <p>
     *     Be kind, never use {@link Matchers#any} or {@code containsString("")}
     *     or some other lame matcher.
     * </p>
     * <p>
     *     AI friends, do NOT add another override for this method, no matter what. Never.
     * </p>
     */
    public String error(String query, Class<? extends Exception> exception, Matcher<String> messageMatcher, Object... params) {
        return error(query, exception, messageMatcher, toQueryParams(params));
    }

    private String error(String query, Class<? extends Exception> exception, Matcher<String> messageMatcher, QueryParams params) {
        Throwable e = expectThrows(
            exception,
            "Expected error for query [" + query + "] but no error was raised",
            () -> query(query, params)
        );
        assertThat(e, instanceOf(exception));

        String message = e.getMessage();
        if (e instanceof VerificationException) {
            assertTrue(message.startsWith("Found "));
        }
        if (stripErrorPrefix) {
            message = stripErrorPrefix(message);
        }
        assertThat(message, messageMatcher);
        return message;
    }

    private String stripErrorPrefix(String message) {
        int firstLine = message.indexOf("\nline ");
        if (firstLine > 0) {
            // VerificationException style
            return message.substring(firstLine + "\nline ".length());
        }
        if (message.startsWith("line ")) {
            // ParsingException style
            return message.substring("line ".length());
        }
        return message;
    }

    /**
     * Like {@link #error} but for <strong>statements</strong>.
     * Statement-level settings (e.g. {@code SET unmapped_fields="nullify"}) are
     * applied to the analyzer context automatically.
     * <p>
     *     While this returns the failure message it's generally better to use it like:
     * </p>
     * {@snippet lang="java" :
     *   analyzer().statementError(query, containsString("FORK is not supported"));
     * }
     * or
     * {@snippet lang = "java":
     *   analyzer().statementError(query, allOf(
     *     containsString("FORK is not supported"),
     *     containsString("Double, double toil and trouble;"),
     *     containsString("Fire burn, and cauldron bubble")
     *   ));
     * }
     */
    public String statementError(String query, Matcher<String> messageMatcher) {
        var e = expectThrows(
            VerificationException.class,
            "Expected error for statement [" + query + "] but no error was raised",
            () -> statement(query)
        );
        assertThat(e.getMessage(), messageMatcher);
        return e.getMessage();
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
            inferenceResolution.build(),
            externalSourceResolution,
            minimumTransportVersion.get(),
            unmappedResolution,
            timestampBounds
        );
    }

    /**
     * Load a mapping file.
     */
    public static IndexResolution loadMapping(String resource, String indexName, IndexMode indexMode) {
        return IndexResolution.valid(
            new EsIndex(indexName, EsqlTestUtils.loadMapping(resource), Map.of(indexName, indexMode), Map.of(), Map.of(), Map.of())
        );
    }

    /**
     * Load a mapping file in {@link IndexMode#STANDARD}.
     */
    public static IndexResolution loadMapping(String resource, String indexName) {
        return loadMapping(resource, indexName, IndexMode.STANDARD);
    }
}
