/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression.PushedBlockLoaderExpression;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.FieldExtract;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.session.Versioned;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.io.ByteArrayInputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_SEARCH_STATS;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.classpathResources;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Corpus-driven dry run of {@code field_extract} block-loader <em>fusion</em>, the whole-corpus sibling of the
 * offline shape matrix in {@code FieldExtractFusionInventoryTests} (unit tests). Instead of a handful of hand-written
 * shapes it feeds the real csv-spec corpus through the same rewrite + plan + walk pipeline that
 * {@link CsvFlattenedKeywordIT} exercises on a cluster, but stops short of executing anything: each query is
 * <ol>
 *     <li>classified by its leading source command: single- and multi-index {@code FROM}, {@code TS} time-series,
 *     {@code LOOKUP JOIN}, and {@code ENRICH} are all in scope (a leading {@code SET ...;} preamble is stripped
 *     first), and an offline analyzer is built over the corresponding flattened mapping(s). Shapes that cannot be
 *     resolved from a dataset mapping offline &mdash; {@code FORK}, subqueries/unions, remote or wildcard index
 *     patterns, unknown indices, and queries that do not start with a source command &mdash; are tallied under
 *     explicit skip buckets so the coverage stays visible;</li>
 *     <li>rewritten with the production {@link AstKeywordFieldRewriter} so every keyword reference becomes
 *     {@code field_extract(&lt;field&gt;, "&lt;subkey&gt;")}, using the same {@link KeywordToFlattenedTransformer}
 *     keyword&rarr;flattened mapping transform the IT uses to build its indices;</li>
 *     <li>planned through the local physical optimizer, then split at the coordinator/data-node exchange; only the
 *     <b>data-node fragment</b> is walked, because {@code PushExpressionsToFieldLoad} is a data-node-local rule and a
 *     coordinator-side {@code EVAL} is never a fusion candidate.</li>
 * </ol>
 * <p>
 *     The walk counts fused loads (synthetic {@link FieldAttribute}s backed by a {@link FunctionEsField} for the
 *     keyed sub-field) versus surviving {@link FieldExtract} fallbacks, bucketing each fallback by the gate in
 *     {@code PushExpressionsToFieldLoad#transformExpression} that rejected it. Stats are the all-supporting
 *     {@link EsqlTestUtils#TEST_SEARCH_STATS}, so this measures the <em>best case</em>: of the extracts that could
 *     fuse, how many the optimizer actually fuses across real query shapes, and where the residue lands.
 * </p>
 * <p>
 *     This lives in {@code internalClusterTest} only because {@link AstKeywordFieldRewriter} and the flattened mapping
 *     transform do; the test itself needs no cluster and extends {@link ESTestCase}. It is a reporting/audit harness:
 *     the aggregate histogram is logged at {@code INFO}, and the assertions stay loose (the corpus grows over time) so
 *     it does not turn into a brittle golden-count test.
 * </p>
 */
public class FieldExtractFusionCorpusInventoryTests extends ESTestCase {

    private static final Logger logger = LogManager.getLogger(FieldExtractFusionCorpusInventoryTests.class);

    /** Leading source command keyword of a query (e.g. {@code FROM}, {@code TS}, {@code ROW}, {@code SHOW}). */
    private static final Pattern LEADING_COMMAND = Pattern.compile("^\\s*([A-Za-z]+)");

    /** One or more leading {@code SET <name> = <value>;} pragmas, stripped before the real source is classified. */
    private static final Pattern SET_PREAMBLE = Pattern.compile("^(?:\\s*SET\\b[^;]*;)+", Pattern.CASE_INSENSITIVE);

    /**
     * Commands that introduce a second source the offline analyzer cannot resolve. Both {@code LOOKUP JOIN} and
     * {@code ENRICH} are deliberately absent: every {@code lookup-settings.json} dataset is pre-resolved into the shared
     * lookup map ({@link #lookupResolutions()}) and every corpus enrich policy into the analyzer's enrich resolution
     * ({@link #registerCorpusEnrichPolicies}), so both shapes are measured. Only {@code FORK}, which fans out into
     * multiple sub-plans, remains out of scope here.
     */
    private static final Pattern MULTI_SOURCE = Pattern.compile("\\bFORK\\b", Pattern.CASE_INSENSITIVE);

    /** How many representative rewritten queries to log per fallback bucket. */
    private static final int MAX_SAMPLES_PER_REASON = 5;

    /**
     * Why a {@code field_extract} did not fuse. Each non-{@link #FUSED} value corresponds to one gate in
     * {@code PushExpressionsToFieldLoad#transformExpression}, in the order they are checked. Mirrors the enum in
     * {@code FieldExtractFusionInventoryTests}.
     */
    enum Fusion {
        FUSED,
        DYNAMIC_KEY,
        NON_FLATTENED_INPUT,
        UNION_TYPE,
        ABOVE_JOIN_OR_MULTISOURCE,
        UNSUPPORTED_LOADER_CONFIG
    }

    /** Why a corpus query was not measured (i.e. contributed no fused/fallback counts). */
    enum Skip {
        /** Carries a {@code skip_flattened_rewrite:} directive: a documented field_extract limitation. */
        SILENCED,
        /** Not a clean single-dataset {@code FROM <known index>} query (multi-source, subquery, unknown index, ...). */
        NOT_SINGLE_DATASET,
        /** The dataset mapping could not be parsed / transformed into an offline analyzer. */
        DATASET_UNAVAILABLE,
        /** The rewrite touched no in-scope keyword reference, so no {@code field_extract} was introduced. */
        NO_KEYWORD_REFS,
        /** Analysis or planning threw (unsupported command, mapping mismatch, ...). */
        UNPLANNABLE,
        /** Planned, but the query produced no data-node fragment to inspect. */
        NO_DATA_NODE_FRAGMENT
    }

    /**
     * Sub-reason for a {@link Skip#NOT_SINGLE_DATASET} query, so the (large) catch-all bucket can be prioritised for
     * further widening. Exactly one applies per rejected query, tested in the same order as {@link #classifySource}.
     */
    enum NotSingle {
        /** Query does not start with {@code FROM} (e.g. {@code ROW}, {@code SHOW}, {@code TS}, {@code EXPLAIN}). */
        NON_FROM_START,
        /** Leading source token is a wildcard ({@code *}) or cross-cluster ({@code remote:index}) pattern. */
        WILDCARD_OR_REMOTE,
        /** Multi-index {@code FROM a, b} or a sub-query source (comma/paren before the first pipe). */
        MULTI_INDEX_FROM,
        /** Contains a {@code FORK}, which fans out into multiple sub-plans. */
        FORK,
        /** Leading {@code FROM <token>} names an index with no known/loadable dataset mapping. */
        UNKNOWN_INDEX
    }

    /**
     * Result of classifying a query's source: either {@code datasets} (measurable, one entry for a single
     * {@code FROM}/{@code TS}, several for a multi-index {@code FROM a, b}) or {@code reason} is non-null. {@code mode}
     * is the {@link IndexMode} the source reads with ({@code STANDARD} for {@code FROM}, {@code TIME_SERIES} for
     * {@code TS}); {@code indexPattern} is the comma-joined pattern the parser produces, used to key the resolution.
     * Only meaningful when {@code datasets} is non-null.
     */
    private record SourceClass(List<CsvTestsDataLoader.TestDataset> datasets, IndexMode mode, String indexPattern, NotSingle reason) {
        static SourceClass rejected(NotSingle reason) {
            return new SourceClass(null, null, null, reason);
        }
    }

    /** Per-dataset offline analyzer plus the keyword paths the rewriter should wrap. {@code null} analyzer means unusable. */
    private record DatasetPlan(Analyzer analyzer, Set<String> keywordPaths) {}

    private final Map<String, DatasetPlan> datasetPlans = new HashMap<>();

    /** Shared {@link IndexMode#LOOKUP} resolutions for {@code LOOKUP JOIN} targets, built once on first use. */
    private Map<String, IndexResolution> lookupResolutions;

    /**
     * The dry run analyzes and plans the whole corpus, so the analyzer legitimately emits planner warnings
     * ("No limit defined ...", "Field 'x' shadowed by field ...") for many queries. Those are irrelevant to the
     * fusion audit and there is no fixed set to assert, so opt out of the end-of-test warning-header check rather
     * than enumerate thousands of expected warnings.
     */
    @Override
    protected boolean enableWarningsCheck() {
        return false;
    }

    public void testCorpusFusionInventory() {
        assumeTrue("field_extract must be part of this build for the plans to analyze", FieldExtract.isFnFieldExtractCapabilityMet());

        int fused = 0;
        Map<Fusion, Integer> fallbackByReason = new EnumMap<>(Fusion.class);
        Map<Skip, Integer> skipByReason = new EnumMap<>(Skip.class);
        // Breakdown of the (large) NOT_SINGLE_DATASET bucket, to prioritise which shape to widen coverage to next.
        Map<NotSingle, Integer> notSingleByReason = new EnumMap<>(NotSingle.class);
        // Further split of NON_FROM_START by leading command (TS/ROW/SHOW/...), since only index-reading sources fuse.
        Map<String, Integer> nonFromLeadingCommand = new TreeMap<>();
        // A few representative rewritten queries per fallback bucket, so each residual is directly inspectable.
        Map<Fusion, List<String>> fallbackSamples = new EnumMap<>(Fusion.class);
        int measuredQueries = 0;

        for (CsvSpecReader.CsvTestCase testCase : loadAllCsvSpecTestCases()) {
            if (testCase.skipFlattenedRewrite != null && testCase.skipFlattenedRewrite.isBlank() == false) {
                skipByReason.merge(Skip.SILENCED, 1, Integer::sum);
                continue;
            }
            // Drop any leading SET pragmas (e.g. SET unmapped_fields="nullify";) so the real FROM/TS source is classified
            // and planned; the dropped pragma is orthogonal to field_extract fusion on mapped keyword fields.
            String query = stripSetPreamble(testCase.query);
            SourceClass source = classifySource(query);
            if (source.datasets() == null) {
                skipByReason.merge(Skip.NOT_SINGLE_DATASET, 1, Integer::sum);
                notSingleByReason.merge(source.reason(), 1, Integer::sum);
                if (source.reason() == NotSingle.NON_FROM_START) {
                    nonFromLeadingCommand.merge(leadingCommand(query), 1, Integer::sum);
                }
                continue;
            }
            DatasetPlan datasetPlan = datasetPlan(source);
            if (datasetPlan == null || datasetPlan.analyzer() == null) {
                skipByReason.merge(Skip.DATASET_UNAVAILABLE, 1, Integer::sum);
                continue;
            }

            AstKeywordFieldRewriter.RewriteResult rewrite = AstKeywordFieldRewriter.rewrite(
                query,
                q -> datasetPlan.keywordPaths(),
                KeywordToFlattenedTransformer.WRAPPER_SUBKEY,
                List.of()
            );
            if (rewrite.modified() == false) {
                skipByReason.merge(Skip.NO_KEYWORD_REFS, 1, Integer::sum);
                continue;
            }

            PhysicalPlan dataNode;
            try {
                dataNode = dataNodeFragment(datasetPlan.analyzer(), rewrite.rewrittenQuery());
            } catch (Exception | AssertionError e) {
                // Analysis/planning of an arbitrary corpus query can throw either (e.g. the analyzer asserts on an
                // unresolved enrich mode); treat any such failure as unplannable rather than a harness error.
                skipByReason.merge(Skip.UNPLANNABLE, 1, Integer::sum);
                logger.debug(() -> "keyword\u2192flattened dry run: unplannable [" + rewrite.rewrittenQuery() + "]", e);
                continue;
            }
            if (dataNode == null) {
                skipByReason.merge(Skip.NO_DATA_NODE_FRAGMENT, 1, Integer::sum);
                continue;
            }

            Inventory inv = walk(dataNode, TEST_SEARCH_STATS);
            fused += inv.fused();
            inv.fallbackByReason().forEach((reason, count) -> {
                fallbackByReason.merge(reason, count, Integer::sum);
                List<String> samples = fallbackSamples.computeIfAbsent(reason, k -> new ArrayList<>());
                if (samples.size() < MAX_SAMPLES_PER_REASON) {
                    samples.add(collapseWhitespace(rewrite.rewrittenQuery()));
                }
            });
            measuredQueries++;
        }

        logReport(measuredQueries, fused, fallbackByReason, skipByReason, notSingleByReason, nonFromLeadingCommand, fallbackSamples);

        assertThat("corpus dry run must plan at least some single-dataset queries", measuredQueries, greaterThan(0));
        assertThat("field_extract must fuse for at least some real corpus shapes", fused, greaterThan(0));
    }

    // ---- per test-case gating --------------------------------------------------------------------------

    /**
     * Classifies a query's source into either the {@link CsvTestsDataLoader.TestDataset}(s) it reads (measurable), or a
     * {@link NotSingle} reason it is out of scope. Kept in one place so the reject reasons stay mutually exclusive and
     * the {@link Skip#NOT_SINGLE_DATASET} bucket can be broken down for prioritising further widening. {@code FROM} and
     * {@code TS} (time-series) sources are in scope, including multi-index {@code FROM a, b} (merged into one resolution
     * with union types for conflicting fields); index names may be quoted. Trailing {@code LOOKUP JOIN} and {@code ENRICH}
     * stay in scope (resolved from {@link #lookupResolutions()} / the analyzer's enrich resolution); only {@code FORK} and
     * cross-cluster/wildcard sources remain out.
     */
    private static SourceClass classifySource(String query) {
        Matcher command = LEADING_COMMAND.matcher(query);
        if (command.find() == false) {
            return SourceClass.rejected(NotSingle.NON_FROM_START);
        }
        IndexMode mode = switch (command.group(1).toUpperCase(Locale.ROOT)) {
            case "FROM" -> IndexMode.STANDARD;
            case "TS" -> IndexMode.TIME_SERIES;
            default -> null;
        };
        if (mode == null) {
            return SourceClass.rejected(NotSingle.NON_FROM_START);
        }
        // The source list runs from the end of the command keyword to the first pipe (or end of query).
        int firstPipe = query.indexOf('|');
        String sourceList = (firstPipe < 0 ? query.substring(command.end()) : query.substring(command.end(), firstPipe)).trim();
        // A sub-query source (open paren in the source list) is out of scope; a comma is a multi-index list, handled below.
        if (sourceList.indexOf('(') >= 0) {
            return SourceClass.rejected(NotSingle.MULTI_INDEX_FROM);
        }
        // FORK fans out into multiple sub-plans the harness does not model, regardless of the source shape.
        if (MULTI_SOURCE.matcher(query).find()) {
            return SourceClass.rejected(NotSingle.FORK);
        }
        List<CsvTestsDataLoader.TestDataset> datasets = new ArrayList<>();
        List<String> names = new ArrayList<>();
        for (String raw : sourceList.split(",")) {
            String token = unquoteIndexToken(raw.trim());
            if (token.indexOf('*') >= 0 || token.indexOf(':') >= 0) {
                return SourceClass.rejected(NotSingle.WILDCARD_OR_REMOTE);
            }
            CsvTestsDataLoader.TestDataset dataset = CsvTestsDataLoader.CSV_DATASET.get(token);
            if (dataset == null || dataset.mappingFileName() == null) {
                return SourceClass.rejected(NotSingle.UNKNOWN_INDEX);
            }
            datasets.add(dataset);
            names.add(token);
        }
        // The parser joins multi-index patterns with a bare comma (IdentifierBuilder), and IndexPattern equality is on
        // that exact string, so reconstruct it the same way to key the resolution.
        return new SourceClass(datasets, mode, String.join(",", names), null);
    }

    /**
     * Extracts the single index name from a source list: strips surrounding quotes/backticks and any trailing options
     * (e.g. {@code METADATA _id}). Returns the bare token, which the caller matches against known datasets.
     */
    private static String unquoteIndexToken(String sourceList) {
        String token = sourceList;
        if (token.isEmpty() == false) {
            char first = token.charAt(0);
            if (first == '"' || first == '\'' || first == '`') {
                int close = token.indexOf(first, 1);
                token = close > 0 ? token.substring(1, close) : token.substring(1);
                return token;
            }
        }
        int space = token.indexOf(' ');
        return space >= 0 ? token.substring(0, space) : token;
    }

    /** The leading command keyword of a query, upper-cased (e.g. {@code TS}, {@code ROW}); {@code "?"} if none matches. */
    private static String leadingCommand(String query) {
        Matcher matcher = LEADING_COMMAND.matcher(query);
        return matcher.find() ? matcher.group(1).toUpperCase(Locale.ROOT) : "?";
    }

    /** Removes any leading {@code SET ...;} pragmas so the real {@code FROM}/{@code TS} source can be classified/planned. */
    private static String stripSetPreamble(String query) {
        return SET_PREAMBLE.matcher(query).replaceFirst("").stripLeading();
    }

    // ---- offline analyzer construction -----------------------------------------------------------------

    /**
     * Builds (and caches) the offline analyzer + keyword-path set for a classified source: one dataset for a single
     * {@code FROM}/{@code TS}, or several merged into one resolution (with {@link InvalidMappedField} union types for
     * cross-index type conflicts) for a multi-index {@code FROM a, b}. The cache key is the index pattern plus the mode,
     * since the same dataset planned as time-series needs a different resolution. Unusable sources cache a {@code null}
     * analyzer.
     */
    private DatasetPlan datasetPlan(SourceClass source) {
        String cacheKey = source.indexPattern() + '|' + source.mode();
        return datasetPlans.computeIfAbsent(cacheKey, key -> {
            try {
                Set<String> keywordPaths = new HashSet<>();
                Map<String, Map<String, EsField>> perIndexFields = new LinkedHashMap<>();
                Map<String, IndexProperties> concreteIndices = new LinkedHashMap<>();
                for (CsvTestsDataLoader.TestDataset dataset : source.datasets()) {
                    String originalMapping = CsvTestsDataLoader.readMappingFile(dataset);
                    collectKeywordPaths("", LoadMapping.loadMapping(stream(originalMapping)), keywordPaths);
                    String flattened = KeywordToFlattenedTransformer.transformMapping(originalMapping, Set.of()).transformedMapping();
                    perIndexFields.put(dataset.indexName(), LoadMapping.loadMapping(stream(flattened)));
                    concreteIndices.put(dataset.indexName(), new IndexProperties(source.mode(), 0));
                }
                Map<String, EsField> fields = mergeFields(perIndexFields);
                EsIndex index = new EsIndex(source.indexPattern(), fields, concreteIndices, Map.of(), Map.of());

                // TestAnalyzer defers enrich resolution until analyze(), keying each ENRICH occurrence by its Source the way
                // production does, so registering every corpus policy up front resolves whichever ones a query uses.
                TestAnalyzer builder = EsqlTestUtils.analyzer().configuration(TEST_CFG).functionRegistry(TEST_FUNCTION_REGISTRY);
                builder.addIndex(source.indexPattern(), IndexResolution.valid(index));
                lookupResolutions().values().forEach(builder::addLookupIndex);
                registerCorpusEnrichPolicies(builder);
                return new DatasetPlan(builder.buildAnalyzer(TEST_VERIFIER), keywordPaths);
            } catch (Exception e) {
                logger.debug(() -> "keyword\u2192flattened dry run: cannot build analyzer for [" + key + "]", e);
                return new DatasetPlan(null, Set.of());
            }
        });
    }

    /**
     * Merges the per-index (already flattened) top-level field maps into one, mirroring the field-caps merge closely
     * enough for the fusion audit: a field with a single data type across every index that has it keeps its
     * {@link EsField}; a field with more than one data type becomes an {@link InvalidMappedField} (the union-type shape
     * {@code field_extract} must reject). Only top-level fields are merged (nested-object merges across indices are rare
     * in the corpus and, when mismatched, surface as {@code UNPLANNABLE} rather than a wrong fusion verdict).
     */
    private static Map<String, EsField> mergeFields(Map<String, Map<String, EsField>> perIndexFields) {
        if (perIndexFields.size() == 1) {
            return perIndexFields.values().iterator().next();
        }
        Map<String, EsField> firstSeen = new LinkedHashMap<>();
        Map<String, LinkedHashMap<String, Set<String>>> typesToIndices = new LinkedHashMap<>();
        perIndexFields.forEach((indexName, fields) -> fields.forEach((fieldName, field) -> {
            firstSeen.putIfAbsent(fieldName, field);
            typesToIndices.computeIfAbsent(fieldName, k -> new LinkedHashMap<>())
                .computeIfAbsent(field.getDataType().typeName(), k -> new HashSet<>())
                .add(indexName);
        }));
        Map<String, EsField> merged = new LinkedHashMap<>();
        typesToIndices.forEach((fieldName, byType) -> {
            if (byType.size() == 1) {
                merged.put(fieldName, firstSeen.get(fieldName));
            } else {
                merged.put(fieldName, new InvalidMappedField(fieldName, byType));
            }
        });
        return merged;
    }

    /**
     * Registers every corpus enrich policy ({@link CsvTestsDataLoader#ENRICH_POLICIES}) into the analyzer builder, so
     * queries containing {@code ENRICH <policy>} resolve offline. The match type and field come from the policy JSON;
     * the enrich-field types come from the source index mapping ({@link TestAnalyzer#addEnrichPolicy} derives the enrich
     * fields as every mapped field except the match field). Each policy is registered under all {@link Enrich.Mode}s, so
     * bare {@code ENRICH policy} as well as {@code _coordinator:}/{@code _remote:} pinned occurrences resolve (an
     * unregistered mode would make the analyzer assert on an unresolved policy rather than fail softly). Registration is
     * best-effort per policy: one whose JSON or mapping cannot be read is skipped rather than breaking the whole analyzer.
     */
    private static void registerCorpusEnrichPolicies(TestAnalyzer builder) {
        for (CsvTestsDataLoader.EnrichConfig policy : CsvTestsDataLoader.ENRICH_POLICIES.values()) {
            try {
                String matchType;
                String matchField;
                try (
                    XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, policy.loadPolicy())
                ) {
                    parser.nextToken(); // START_OBJECT
                    parser.nextToken(); // FIELD_NAME: the match type (match / range / geo_match)
                    matchType = parser.currentName();
                    parser.nextToken(); // START_OBJECT: the policy body
                    matchField = (String) parser.map().get("match_field");
                }
                for (Enrich.Mode mode : Enrich.Mode.values()) {
                    builder.addEnrichPolicy(
                        mode,
                        matchType,
                        policy.policyName(),
                        matchField,
                        policy.index(),
                        "mapping-" + policy.index() + ".json"
                    );
                }
            } catch (Exception e) {
                logger.debug(() -> "keyword\u2192flattened dry run: cannot register enrich policy [" + policy.policyName() + "]", e);
            }
        }
    }

    /**
     * Shared {@code LOOKUP JOIN} target resolutions (keyed by index name, {@link IndexMode#LOOKUP}), built once from
     * every {@code lookup-settings.json} dataset with the same keyword&#8594;flattened mapping remap applied. Attaching
     * these to every per-dataset analyzer lets {@code FROM <main> | ... | LOOKUP JOIN <lookup> ON <key>} shapes plan
     * offline. The join key is never a keyword the rewriter wraps (see the {@code LOOKUP_JOIN_ON} skip site in
     * {@code CsvFlattenedKeywordIT}), so it stays intact across the flatten and the join still type-checks. Datasets
     * whose mapping cannot be parsed are simply omitted; queries that reference them fall to {@code UNPLANNABLE}.
     */
    private Map<String, IndexResolution> lookupResolutions() {
        if (lookupResolutions == null) {
            Map<String, IndexResolution> resolutions = new HashMap<>();
            for (CsvTestsDataLoader.TestDataset dataset : CsvTestsDataLoader.CSV_DATASET.values()) {
                if ("lookup-settings.json".equals(dataset.settingFileName()) == false || dataset.mappingFileName() == null) {
                    continue;
                }
                resolutions.computeIfAbsent(dataset.indexName(), name -> {
                    try {
                        String flattened = KeywordToFlattenedTransformer.transformMapping(
                            CsvTestsDataLoader.readMappingFile(dataset),
                            Set.of()
                        ).transformedMapping();
                        Map<String, EsField> fields = LoadMapping.loadMapping(stream(flattened));
                        EsIndex index = new EsIndex(
                            name,
                            fields,
                            Map.of(name, new IndexProperties(IndexMode.LOOKUP, 0)),
                            Map.of(),
                            Map.of()
                        );
                        return IndexResolution.valid(index);
                    } catch (Exception e) {
                        logger.debug(() -> "keyword\u2192flattened dry run: cannot build lookup resolution for [" + name + "]", e);
                        return null;
                    }
                });
            }
            lookupResolutions = Collections.unmodifiableMap(resolutions);
        }
        return lookupResolutions;
    }

    private static ByteArrayInputStream stream(String json) {
        return new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
    }

    /** Recursively collects the dotted paths of every {@code KEYWORD}-typed field, matching what the transform flattens. */
    private static void collectKeywordPaths(String prefix, Map<String, EsField> fields, Set<String> out) {
        for (Map.Entry<String, EsField> entry : fields.entrySet()) {
            EsField field = entry.getValue();
            String path = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
            if (field.getDataType() == DataType.KEYWORD) {
                out.add(path);
            }
            if (field.getProperties().isEmpty() == false) {
                collectKeywordPaths(path, field.getProperties(), out);
            }
        }
    }

    // ---- planning --------------------------------------------------------------------------------------

    /**
     * Analyzes, optimizes and localizes a query, then returns just the data-node fragment (below the exchange), which
     * is where {@code PushExpressionsToFieldLoad} runs. Returns {@code null} when there is no data-node fragment.
     * Mirrors the pipeline in {@code TestPlannerOptimizer} (which lives in the unit-test source set and is not visible
     * here), minus the local-reduction alignment that does not affect field-load fusion.
     */
    private PhysicalPlan dataNodeFragment(Analyzer analyzer, String query) {
        var minVersion = analyzer.context().minimumVersion();
        var logicalOptimizer = new LogicalPlanOptimizer(new LogicalOptimizerContext(TEST_CFG, FoldContext.small(), minVersion));
        var physicalOptimizer = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(TEST_CFG, minVersion));
        var localLogical = new LocalLogicalPlanOptimizer(
            new LocalLogicalOptimizerContext(TEST_CFG, FoldContext.small(), TEST_SEARCH_STATS)
        );
        var localPhysical = new LocalPhysicalPlanOptimizer(
            new LocalPhysicalOptimizerContext(
                PlannerSettings.DEFAULTS,
                new EsqlFlags(true),
                TEST_CFG,
                FoldContext.small(),
                TEST_SEARCH_STATS
            )
        );

        LogicalPlan logical = logicalOptimizer.optimize(analyzer.analyze(EsqlTestUtils.TEST_PARSER.parseQuery(query)));
        PhysicalPlan physical = new Mapper().map(new Versioned<>(logical, minVersion));
        physical = EstimatesRowSize.estimateRowSize(0, physicalOptimizer.optimize(physical));
        PhysicalPlan localized = PlannerUtils.localPlan(physical, localLogical, localPhysical, null);

        Tuple<PhysicalPlan, PhysicalPlan> split = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(localized, TEST_CFG);
        return split.v2();
    }

    // ---- walker (ported from FieldExtractFusionInventoryTests) -----------------------------------------

    /** Outcome of walking one data-node fragment: how many loads fused, and the fallback bucket histogram. */
    record Inventory(int fused, Map<Fusion, Integer> fallbackByReason) {}

    private Inventory walk(PhysicalPlan plan, SearchStats stats) {
        // Count distinct fused loads by attribute NameId: the same synthetic attribute recurs across plan nodes
        // (so we must de-dup), and NameId is stable per logical attribute, avoiding both instance double-counting
        // and the name collisions that a plain name set would silently collapse.
        Set<NameId> fused = new HashSet<>();
        Map<Fusion, Integer> fallbackByReason = new EnumMap<>(Fusion.class);
        Set<FieldExtract> seen = Collections.newSetFromMap(new IdentityHashMap<>());

        plan.forEachDown(PhysicalPlan.class, node -> {
            node.forEachExpressionDown(FieldAttribute.class, fa -> {
                if (fa.field() instanceof FunctionEsField fe
                    && fe.functionConfig() != null
                    && fe.functionConfig().function() == BlockLoaderFunctionConfig.Function.EXTRACT_FLATTENED_SUBFIELD) {
                    fused.add(fa.id());
                }
            });
            node.forEachExpressionDown(FieldExtract.class, fx -> {
                if (seen.add(fx)) {
                    fallbackByReason.merge(classify(fx, stats), 1, Integer::sum);
                }
            });
        });
        return new Inventory(fused.size(), fallbackByReason);
    }

    /**
     * Re-derives the fusion decision for a residual {@link FieldExtract}, mirroring the gate order in
     * {@code PushExpressionsToFieldLoad#transformExpression} (including the configured field-extract preference).
     * Kept in sync with the classifier in {@code FieldExtractFusionInventoryTests}.
     */
    private Fusion classify(FieldExtract fx, SearchStats stats) {
        PushedBlockLoaderExpression fuse = fx.tryPushToFieldLoading(stats);
        if (fuse == null) {
            Expression pathArg = fx.children().get(1);
            return pathArg.foldable() ? Fusion.NON_FLATTENED_INPUT : Fusion.DYNAMIC_KEY;
        }
        if (fuse.field().field() instanceof UnionTypeEsField) {
            return Fusion.UNION_TYPE;
        }
        // Mirror PushExpressionsToFieldLoad, which passes the configured pragma preference (not a hardcoded NONE),
        // so the audit stays aligned if the default field-extract preference ever changes.
        if (stats.supportsLoaderConfig(fuse.field().fieldName(), fuse.config(), TEST_CFG.pragmas().fieldExtractPreference()) == false) {
            return Fusion.UNSUPPORTED_LOADER_CONFIG;
        }
        return Fusion.ABOVE_JOIN_OR_MULTISOURCE;
    }

    // ---- corpus enumeration + reporting ----------------------------------------------------------------

    /** Loads every csv-spec test case on the classpath, the same way {@link CsvFlattenedKeywordIT} does. */
    private static List<CsvSpecReader.CsvTestCase> loadAllCsvSpecTestCases() {
        try {
            List<URL> urls = classpathResources("/*.csv-spec");
            List<Object[]> rows = SpecReader.readScriptSpec(urls, CsvSpecReader::specParser);
            List<CsvSpecReader.CsvTestCase> cases = new ArrayList<>(rows.size());
            for (Object[] row : rows) {
                if (row[4] instanceof CsvSpecReader.CsvTestCase tc) {
                    cases.add(tc);
                }
            }
            return cases;
        } catch (Exception e) {
            throw new AssertionError("failed to enumerate csv-spec resources", e);
        }
    }

    private static void logReport(
        int measured,
        int fused,
        Map<Fusion, Integer> fallbackByReason,
        Map<Skip, Integer> skipByReason,
        Map<NotSingle, Integer> notSingleByReason,
        Map<String, Integer> nonFromLeadingCommand,
        Map<Fusion, List<String>> fallbackSamples
    ) {
        int fallback = fallbackByReason.values().stream().mapToInt(Integer::intValue).sum();
        StringBuilder report = new StringBuilder("field_extract corpus fusion inventory: ").append(measured)
            .append(" measured queries, ")
            .append(fused)
            .append(" fused, ")
            .append(fallback)
            .append(" fallback");
        fallbackByReason.forEach((reason, count) -> report.append(", ").append(reason).append('=').append(count));
        logger.info(report.toString());

        StringBuilder skips = new StringBuilder("field_extract corpus fusion inventory (skipped)");
        // EnumMap iterates in declaration order; copy into a TreeMap keyed by name only to keep the log stable if reordered.
        Map<String, Integer> stable = new TreeMap<>();
        skipByReason.forEach((reason, count) -> stable.put(reason.name(), count));
        stable.forEach((reason, count) -> skips.append(": ").append(reason).append('=').append(count));
        logger.info(skips.toString());

        StringBuilder notSingle = new StringBuilder("field_extract corpus fusion inventory (not-single-dataset breakdown)");
        notSingleByReason.forEach((reason, count) -> notSingle.append(": ").append(reason).append('=').append(count));
        logger.info(notSingle.toString());

        StringBuilder nonFrom = new StringBuilder("field_extract corpus fusion inventory (non-FROM leading command)");
        nonFromLeadingCommand.forEach((cmd, count) -> nonFrom.append(": ").append(cmd).append('=').append(count));
        logger.info(nonFrom.toString());

        // One log line per sampled fallback query, so each residual bucket is directly inspectable.
        fallbackSamples.forEach((reason, samples) -> {
            for (String sample : samples) {
                logger.info("field_extract fallback sample [{}]: {}", reason, sample);
            }
        });
    }

    private static String collapseWhitespace(String query) {
        return query.replaceAll("\\s+", " ").trim();
    }
}
