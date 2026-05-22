/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.junit.AssumptionViolatedException;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Integration test that runs the {@link CsvIT} csv-spec corpus against indices where every field
 * declared as {@code keyword} in the dataset's mapping has been rewritten to {@code flattened},
 * each source document has its keyword field values wrapped accordingly, and every reference to
 * such a field in the query is wrapped in a call to
 * {@code field_extract(<field>, "<wrapper sub-key>")}.
 * <p>
 * The goal is to exercise the {@code field_extract} function in every place where a keyword field
 * appears in the existing csv-spec corpus, so that we can inventory which ES|QL surfaces are (and
 * are not) compatible with replacing a keyword field with {@code flattened + field_extract}. Tests
 * whose query references no keyword field are skipped, because re-running them with the same query
 * and the same expected results would only re-test unmodified behavior.
 * <p>
 * Opt-in: this test does not run by default. Pass
 * {@code -Dtests.run_keyword_flattened_variant=true} to enable it. To also log the full rewritten
 * query (one multi-line {@code INFO} message per launched test, prefixed with
 * {@code keyword→flattened: rewritten query:}), additionally pass
 * {@code -Dtests.run_keyword_flattened_variant.log_queries=true}. The short
 * {@code keyword→flattened: launched; rewrote field references [...]} marker is always emitted so
 * that the per-test rewrite is correlatable from logs even when query logging is off.
 *
 * <h2>Decision-transparency log lines</h2>
 * Every place the variant declines to convert or wrap a keyword field is reported in the run log
 * with a stable {@code keyword→flattened:} prefix so the user can {@code grep} an inventory of
 * "where {@code field_extract} was not exercised, and why" without re-reading the rewriter source.
 * Three categories are emitted:
 * <ul>
 *   <li>{@code skip-convert; dataset=&lt;X&gt;; field=&lt;Y&gt;; reason=...} &mdash; once at startup
 *       per keyword field that this variant will never convert to {@code flattened}. Two reasons
 *       are produced: {@code reason=enrich-policy match_field for [policy]} for fields that must
 *       remain {@code keyword} in the source dataset of an enrich policy, and
 *       {@code reason=keyword carries [param]} for fields whose declaration uses a parameter in
 *       {@link KeywordToFlattenedTransformer#PARAMS_INCOMPATIBLE_WITH_FLATTENED}.</li>
 *   <li>{@code scope-excluded; field=&lt;X&gt;; reason=keyword in {...} but non-keyword in {...}} &mdash;
 *       per launched query (and per recursive subquery), once per field whose cross-dataset path
 *       intersection eliminates it from the rewrite scope. The two dataset sets identify which
 *       indices contributed each side of the conflict.</li>
 *   <li>{@code skip-wrap; site=&lt;SITE&gt;; field=&lt;X&gt;; reason=...} &mdash; per launched query,
 *       once per distinct {@code (site, field)} pair the rewriter's
 *       {@link EsqlQueryKeywordFieldRewriter.SkipSite} machinery declined to wrap. Sites cover
 *       grammar slots that accept only an attribute (e.g. {@code MV_EXPAND}, {@code ENRICH ON})
 *       and the LHS of the match operator {@code :}. These positions are still exercised at the
 *       runtime layer (the bare attribute reaches the engine on a {@code flattened} column), but
 *       {@code field_extract} itself is not tested there.</li>
 * </ul>
 *
 * <p>Known limitations:</p>
 * <ul>
 *   <li>Only top-level and {@code properties}-nested keyword fields are converted; multi-field
 *       sub-fields under {@code fields} are left alone (see {@link KeywordToFlattenedTransformer}).</li>
 *   <li>Datasets whose keyword fields are referenced by special index modes (e.g. time-series
 *       dimensions) may fail at index creation time.</li>
 *   <li>Query rewriting is regex-driven; ES|QL grammar surfaces that take an attribute but not an
 *       arbitrary expression (e.g. {@code DISSECT &lt;field&gt;}, {@code LOOKUP JOIN ... ON &lt;field&gt;})
 *       will fail when the field is wrapped in a function call. Those failures are the intended
 *       output of this test. See {@link EsqlQueryKeywordFieldRewriter} for the full list.</li>
 *   <li>Output column types: {@code field_extract} is only injected in expression contexts, so a
 *       converted keyword field that is projected directly (e.g. {@code KEEP first_name},
 *       {@code SORT first_name}, or appearing untouched in the output of a STATS-less query) comes
 *       through with type {@code flattened}, while csv-spec expected results declare the column
 *       as {@code keyword}. These tests will fail with a column-type mismatch &mdash; that failure
 *       is the intended signal that those particular surfaces require an additional projection
 *       step (e.g. {@code EVAL field = field_extract(field, "v")} immediately before the
 *       projection) to fully recover keyword semantics. Synthesizing that extra projection is not
 *       yet implemented.</li>
 * </ul>
 */
public class CsvFlattenedKeywordIT extends CsvIT {

    private static final Logger logger = LogManager.getLogger(CsvFlattenedKeywordIT.class);

    /**
     * When {@code true}, {@link KeywordToFlattenedStrategy#transformQuery} additionally emits the
     * full rewritten query as an {@code INFO} log line prefixed with
     * {@code keyword→flattened: rewritten query:}. When unset or {@code false} the rewriter only
     * emits the short {@code keyword→flattened: launched; rewrote field references [...]} marker.
     * <p>
     * Defaults to {@code false} because the rewritten query is multi-line and one block per
     * launched csv-spec test produces a few thousand entries when running the full corpus &mdash;
     * useful while analyzing a run, noisy as a default.
     */
    public static final String LOG_REWRITTEN_QUERIES_PROPERTY = "tests.run_keyword_flattened_variant.log_queries";

    public CsvFlattenedKeywordIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvSpecReader.CsvTestCase testCase,
        String instructions
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions);
    }

    /**
     * Runs after {@link CsvIT#setupCluster()} (which JUnit guarantees runs first because it is
     * declared on the superclass) and replaces the identity strategy with one that rewrites
     * keyword fields to flattened, wraps source values, and rewrites the query.
     */
    @BeforeClass
    public static void installKeywordToFlattenedStrategy() {
        indexLoadStrategy = new KeywordToFlattenedStrategy();
    }

    /**
     * Strategy implementation that delegates JSON manipulation to {@link KeywordToFlattenedTransformer}
     * and query rewriting to {@link EsqlQueryKeywordFieldRewriter}.
     * <p>
     * Three pieces of state are kept, all populated eagerly at construction time:
     * <ul>
     *   <li>{@code protectedKeywordPathsByEnrichSourceIndex} &mdash; for every enrich policy
     *       declared in {@link CsvTestsDataLoader#ENRICH_POLICIES}, the policy's
     *       {@code match_field} (under the policy's source index). These paths must remain
     *       {@code keyword} regardless of the variant under test: the enrich-policy reindex into
     *       the internal {@code .enrich-*} index expects a {@code keyword} value at the match
     *       field, and converting the source field to {@code flattened} causes that reindex to
     *       fail. The failure is cached by {@link CsvIT}'s resource loader, so every later test in
     *       the same JVM fails with the same {@code RuntimeException: Resource loading failure}.
     *       Excluding the match field at the source keeps the policy load deterministic.</li>
     *   <li>{@code keywordPathsByDatasetIndexName} &mdash; the keyword paths that
     *       {@link KeywordToFlattenedTransformer} actually rewrites for each dataset (after the
     *       enrich-policy exclusions above are applied). Used both to wrap per-document source
     *       values when {@link CsvIT} indexes the dataset, and as one input to the per-query
     *       scope computed by {@link #resolveKeywordPathsForQuery}.</li>
     *   <li>{@code nonKeywordPathsByDatasetIndexName} &mdash; every dotted field path declared in
     *       a dataset's mapping whose type is <em>not</em> {@code keyword} (e.g. {@code long},
     *       {@code ip}, {@code text}, {@code geo_point}, plus keyword-typed paths that the
     *       transformer's denylist or the per-dataset enrich-policy exclusions kept as keyword).
     *       Used to subtract cross-dataset conflicts from the per-query keyword scope &mdash; if
     *       a field exists as keyword in one dataset and as a non-keyword type in another dataset
     *       referenced by the same query, wrapping the field in {@code field_extract(...)} would
     *       fail verification on the second dataset, so the field is excluded from the rewrite
     *       scope for that query.</li>
     * </ul>
     */
    private static final class KeywordToFlattenedStrategy implements IndexLoadStrategy {
        private final Map<String, Set<String>> protectedKeywordPathsByEnrichSourceIndex;
        private final Map<String, Set<String>> keywordPathsByDatasetIndexName;
        private final Map<String, Set<String>> nonKeywordPathsByDatasetIndexName;

        KeywordToFlattenedStrategy() {
            EnrichExclusionResult enrichResult = computeEnrichMatchFieldExclusions();
            this.protectedKeywordPathsByEnrichSourceIndex = enrichResult.byIndex();
            DatasetPathsResult datasetPaths = computeDatasetPaths(protectedKeywordPathsByEnrichSourceIndex);
            this.keywordPathsByDatasetIndexName = datasetPaths.keywordPathsByDatasetIndexName();
            this.nonKeywordPathsByDatasetIndexName = datasetPaths.nonKeywordPathsByDatasetIndexName();
            // C-class transparency: emit one INFO line for every keyword field that this variant will
            // intentionally never convert. The user can grep for "skip-convert" to inventory the
            // permanent blind spots of the test variant. Both sources are emitted here:
            // * enrich-policy match_fields: the policy reindex into .enrich-* requires keyword,
            // and converting the source field would poison the test resource loader.
            // * mapping-denylist hits: keyword fields declaring TSDB / multi-fields / copy_to /
            // runtime-script parameters that are incompatible with flattened.
            logEnrichMatchFieldExclusions(enrichResult.exclusions());
            logMappingDenylistHits(datasetPaths.skippedFieldsByDataset());
        }

        /**
         * Bundles the per-dataset path maps that {@link #computeDatasetPaths} produces in a single
         * mapping pass: one entry per declared field is classified as either keyword-being-rewritten
         * or not, so a single walk of the mapping populates both maps deterministically. The
         * {@link #skippedFieldsByDataset} map carries the {@link KeywordToFlattenedTransformer}'s
         * own report of every keyword field it left untouched (with reasons), so the strategy can
         * forward those decisions to the logger without re-reading the mapping.
         */
        private record DatasetPathsResult(
            Map<String, Set<String>> keywordPathsByDatasetIndexName,
            Map<String, Set<String>> nonKeywordPathsByDatasetIndexName,
            Map<String, List<KeywordToFlattenedTransformer.SkippedField>> skippedFieldsByDataset
        ) {}

        /**
         * Walks every dataset's mapping resource once and produces both per-dataset path maps that
         * {@link #resolveKeywordPathsForQuery} relies on:
         * <ul>
         *   <li>For each dataset, the keyword paths that
         *       {@link KeywordToFlattenedTransformer#transformMapping(String, Set)} actually
         *       converts to {@code flattened} given the per-dataset enrich-policy exclusions in
         *       {@code protectedByIndex} (the enrich-policy {@code match_field} stays
         *       {@code keyword} but is intentionally absent from this map, so the rewriter does
         *       not wrap references to it).</li>
         *   <li>For each dataset, every other dotted leaf field path declared in the mapping
         *       (i.e. all paths returned by {@link KeywordToFlattenedTransformer#extractAllFieldPaths}
         *       minus the keyword paths above). This includes both true non-keyword leaves (long,
         *       ip, text, geo_point, &hellip;) and keyword-typed leaves that the transformer
         *       denylist or the enrich-policy exclusion left intact.</li>
         *   <li>For each dataset, the {@link KeywordToFlattenedTransformer.SkippedField} entries
         *       describing every keyword field declaration that was intentionally not converted.
         *       Forwarded to the strategy's startup logger so the test variant's permanent blind
         *       spots are visible in the run output.</li>
         * </ul>
         * The transformed mapping itself is discarded here; the per-index transformation still
         * runs through {@link #transformMapping(CsvTestsDataLoader.TestDataset, String)} when
         * {@link CsvIT} actually loads each index.
         */
        private static DatasetPathsResult computeDatasetPaths(Map<String, Set<String>> protectedByIndex) {
            Map<String, Set<String>> keywordMap = new HashMap<>();
            Map<String, Set<String>> nonKeywordMap = new HashMap<>();
            Map<String, List<KeywordToFlattenedTransformer.SkippedField>> skippedMap = new HashMap<>();
            for (CsvTestsDataLoader.TestDataset dataset : CsvTestsDataLoader.CSV_DATASET.values()) {
                if (dataset.mappingFileName() == null) {
                    continue;
                }
                try {
                    String mapping = CsvTestsDataLoader.readMappingFile(dataset);
                    Set<String> excluded = protectedByIndex.getOrDefault(dataset.indexName(), Set.of());
                    KeywordToFlattenedTransformer.MappingResult result = KeywordToFlattenedTransformer.transformMapping(mapping, excluded);
                    Set<String> keywordPaths = Set.copyOf(result.keywordFieldPaths());
                    Set<String> allPaths = KeywordToFlattenedTransformer.extractAllFieldPaths(mapping);
                    Set<String> nonKeywordPaths = new HashSet<>(allPaths);
                    nonKeywordPaths.removeAll(keywordPaths);
                    keywordMap.put(dataset.indexName(), keywordPaths);
                    nonKeywordMap.put(dataset.indexName(), Set.copyOf(nonKeywordPaths));
                    if (result.skippedFields().isEmpty() == false) {
                        skippedMap.put(dataset.indexName(), result.skippedFields());
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException("failed to read mapping for dataset [" + dataset.indexName() + "]", e);
                }
            }
            return new DatasetPathsResult(Map.copyOf(keywordMap), Map.copyOf(nonKeywordMap), Map.copyOf(skippedMap));
        }

        /**
         * One enrich policy that contributes a {@code match_field} to the per-source-index
         * exclusion set, with enough metadata for the startup logger to attribute each exclusion
         * to its policy. Used only as a logging carrier; the actual exclusion check inside the
         * transformer goes through the by-index map.
         *
         * @param policyName  the enrich policy name as declared in
         *                    {@link CsvTestsDataLoader#ENRICH_POLICIES}
         * @param sourceIndex the {@code index} field of the policy &mdash; the dataset whose
         *                    mapping the transformer will skip the {@code match_field} in
         * @param matchField  the field path that must remain {@code keyword} in the source dataset
         */
        private record EnrichMatchFieldExclusion(String policyName, String sourceIndex, String matchField) {}

        /**
         * Bundles the by-source-index protected-paths map (used at runtime by the transformer)
         * with the per-policy exclusion list (used at startup by the logger). Both views are
         * populated by a single walk over {@link CsvTestsDataLoader#ENRICH_POLICIES} so the test
         * variant cannot drift between "what we tell the transformer to keep as keyword" and
         * "what we report to the user as kept".
         */
        private record EnrichExclusionResult(Map<String, Set<String>> byIndex, List<EnrichMatchFieldExclusion> exclusions) {}

        /**
         * Reads every entry in {@link CsvTestsDataLoader#ENRICH_POLICIES}, parses out each
         * policy's {@code match_field}, and returns both
         * <ol>
         *   <li>a by-source-index map (the value passed to
         *       {@link KeywordToFlattenedTransformer#transformMapping(String, Set)}), and</li>
         *   <li>a per-policy list (used by the startup logger to attribute each exclusion to its
         *       policy by name).</li>
         * </ol>
         * Policy type ({@code match}, {@code geo_match}, {@code range}) is irrelevant here:
         * adding a non-keyword path to the by-index set is a no-op inside the transformer because
         * it only ever rewrites declared {@code keyword} fields, so all policies are accepted and
         * the transformer ignores the paths it would not have touched.
         */
        private static EnrichExclusionResult computeEnrichMatchFieldExclusions() {
            ObjectMapper jsonMapper = new ObjectMapper();
            Map<String, Set<String>> exclusionsByIndex = new HashMap<>();
            List<EnrichMatchFieldExclusion> perPolicy = new ArrayList<>();
            for (CsvTestsDataLoader.EnrichConfig policy : CsvTestsDataLoader.ENRICH_POLICIES.values()) {
                String matchField = readMatchFieldFromPolicy(jsonMapper, policy);
                if (matchField == null) {
                    continue;
                }
                exclusionsByIndex.computeIfAbsent(policy.index(), k -> new HashSet<>()).add(matchField);
                perPolicy.add(new EnrichMatchFieldExclusion(policy.policyName(), policy.index(), matchField));
            }
            Map<String, Set<String>> immutable = new HashMap<>();
            for (Map.Entry<String, Set<String>> entry : exclusionsByIndex.entrySet()) {
                immutable.put(entry.getKey(), Set.copyOf(entry.getValue()));
            }
            return new EnrichExclusionResult(Map.copyOf(immutable), List.copyOf(perPolicy));
        }

        /**
         * Parses {@code policy.loadPolicy()} and returns the {@code match_field} value of its
         * single policy-type block (the top-level key is the policy type: {@code match},
         * {@code geo_match}, or {@code range}). Returns {@code null} if the policy JSON does not
         * conform to that shape.
         */
        private static String readMatchFieldFromPolicy(ObjectMapper jsonMapper, CsvTestsDataLoader.EnrichConfig policy) {
            try {
                JsonNode root = jsonMapper.readTree(policy.loadPolicy());
                if (root.isObject() == false) {
                    return null;
                }
                Iterator<Map.Entry<String, JsonNode>> fields = root.fields();
                if (fields.hasNext() == false) {
                    return null;
                }
                JsonNode body = fields.next().getValue();
                if (body.isObject() == false) {
                    return null;
                }
                JsonNode matchFieldNode = body.path("match_field");
                return matchFieldNode.isTextual() ? matchFieldNode.asText() : null;
            } catch (IOException e) {
                throw new UncheckedIOException("failed to read enrich policy [" + policy.policyName() + "]", e);
            }
        }

        @Override
        public String transformMapping(CsvTestsDataLoader.TestDataset dataset, String originalMapping) throws IOException {
            Set<String> excluded = protectedKeywordPathsByEnrichSourceIndex.getOrDefault(dataset.indexName(), Set.of());
            return KeywordToFlattenedTransformer.transformMapping(originalMapping, excluded).transformedMapping();
        }

        @Override
        public String transformDocument(CsvTestsDataLoader.TestDataset dataset, String originalDocumentJson) throws IOException {
            Set<String> paths = keywordPathsByDatasetIndexName.getOrDefault(dataset.indexName(), Set.of());
            return KeywordToFlattenedTransformer.wrapKeywordValuesAsFlattened(originalDocumentJson, paths);
        }

        @Override
        public String transformQuery(CsvSpecReader.CsvTestCase testCase) {
            String originalQuery = testCase.query;
            List<String> expectedColumnOrder = parseExpectedColumnOrder(testCase.expectedResults);
            // Pass the per-query resolver itself rather than a pre-resolved flat scope so that the
            // rewriter can re-resolve scope for each parenthesised sub-pipeline it recurses into.
            // The outer scope is the union of every dataset the top-level FROM/TS/LOOKUP JOIN
            // touches (minus cross-dataset non-keyword conflicts), but a subquery's FROM typically
            // references only a subset of those datasets and therefore must not see fields the
            // subquery's schema does not produce.
            EsqlQueryKeywordFieldRewriter.RewriteResult result = EsqlQueryKeywordFieldRewriter.rewrite(
                originalQuery,
                this::resolveKeywordPathsForQuery,
                KeywordToFlattenedTransformer.WRAPPER_SUBKEY,
                expectedColumnOrder
            );
            if (result.modified() == false) {
                // Logged at INFO so the launched/skipped split is visible in the test JVM stdout, alongside the
                // assumption-violation message that surfaces in the JUnit XML <skipped> element.
                logger.info("keyword→flattened: skipping; no keyword field references in query");
                throw new AssumptionViolatedException(
                    "skipping: query references no keyword field that this variant rewrites to flattened, "
                        + "so re-running the spec would only re-test the unmodified behavior"
                );
            }
            // The short "launched" marker is emitted unconditionally so that every launched test has a single,
            // grep-able log line tying the test method (from the JUnit thread context) to the set of fields
            // the rewriter actually wrapped. The multi-line rewritten query itself is gated on
            // LOG_REWRITTEN_QUERIES_PROPERTY because, at full-corpus scale, dumping every rewritten query at
            // INFO produces thousands of multi-line blocks &mdash; useful when analyzing a specific run, but
            // noisy enough to bury the rest of the test output when always on.
            logger.info("keyword→flattened: launched; rewrote field references {}", result.rewrittenFieldNames());
            // B-class transparency: emit one INFO line per intentionally-skipped wrap site so the
            // user can grep for "skip-wrap" and inventory exactly which positions in this query
            // the rewriter declined to wrap, and why. Aggregated by (site, field) so a body that
            // mentions the same field twice surfaces a single line.
            logRewriterSkipEvents(result.skipEvents());
            if (Boolean.getBoolean(LOG_REWRITTEN_QUERIES_PROPERTY)) {
                logger.info("keyword→flattened: rewritten query:\n{}", result.rewrittenQuery());
            }
            return result.rewrittenQuery();
        }

        /**
         * Emits one INFO line per distinct {@code (site, field)} pair in {@code events}, sorted by
         * {@code site} then {@code field} so the output is deterministic across runs (the
         * rewriter's iteration order is otherwise dependent on the in-scope set's iteration
         * order). Each line carries the site, the field name, and a reason string short enough
         * that a {@code grep "site=MV_EXPAND_ARG"} or
         * {@code grep "site=MATCH_OPERATOR_LHS"} produces a usable inventory.
         */
        private static void logRewriterSkipEvents(List<EsqlQueryKeywordFieldRewriter.SkipEvent> events) {
            if (events.isEmpty()) {
                return;
            }
            Map<EsqlQueryKeywordFieldRewriter.SkipSite, Set<String>> bySite = new TreeMap<>();
            for (EsqlQueryKeywordFieldRewriter.SkipEvent event : events) {
                bySite.computeIfAbsent(event.site(), k -> new TreeSet<>()).add(event.field());
            }
            for (Map.Entry<EsqlQueryKeywordFieldRewriter.SkipSite, Set<String>> entry : bySite.entrySet()) {
                String reason = skipSiteReason(entry.getKey());
                for (String field : entry.getValue()) {
                    logger.info("keyword→flattened: skip-wrap; site={}; field={}; reason={}", entry.getKey(), field, reason);
                }
            }
        }

        /**
         * Human-readable reason string for each {@link EsqlQueryKeywordFieldRewriter.SkipSite}.
         * Kept as a centralised mapping (rather than as a field on {@code SkipEvent}) so the
         * reason text can evolve with the test variant's documentation without changing the
         * rewriter's API surface.
         */
        private static String skipSiteReason(EsqlQueryKeywordFieldRewriter.SkipSite site) {
            return switch (site) {
                case MV_EXPAND_ARG -> "MV_EXPAND grammar slot accepts only an attribute, not an expression";
                case ENRICH_BODY -> "ENRICH ON / WITH grammar slots accept only attributes, not expressions";
                case MATCH_OPERATOR_LHS -> "match operator [:] LHS accepts only an attribute, not an expression";
            };
        }

        /**
         * Emits one INFO line per enrich policy whose {@code match_field} this variant keeps as
         * {@code keyword} in the policy's source dataset. The user can grep for
         * {@code skip-convert} to inventory every field the test variant will never exercise with
         * {@code field_extract}, and grep for {@code reason=enrich} to narrow down to this
         * subset.
         */
        private static void logEnrichMatchFieldExclusions(List<EnrichMatchFieldExclusion> exclusions) {
            List<EnrichMatchFieldExclusion> sorted = new ArrayList<>(exclusions);
            sorted.sort(
                Comparator.comparing(EnrichMatchFieldExclusion::sourceIndex)
                    .thenComparing(EnrichMatchFieldExclusion::matchField)
                    .thenComparing(EnrichMatchFieldExclusion::policyName)
            );
            for (EnrichMatchFieldExclusion exclusion : sorted) {
                logger.info(
                    "keyword→flattened: skip-convert; dataset={}; field={}; reason=enrich-policy match_field for [{}]",
                    exclusion.sourceIndex(),
                    exclusion.matchField(),
                    exclusion.policyName()
                );
            }
        }

        /**
         * Emits one INFO line per keyword field declaration that
         * {@link KeywordToFlattenedTransformer} left untouched because the field declared a
         * {@link KeywordToFlattenedTransformer#PARAMS_INCOMPATIBLE_WITH_FLATTENED}
         * parameter. The {@link KeywordToFlattenedTransformer.SkipReason#CALLER_EXCLUDED_PATH}
         * subset is intentionally not logged from here: those are the enrich-policy exclusions
         * already reported by {@link #logEnrichMatchFieldExclusions} (with the policy name as
         * extra context), so re-emitting them under {@code reason=keyword carries [...]} would
         * double-count. Output is sorted by {@code (dataset, field)} for deterministic grep.
         */
        private static void logMappingDenylistHits(Map<String, List<KeywordToFlattenedTransformer.SkippedField>> skippedByDataset) {
            List<String> datasets = new ArrayList<>(skippedByDataset.keySet());
            datasets.sort(Comparator.naturalOrder());
            for (String dataset : datasets) {
                List<KeywordToFlattenedTransformer.SkippedField> skipped = new ArrayList<>(skippedByDataset.get(dataset));
                skipped.sort(Comparator.comparing(KeywordToFlattenedTransformer.SkippedField::fieldPath));
                for (KeywordToFlattenedTransformer.SkippedField field : skipped) {
                    if (field.reason() != KeywordToFlattenedTransformer.SkipReason.INCOMPATIBLE_PARAMETER) {
                        continue;
                    }
                    logger.info(
                        "keyword→flattened: skip-convert; dataset={}; field={}; reason=keyword carries [{}]",
                        dataset,
                        field.fieldPath(),
                        field.parameter()
                    );
                }
            }
        }

        /**
         * Extracts the expected output column names, in declaration order, from a csv-spec test
         * case's {@link CsvSpecReader.CsvTestCase#expectedResults} body. The first non-empty line
         * of the body is the header line of the expected result table, with columns
         * separated by {@code |} and each column written as {@code name:type}. The {@code :type}
         * suffix is stripped here because only the order and name of columns matter for the
         * trailing {@code KEEP} that the rewriter appends &mdash; the {@code field_extract}
         * tail-end recovery already converts the column type back to {@code keyword}.
         * <p>
         * Returns an empty list when {@code expectedResults} is {@code null}, blank, or otherwise
         * cannot be parsed into a header line of {@code name:type} pairs. In that case the
         * rewriter falls back to a bare tail-end EVAL with no column-order restoration; tests
         * whose expected column order matters will then surface a column-order failure rather
         * than silently changing behavior.
         */
        private static List<String> parseExpectedColumnOrder(String expectedResults) {
            if (expectedResults == null || expectedResults.isBlank()) {
                return List.of();
            }
            String headerLine = firstNonBlankLine(expectedResults);
            if (headerLine == null) {
                return List.of();
            }
            List<String> names = new ArrayList<>();
            for (String part : headerLine.split("\\|")) {
                String trimmed = part.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                int colon = trimmed.indexOf(':');
                String name = colon < 0 ? trimmed : trimmed.substring(0, colon).trim();
                if (name.isEmpty()) {
                    continue;
                }
                names.add(name);
            }
            return names;
        }

        private static String firstNonBlankLine(String text) {
            int start = 0;
            int n = text.length();
            while (start < n) {
                int newline = text.indexOf('\n', start);
                int end = newline < 0 ? n : newline;
                String line = text.substring(start, end);
                if (line.isBlank() == false) {
                    return line;
                }
                if (newline < 0) {
                    return null;
                }
                start = newline + 1;
            }
            return null;
        }

        /**
         * Returns the per-query rewrite scope: every keyword path that
         * {@link KeywordToFlattenedTransformer} converted to {@code flattened} in at least one
         * dataset the query touches via {@code FROM}, {@code TS}, or {@code LOOKUP JOIN},
         * <em>minus</em> every dotted path that exists as a non-keyword leaf in any other dataset
         * the same query touches.
         * <p>
         * The subtraction is what makes a multi-index query like
         * {@code FROM sample_data, sample_data_ts_long, sample_data_ts_nanos} safe to rewrite:
         * {@code client_ip} is keyword-now-flattened in {@code sample_data} but {@code ip} in the
         * two timestamp variants, and the merged schema would otherwise reject any
         * {@code field_extract(client_ip, "v")} because the runtime field is not flattened in two
         * out of three sources. Subtracting the union of non-keyword paths drops {@code client_ip}
         * from the rewrite scope for that query while still wrapping every <em>other</em>
         * keyword field that has no cross-dataset conflict.
         * <p>
         * Note that we intersect on field paths (names), not on declared types per dataset: if the
         * same name is keyword in two datasets and non-keyword in a third, the field is excluded
         * from this query's scope. That is intentionally conservative &mdash; over-scoping would
         * wrap a field on shards where the runtime type is non-flattened and surface as a
         * verifier error masquerading as the rewriter's bug rather than a genuine ES|QL finding.
         * The opposite direction (under-scoping) only loses an opportunity to exercise
         * {@code field_extract} on a particular field for that one query, which is preferable.
         */
        private Set<String> resolveKeywordPathsForQuery(String query) {
            Set<CsvTestsDataLoader.TestDataset> datasets = EsqlQueryDatasetResolver.resolveDatasetsForQuery(
                query,
                CsvTestsDataLoader.CSV_DATASET
            );
            // Track per-dataset classification of every path so that, if the cross-dataset
            // intersection eliminates a field from the rewrite scope, the strategy can also log
            // which datasets contributed the conflict. Without this attribution the user sees an
            // exclusion but cannot tell whether it came from one specific dataset (a clear
            // mapping difference) or several (a scattered keyword/non-keyword mix).
            Set<String> keywordCandidates = new HashSet<>();
            Set<String> nonKeywordExclusions = new HashSet<>();
            Map<String, Set<String>> keywordContributorsByField = new HashMap<>();
            Map<String, Set<String>> nonKeywordContributorsByField = new HashMap<>();
            for (CsvTestsDataLoader.TestDataset d : datasets) {
                Set<String> kw = keywordPathsByDatasetIndexName.getOrDefault(d.indexName(), Set.of());
                Set<String> nonKw = nonKeywordPathsByDatasetIndexName.getOrDefault(d.indexName(), Set.of());
                keywordCandidates.addAll(kw);
                nonKeywordExclusions.addAll(nonKw);
                for (String f : kw) {
                    keywordContributorsByField.computeIfAbsent(f, k -> new TreeSet<>()).add(d.indexName());
                }
                for (String f : nonKw) {
                    nonKeywordContributorsByField.computeIfAbsent(f, k -> new TreeSet<>()).add(d.indexName());
                }
            }
            // The fields actually subtracted by the cross-dataset intersection are exactly the
            // keyword candidates that also appear as non-keyword in some referenced dataset. We
            // log one INFO line per such field with the conflicting datasets so the user can
            // grep for "scope-excluded" and see exactly which queries lost which coverage.
            Set<String> excluded = new TreeSet<>();
            for (String f : keywordCandidates) {
                if (nonKeywordExclusions.contains(f)) {
                    excluded.add(f);
                }
            }
            keywordCandidates.removeAll(nonKeywordExclusions);
            logScopeExclusions(excluded, keywordContributorsByField, nonKeywordContributorsByField);
            return keywordCandidates;
        }

        /**
         * Emits one INFO line per field excluded from the rewrite scope by cross-dataset
         * intersection, naming the datasets that contributed each side of the conflict. The
         * resolver runs once per top-level query and once per recursively rewritten subquery; the
         * resulting volume is bounded by (queries) &times; (subquery depth) &times; (excluded
         * fields per query) and in practice produces a handful of lines per conflicted query.
         */
        private static void logScopeExclusions(
            Set<String> excludedFields,
            Map<String, Set<String>> keywordContributors,
            Map<String, Set<String>> nonKeywordContributors
        ) {
            for (String field : excludedFields) {
                Set<String> kwContributors = keywordContributors.getOrDefault(field, Set.of());
                Set<String> nonKwContributors = nonKeywordContributors.getOrDefault(field, Set.of());
                logger.info(
                    "keyword→flattened: scope-excluded; field={}; "
                        + "reason=keyword in {} but non-keyword (or denylisted/enrich-protected) in {}",
                    field,
                    kwContributors,
                    nonKwContributors
                );
            }
        }
    }
}
