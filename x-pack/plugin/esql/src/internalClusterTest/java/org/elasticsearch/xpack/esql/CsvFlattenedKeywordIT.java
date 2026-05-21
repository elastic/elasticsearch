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
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    public static final String OPT_IN_PROPERTY = "tests.run_keyword_flattened_variant";

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

    @Before
    public void onlyRunWhenOptedIn() {
        assumeTrue(
            "Set -D" + OPT_IN_PROPERTY + "=true to run the keyword→flattened variant of csv-spec tests",
            Boolean.getBoolean(OPT_IN_PROPERTY)
        );
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
            this.protectedKeywordPathsByEnrichSourceIndex = computeProtectedKeywordPathsByEnrichSourceIndex();
            DatasetPathsResult datasetPaths = computeDatasetPaths(protectedKeywordPathsByEnrichSourceIndex);
            this.keywordPathsByDatasetIndexName = datasetPaths.keywordPathsByDatasetIndexName();
            this.nonKeywordPathsByDatasetIndexName = datasetPaths.nonKeywordPathsByDatasetIndexName();
        }

        /**
         * Bundles the two per-dataset path maps that {@link #computeDatasetPaths} produces in a
         * single mapping pass: one entry per declared field is classified as either
         * keyword-being-rewritten or not, so a single walk of the mapping populates both maps
         * deterministically.
         */
        private record DatasetPathsResult(
            Map<String, Set<String>> keywordPathsByDatasetIndexName,
            Map<String, Set<String>> nonKeywordPathsByDatasetIndexName
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
         * </ul>
         * The transformed mapping itself is discarded here; the per-index transformation still
         * runs through {@link #transformMapping(CsvTestsDataLoader.TestDataset, String)} when
         * {@link CsvIT} actually loads each index.
         */
        private static DatasetPathsResult computeDatasetPaths(Map<String, Set<String>> protectedByIndex) {
            Map<String, Set<String>> keywordMap = new HashMap<>();
            Map<String, Set<String>> nonKeywordMap = new HashMap<>();
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
                } catch (IOException e) {
                    throw new UncheckedIOException("failed to read mapping for dataset [" + dataset.indexName() + "]", e);
                }
            }
            return new DatasetPathsResult(Map.copyOf(keywordMap), Map.copyOf(nonKeywordMap));
        }

        /**
         * Builds the protected-path map by reading every entry in
         * {@link CsvTestsDataLoader#ENRICH_POLICIES} and recording each policy's {@code match_field}
         * under its source index name (the {@code index} field of the
         * {@link CsvTestsDataLoader.EnrichConfig}). Policy type ({@code match}, {@code geo_match},
         * {@code range}) is irrelevant here: adding a non-keyword path to the exclusion set is a
         * no-op because {@link KeywordToFlattenedTransformer} only ever rewrites declared
         * {@code keyword} fields, so we accept all policies and let the transformer ignore the
         * paths it would not have touched.
         */
        private static Map<String, Set<String>> computeProtectedKeywordPathsByEnrichSourceIndex() {
            ObjectMapper jsonMapper = new ObjectMapper();
            Map<String, Set<String>> exclusions = new HashMap<>();
            for (CsvTestsDataLoader.EnrichConfig policy : CsvTestsDataLoader.ENRICH_POLICIES.values()) {
                String matchField = readMatchFieldFromPolicy(jsonMapper, policy);
                if (matchField == null) {
                    continue;
                }
                exclusions.computeIfAbsent(policy.index(), k -> new HashSet<>()).add(matchField);
            }
            Map<String, Set<String>> immutable = new HashMap<>();
            for (Map.Entry<String, Set<String>> entry : exclusions.entrySet()) {
                immutable.put(entry.getKey(), Set.copyOf(entry.getValue()));
            }
            return Map.copyOf(immutable);
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
            if (Boolean.getBoolean(LOG_REWRITTEN_QUERIES_PROPERTY)) {
                logger.info("keyword→flattened: rewritten query:\n{}", result.rewrittenQuery());
            }
            return result.rewrittenQuery();
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
            Set<String> keywordCandidates = new HashSet<>();
            Set<String> nonKeywordExclusions = new HashSet<>();
            for (CsvTestsDataLoader.TestDataset d : datasets) {
                keywordCandidates.addAll(keywordPathsByDatasetIndexName.getOrDefault(d.indexName(), Set.of()));
                nonKeywordExclusions.addAll(nonKeywordPathsByDatasetIndexName.getOrDefault(d.indexName(), Set.of()));
            }
            keywordCandidates.removeAll(nonKeywordExclusions);
            return keywordCandidates;
        }
    }
}
