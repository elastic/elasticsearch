/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.junit.AfterClass;
import org.junit.AssumptionViolatedException;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.classpathResources;
import static org.elasticsearch.xpack.esql.KeywordToFlattenedTransformer.FlattenedJunkConfig;

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
 * This class is part of the normal {@code internalClusterTest} suite and runs whenever
 * {@code CsvFlattenedKeywordIT} is invoked (for example
 * {@code ./gradlew :x-pack:plugin:esql:internalClusterTest --tests org.elasticsearch.xpack.esql.CsvFlattenedKeywordIT}).
 * At the end of the run {@link #logKeywordToFlattenedSummary()} emits a single
 * {@code keyword→flattened summary:} line (grep-able from the JUnit XML {@code <system-out>})
 * breaking down how many tests were launched, silenced, or skipped because the query had
 * nothing for the rewriter to wrap. Tests that exercise a known limitation of
 * {@code field_extract()} or of an upstream grammar/engine constraint carry a
 * {@code skip_flattened_rewrite: <reason>} preamble line in the csv-spec entry itself; this
 * variant reads that directive and skips the test with the verbatim reason. To re-enable a
 * silenced test locally a developer deletes the directive line from the csv-spec entry; no
 * Java change is required.
 * <p>
 * Optional logging: to also emit the full rewritten query (one multi-line {@code INFO} message
 * per launched test, prefixed with {@code keyword→flattened: rewritten query:}), pass
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
 *       per keyword field that this variant will never convert to {@code flattened}. Three reasons
 *       are produced: {@code reason=enrich-policy match_field for [policy]} for fields that must
 *       remain {@code keyword} in the source dataset of an enrich policy,
 *       {@code reason=LOOKUP JOIN match field for [target]} for fields used as join keys in any
 *       csv-spec {@code LOOKUP JOIN ... ON ...} clause (kept as keyword on every dataset that
 *       participates in the join, so neither side becomes {@code flattened} and the join's type
 *       check passes), and {@code reason=keyword carries [param]} for fields whose declaration
 *       uses a parameter in {@link KeywordToFlattenedTransformer#PARAMS_INCOMPATIBLE_WITH_FLATTENED}.</li>
 *   <li>{@code scope-excluded; field=&lt;X&gt;; reason=keyword in {...} but non-keyword in {...}} &mdash;
 *       per launched query (and per recursive subquery), once per field whose cross-dataset path
 *       intersection eliminates it from the rewrite scope. The two dataset sets identify which
 *       indices contributed each side of the conflict.</li>
 *   <li>{@code skip-wrap; site=&lt;SITE&gt;; field=&lt;X&gt;; reason=...} &mdash; per launched query,
 *       once per distinct {@code (site, field)} pair the rewriter's
 *       {@link AstKeywordFieldRewriter.SkipSite} machinery declined to wrap. Sites cover
 *       grammar slots that accept only an attribute ({@code MV_EXPAND}, {@code ENRICH ON / WITH},
 *       {@code LOOKUP JOIN ... ON ...}) and the LHS of the match operator {@code :}. These
 *       positions are still exercised at the runtime layer (the bare attribute reaches the
 *       engine), but {@code field_extract} itself is not tested there. Tests whose
 *       <em>only</em> in-scope field references are inside {@code LOOKUP JOIN ... ON ...} are
 *       additionally marked skipped (via {@code AssumptionViolatedException}) so the JUnit XML
 *       {@code <skipped>} element carries a precise reason instead of running as if
 *       {@code field_extract} were exercised.</li>
 * </ul>
 *
 * <p>Known limitations:</p>
 * <ul>
 *   <li>Only top-level and {@code properties}-nested keyword fields are converted; multi-field
 *       sub-fields under {@code fields} are left alone (see {@link KeywordToFlattenedTransformer}).</li>
 *   <li>Datasets whose keyword fields are referenced by special index modes (e.g. time-series
 *       dimensions) may fail at index creation time.</li>
 *   <li>Query rewriting is regex-driven; ES|QL grammar surfaces that take an attribute but not an
 *       arbitrary expression (e.g. {@code DISSECT &lt;field&gt;}) will fail when the field is wrapped
 *       in a function call. Those failures are the intended output of this test &mdash; the
 *       attribute-only positions the rewriter explicitly carves out
 *       ({@code MV_EXPAND}, {@code ENRICH ON / WITH}, {@code LOOKUP JOIN ... ON ...}, match
 *       operator {@code :} LHS) emit {@code skip-wrap} log lines instead so the inventory remains
 *       complete. See {@link AstKeywordFieldRewriter} for the full list.</li>
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
     * Per-JVM counters for the post-run summary emitted by {@link #logKeywordToFlattenedSummary}.
     * Each counter corresponds to one exit path of {@link KeywordToFlattenedStrategy#transformQuery}
     * &mdash; the four paths together account for every test method this strategy sees, so the
     * summary lets a developer read a single log line and know how many tests the rewriter
     * actually ran versus how many it short-circuited (and why).
     * <p>
     * The counters are deliberately not reset between tests in the same JVM: the summary
     * reports totals for the full lifetime of the test class, which is what is interesting
     * after a {@code ./gradlew} invocation. With Gradle's forked test JVMs each fork emits its
     * own summary line; the user can {@code grep "keyword→flattened summary"} the run output
     * to see all of them.
     */
    private static final AtomicInteger LAUNCHED_COUNT = new AtomicInteger();
    private static final AtomicInteger NO_KEYWORD_REFS_COUNT = new AtomicInteger();
    private static final AtomicInteger ONLY_LOOKUP_JOIN_ON_COUNT = new AtomicInteger();
    /**
     * Per-reason silenced counter, keyed by the verbatim value of the
     * {@code skip_flattened_rewrite:} preamble line on the csv-spec test. Built up lazily as
     * skipped tests are seen so the post-run summary only reports reasons that were actually
     * encountered by this JVM.
     */
    private static final ConcurrentMap<String, AtomicInteger> SILENCED_COUNTS_BY_REASON = new ConcurrentHashMap<>();

    /**
     * Installs the keyword&rarr;flattened rewrite strategy, after first skipping the whole variant
     * on release builds.
     * <p>
     * This runs after {@link CsvIT#setupCluster()} (JUnit guarantees the superclass
     * {@code @BeforeClass} runs first) and replaces the identity strategy with one that rewrites
     * keyword fields to {@code flattened}, wraps source values, and wraps every query reference in
     * {@code field_extract(<field>, "<sub-key>")}.
     * <p>
     * Both the {@code flattened} datatype ({@link DataType#FLATTENED}) and the {@code field_extract}
     * function are under construction and therefore active only in snapshot builds. In a release
     * build {@code DataType.fromEs("flattened")} resolves to {@code UNSUPPORTED} &mdash; so every
     * converted field would fail analysis with {@code Cannot use field [...] with unsupported type
     * [flattened]} &mdash; and {@code field_extract} is not registered at all, leaving nothing for
     * this variant to exercise. The {@code assumeTrue} gate therefore skips the class as a unit.
     * It deliberately tests the same {@code supportedLocally()} predicate the analyzer itself uses
     * to reject the type, so the gate cannot drift from the condition it guards against.
     */
    @BeforeClass
    public static void installKeywordToFlattenedStrategy() {
        assumeTrue(
            "keyword→flattened variant requires the flattened datatype and field_extract(), "
                + "which are under construction (snapshot-only) and unavailable in release builds",
            DataType.FLATTENED.supportedVersion().supportedLocally()
        );
        indexLoadStrategy = new KeywordToFlattenedStrategy();
    }

    @AfterClass
    public static void logKeywordToFlattenedSummary() {
        int silenced = 0;
        for (AtomicInteger c : SILENCED_COUNTS_BY_REASON.values()) {
            silenced += c.get();
        }
        int launched = LAUNCHED_COUNT.get();
        int noRefs = NO_KEYWORD_REFS_COUNT.get();
        int onlyLookup = ONLY_LOOKUP_JOIN_ON_COUNT.get();
        int touched = launched + silenced + noRefs + onlyLookup;
        logger.info(
            "keyword→flattened summary: touched={} launched={} silenced={} no-keyword-refs={} only-lookup-join-on={}",
            touched,
            launched,
            silenced,
            noRefs,
            onlyLookup
        );
        SILENCED_COUNTS_BY_REASON.entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(e -> logger.info("keyword→flattened summary: silenced[{}]={}", e.getKey(), e.getValue().get()));
    }

    /**
     * Strategy implementation that delegates JSON manipulation to {@link KeywordToFlattenedTransformer}
     * and query rewriting to {@link AstKeywordFieldRewriter}.
     * <p>
     * Three pieces of state are kept, all populated eagerly at construction time:
     * <ul>
     *   <li>{@code protectedKeywordPathsByDatasetIndexName} &mdash; for every dataset, the union
     *       of paths that must remain {@code keyword} regardless of how this variant operates.
     *       The union has two contributors:
     *       <ol>
     *         <li>For every enrich policy declared in
     *             {@link CsvTestsDataLoader#ENRICH_POLICIES}, the policy's {@code match_field}
     *             under the policy's source index. The enrich-policy reindex into the internal
     *             {@code .enrich-*} index expects a {@code keyword} value at the match field;
     *             converting the source field to {@code flattened} causes that reindex to fail
     *             and the failure is cached by {@link CsvIT}'s resource loader, so every later
     *             test in the same JVM fails with the same
     *             {@code RuntimeException: Resource loading failure}. Excluding the match field
     *             at the source keeps the policy load deterministic.</li>
     *         <li>For every {@code LOOKUP JOIN ... ON &lt;field&gt;} clause in any csv-spec test
     *             query, the join attribute is added to the <em>lookup target index only</em>.
     *             The target must remain {@code keyword} because the engine refuses
     *             {@code JOIN with right field [...] of type [FLATTENED] is not supported}.
     *             {@code FROM}-source datasets are no longer excluded here: the cross-dataset
     *             intersection in {@link #resolveKeywordPathsForQuery} already removes the join
     *             key from the rewrite scope for any query that references the lookup target,
     *             so converting the source side to {@code flattened} is safe.</li>
     *       </ol>
     *   </li>
     *   <li>{@code keywordPathsByDatasetIndexName} &mdash; the keyword paths that
     *       {@link KeywordToFlattenedTransformer} actually rewrites for each dataset (after the
     *       protected paths above are applied). Used both to wrap per-document source values when
     *       {@link CsvIT} indexes the dataset, and as one input to the per-query scope computed
     *       by {@link #resolveKeywordPathsForQuery}.</li>
     *   <li>{@code nonKeywordPathsByDatasetIndexName} &mdash; every dotted field path declared in
     *       a dataset's mapping whose type is <em>not</em> {@code keyword} (e.g. {@code long},
     *       {@code ip}, {@code text}, {@code geo_point}, plus keyword-typed paths that the
     *       transformer's denylist or the protected-paths set kept as keyword). Used to subtract
     *       cross-dataset conflicts from the per-query keyword scope &mdash; if a field exists as
     *       keyword in one dataset and as a non-keyword type in another dataset referenced by
     *       the same query, wrapping the field in {@code field_extract(...)} would fail
     *       verification on the second dataset, so the field is excluded from the rewrite scope
     *       for that query.</li>
     * </ul>
     */
    private static final class KeywordToFlattenedStrategy implements IndexLoadStrategy {
        /**
         * Per-dataset union of every path that must remain {@code keyword} regardless of why.
         * Both the per-index startup mapping pass ({@link #computeDatasetPaths}) and the runtime
         * mapping rewrite ({@link #transformMapping(CsvTestsDataLoader.TestDataset, String)})
         * read from this single map so the two paths cannot drift apart and produce a dataset
         * whose declared mapping disagrees with the variant's view of which paths are keyword.
         * The map carries the union of:
         * <ul>
         *   <li>enrich-policy {@code match_field}s (per
         *       {@link #computeEnrichMatchFieldExclusions}) &mdash; keeps the policy's source
         *       field as keyword so the policy reindex into {@code .enrich-*} succeeds, and</li>
         *   <li>{@code LOOKUP JOIN ... ON ...} attributes (per
         *       {@link #computeLookupJoinFieldExclusions}) &mdash; keeps the join key as keyword
         *       on the <em>lookup target index only</em>, preventing the
         *       {@code JOIN with right field of type [FLATTENED] is not supported} error.
         *       {@code FROM}-source datasets are excluded from this set; the cross-dataset
         *       intersection in {@link #resolveKeywordPathsForQuery} already removes the join
         *       key from the rewrite scope for those queries, so no explicit exclusion is
         *       needed on the source side.</li>
         * </ul>
         */
        private final Map<String, Set<String>> protectedKeywordPathsByDatasetIndexName;
        private final Map<String, Set<String>> keywordPathsByDatasetIndexName;
        private final Map<String, Set<String>> nonKeywordPathsByDatasetIndexName;
        private final Map<String, FlattenedJunkConfig> junkConfigByDatasetIndexName;

        KeywordToFlattenedStrategy() {
            EnrichExclusionResult enrichResult = computeEnrichMatchFieldExclusions();
            LookupJoinExclusionResult lookupResult = computeLookupJoinFieldExclusions();
            this.protectedKeywordPathsByDatasetIndexName = unionExclusionsByIndex(enrichResult.byIndex(), lookupResult.byIndex());

            DatasetPathsResult datasetPaths = computeDatasetPaths(protectedKeywordPathsByDatasetIndexName);
            this.keywordPathsByDatasetIndexName = datasetPaths.keywordPathsByDatasetIndexName();
            this.nonKeywordPathsByDatasetIndexName = datasetPaths.nonKeywordPathsByDatasetIndexName();

            // Compute per-dataset junk configuration: flip one coin per dataset.
            // selectJunkFields already returns EMPTY for an empty input set, and
            // datasets without a mapping file have no converted paths so they also get EMPTY.
            Map<String, FlattenedJunkConfig> junkMap = new HashMap<>();
            for (CsvTestsDataLoader.TestDataset dataset : CsvTestsDataLoader.CSV_DATASET.values()) {
                Set<String> kw = this.keywordPathsByDatasetIndexName.getOrDefault(dataset.indexName(), Set.of());
                junkMap.put(dataset.indexName(), FlattenedJunkConfig.selectJunkFields(kw));
            }
            this.junkConfigByDatasetIndexName = Map.copyOf(junkMap);

            // Emit one INFO line for every keyword field that this variant will
            // intentionally never convert. The user can grep for "skip-convert" to inventory the
            // permanent blind spots of the test variant. Three sources are emitted:
            // * enrich-policy match_fields: the policy reindex into .enrich-* requires keyword,
            // and converting the source field would poison the test resource loader.
            // * LOOKUP JOIN ON attributes: must remain keyword on every participating dataset to
            // keep both sides of the join type-aligned and to dodge the engine's lack of support
            // for JOIN on flattened.
            // * mapping-denylist hits: keyword fields declaring TSDB / multi-fields / copy_to /
            // runtime-script parameters that are incompatible with flattened.
            logEnrichMatchFieldExclusions(enrichResult.exclusions());
            logLookupJoinFieldExclusions(lookupResult.exclusions());
            logMappingDenylistHits(datasetPaths.skippedFieldsByDataset());
            logJunkConfig(this.junkConfigByDatasetIndexName);
        }

        /**
         * Returns a per-index map whose value at each key is the union of the values that key
         * has in {@code first} and {@code second}. Both inputs may omit keys; missing keys are
         * treated as the empty set. The returned map and its value sets are immutable.
         */
        private static Map<String, Set<String>> unionExclusionsByIndex(Map<String, Set<String>> first, Map<String, Set<String>> second) {
            Map<String, Set<String>> merged = new HashMap<>();
            for (Map.Entry<String, Set<String>> e : first.entrySet()) {
                merged.computeIfAbsent(e.getKey(), k -> new HashSet<>()).addAll(e.getValue());
            }
            for (Map.Entry<String, Set<String>> e : second.entrySet()) {
                merged.computeIfAbsent(e.getKey(), k -> new HashSet<>()).addAll(e.getValue());
            }
            Map<String, Set<String>> immutable = new HashMap<>();
            for (Map.Entry<String, Set<String>> e : merged.entrySet()) {
                immutable.put(e.getKey(), Set.copyOf(e.getValue()));
            }
            return Map.copyOf(immutable);
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
         * One {@code LOOKUP JOIN} attribute that contributes to the per-participating-index
         * exclusion set, with enough metadata for the startup logger to attribute each exclusion
         * to a representative {@code (target, field)} occurrence in the csv-spec corpus. The same
         * {@code (sourceIndex, field)} pair is emitted only once even if many queries mention it,
         * so the logger does not double-count multi-use fields.
         *
         * @param sourceIndex the lookup-target dataset whose mapping will keep {@code field} as
         *                    {@code keyword}; {@code FROM}-source datasets are no longer included
         *                    here because the cross-dataset intersection in
         *                    {@link #resolveKeywordPathsForQuery} already prevents the join key
         *                    from entering the rewrite scope for queries that reference this target
         * @param field       the dotted field path that must remain {@code keyword} in
         *                    {@code sourceIndex}
         * @param target      the lookup-target index where {@code field} is the join key &mdash;
         *                    used only in the log line so the user can see which join introduced
         *                    the exclusion. May equal {@code sourceIndex} when the source-side of
         *                    the exclusion is the lookup target itself.
         */
        private record LookupJoinFieldExclusion(String sourceIndex, String field, String target) {}

        /**
         * Bundles the by-index protected-paths map (used at runtime by the transformer) with the
         * per-{@code (sourceIndex, field)} exclusion list (used at startup by the logger). Both
         * views are populated by a single scan of every csv-spec query so the test variant cannot
         * drift between "what we tell the transformer to keep as keyword" and "what we report to
         * the user as kept".
         */
        private record LookupJoinExclusionResult(Map<String, Set<String>> byIndex, List<LookupJoinFieldExclusion> exclusions) {}

        /**
         * Walks every csv-spec test query in the corpus, extracts each
         * {@code LOOKUP JOIN &lt;target&gt; ON &lt;body&gt;} clause via
         * {@link EsqlQueryDatasetResolver#extractLookupJoinTargets}, and returns both
         * <ol>
         *   <li>a by-source-index map that adds each join attribute to the <em>lookup target
         *       index only</em>. The target must stay {@code keyword} because the engine refuses
         *       {@code JOIN with right field [...] of type [FLATTENED] is not supported}.
         *       {@code FROM}-source datasets are excluded: the cross-dataset intersection in
         *       {@link #resolveKeywordPathsForQuery} already removes the join key from the
         *       rewrite scope for any query that references the lookup target.</li>
         *   <li>a per-{@code (sourceIndex, field, target)} list deduplicated to one entry per
         *       triple, used by the startup logger to surface every exclusion the variant
         *       installed.</li>
         * </ol>
         * Resolution of the lookup target uses the same dataset registry as
         * {@link EsqlQueryDatasetResolver#resolveDatasetsForQuery}, so an unknown target index
         * (e.g. a typo or a future-only test) is silently dropped &mdash; the exclusion set
         * cannot reference a dataset that the variant will not load anyway.
         */
        private static LookupJoinExclusionResult computeLookupJoinFieldExclusions() {
            Map<String, Set<String>> exclusionsByIndex = new HashMap<>();
            Map<String, LookupJoinFieldExclusion> uniqueExclusions = new LinkedHashMap<>();
            for (CsvSpecReader.CsvTestCase testCase : loadAllCsvSpecTestCases()) {
                String query = testCase.query;
                if (query == null || query.isEmpty()) {
                    continue;
                }
                Map<String, Set<String>> targets = EsqlQueryDatasetResolver.extractLookupJoinTargets(query);
                if (targets.isEmpty()) {
                    continue;
                }
                for (Map.Entry<String, Set<String>> targetEntry : targets.entrySet()) {
                    String targetIndex = targetEntry.getKey();
                    Set<String> fields = targetEntry.getValue();
                    CsvTestsDataLoader.TestDataset targetDataset = CsvTestsDataLoader.CSV_DATASET.get(targetIndex);
                    if (targetDataset == null) {
                        continue;
                    }
                    for (String field : fields) {
                        exclusionsByIndex.computeIfAbsent(targetDataset.indexName(), k -> new HashSet<>()).add(field);
                        uniqueExclusions.putIfAbsent(
                            targetDataset.indexName() + "\0" + field + "\0" + targetIndex,
                            new LookupJoinFieldExclusion(targetDataset.indexName(), field, targetIndex)
                        );
                    }
                }
            }
            Map<String, Set<String>> immutable = new HashMap<>();
            for (Map.Entry<String, Set<String>> entry : exclusionsByIndex.entrySet()) {
                immutable.put(entry.getKey(), Set.copyOf(entry.getValue()));
            }
            return new LookupJoinExclusionResult(Map.copyOf(immutable), List.copyOf(uniqueExclusions.values()));
        }

        /**
         * Loads every csv-spec test case on the test classpath via the same machinery
         * {@link CsvIT#readScriptSpec} uses for the parameterized factory. Used by
         * {@link #computeLookupJoinFieldExclusions} to scan the corpus once at startup;
         * propagating the unchecked exceptions out of here is acceptable because the strategy
         * cannot operate without knowing which fields participate in any {@code LOOKUP JOIN}.
         */
        static List<CsvSpecReader.CsvTestCase> loadAllCsvSpecTestCases() {
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
            } catch (IOException e) {
                throw new UncheckedIOException("failed to enumerate csv-spec resources for LOOKUP JOIN exclusions", e);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException("failed to load csv-spec test cases for LOOKUP JOIN exclusions", e);
            }
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
            Set<String> excluded = protectedKeywordPathsByDatasetIndexName.getOrDefault(dataset.indexName(), Set.of());
            return KeywordToFlattenedTransformer.transformMapping(originalMapping, excluded).transformedMapping();
        }

        @Override
        public Settings transformSettings(CsvTestsDataLoader.TestDataset dataset, Settings settings) {
            // Update dimensions in the routing path to their transformed path
            List<String> routingPaths = settings.getAsList("index.routing_path");
            if (routingPaths.isEmpty()) {
                return settings;
            }
            Set<String> convertedPaths = keywordPathsByDatasetIndexName.getOrDefault(dataset.indexName(), Set.of());
            if (convertedPaths.isEmpty()) {
                return settings;
            }
            boolean changed = false;
            List<String> rewritten = new ArrayList<>(routingPaths.size());
            for (String path : routingPaths) {
                if (convertedPaths.contains(path)) {
                    rewritten.add(path + "." + KeywordToFlattenedTransformer.WRAPPER_SUBKEY);
                    changed = true;
                } else {
                    rewritten.add(path);
                }
            }
            if (changed == false) {
                return settings;
            }
            return Settings.builder().put(settings).putList("index.routing_path", rewritten).build();
        }

        @Override
        public String transformDocument(CsvTestsDataLoader.TestDataset dataset, String originalDocumentJson) throws IOException {
            Set<String> paths = keywordPathsByDatasetIndexName.getOrDefault(dataset.indexName(), Set.of());
            FlattenedJunkConfig junk = junkConfigByDatasetIndexName.get(dataset.indexName());
            assert junk != null : "no junk config for dataset: " + dataset.indexName();
            return KeywordToFlattenedTransformer.wrapKeywordValuesAsFlattened(originalDocumentJson, paths, junk);
        }

        @Override
        public IndexLoadStrategy.TransformedQuery transformQuery(String testId, CsvSpecReader.CsvTestCase testCase) {
            // Tests requiring ts_info_command or metrics_info_command expose TSDB dimension names
            // directly in query output (e.g. _timeseries, _tsid). After the keyword→flattened
            // rewrite those names change from "cluster" to "cluster.v", so the expected results
            // no longer match. Skip the whole variant for these tests rather than updating every
            // expected result to the sub-key form.
            for (String cap : testCase.requiredCapabilities) {
                if (cap.equals("ts_info_command") || cap.equals("metrics_info_command")) {
                    throw new StacklessAssumptionViolatedException(
                        "skipping: test requires " + cap + " which exposes raw TSDB dimension names in output"
                    );
                }
            }
            // A {@code skip_flattened_rewrite:} preamble line marks a test whose failure under
            // this variant is a known limitation of field_extract() or of an upstream
            // grammar/engine constraint. Running the rewrite would only reproduce the
            // documented failure and obscure new regressions in the rest of the suite; the
            // assumption-violation message carries the verbatim reason from the csv-spec so
            // the JUnit XML <skipped> element explains why the test was skipped. To re-enable
            // a silenced test locally a developer deletes the directive line from the
            // csv-spec entry; nothing else needs to be touched.
            String skipReason = testCase.skipFlattenedRewrite;
            if (skipReason != null && skipReason.isBlank() == false) {
                SILENCED_COUNTS_BY_REASON.computeIfAbsent(skipReason, k -> new AtomicInteger()).incrementAndGet();
                logger.info("keyword→flattened: skipping; silenced [{}]: {}", testId, skipReason);
                throw new StacklessAssumptionViolatedException(
                    String.format(Locale.ROOT, "silenced known field_extract() limitation [%s]: %s", testId, skipReason)
                );
            }
            String originalQuery = testCase.query;
            List<String> expectedColumnOrder = parseExpectedColumnOrder(testCase.expectedResults);
            // Pass the per-query resolver itself rather than a pre-resolved flat scope so that the
            // rewriter can re-resolve scope for each parenthesised sub-pipeline it recurses into.
            // The outer scope is the union of every dataset the top-level FROM/TS/LOOKUP JOIN
            // touches (minus cross-dataset non-keyword conflicts), but a subquery's FROM typically
            // references only a subset of those datasets and therefore must not see fields the
            // subquery's schema does not produce.
            AstKeywordFieldRewriter.RewriteResult result = AstKeywordFieldRewriter.rewrite(
                originalQuery,
                this::resolveKeywordPathsForQuery,
                KeywordToFlattenedTransformer.WRAPPER_SUBKEY,
                expectedColumnOrder
            );
            if (result.modified() == false) {
                NO_KEYWORD_REFS_COUNT.incrementAndGet();
                // Logged at INFO so the launched/skipped split is visible in the test JVM stdout, alongside the
                // assumption-violation message that surfaces in the JUnit XML <skipped> element.
                logger.info("keyword→flattened: skipping; no keyword field references in query");
                // TODO this should probably not skip the test - just not build a test at all
                throw new StacklessAssumptionViolatedException("skipping: no keyword fields");
            }
            // Even when the query was modified (typically by tail-end EVAL/KEEP recovery
            // alone), if the only sites the rewriter actually visited were LOOKUP JOIN ... ON
            // bodies then no field_extract call survives in the launched query. ES|QL's analyzer
            // rejects an expression on either side of a join condition, so wrapping the join key
            // is not an option; the test would re-run unchanged behavior and therefore adds no
            // new coverage of field_extract. We mark such tests as skipped via
            // AssumptionViolatedException so the JUnit XML <skipped> element carries the precise
            // reason, while still emitting the per-skip-event log lines below so the inventory
            // remains complete.
            if (result.rewrittenFieldNames().isEmpty() && hasOnlyLookupJoinOnSkips(result.skipEvents())) {
                ONLY_LOOKUP_JOIN_ON_COUNT.incrementAndGet();
                logger.info("keyword→flattened: skipping; only field references in query are inside LOOKUP JOIN ... ON ...");
                logRewriterSkipEvents(result.skipEvents());
                throw new StacklessAssumptionViolatedException(
                    "skipping: query's only keyword field references appear inside LOOKUP JOIN ... ON ..., "
                        + "where ES|QL accepts only a bare attribute and field_extract cannot be substituted; "
                        + "the unmodified spec already covers this behavior"
                );
            }
            // LOOKUP JOIN phase 1 - source-side type mismatch.
            // If a LOOKUP JOIN field is keyword-protected in the target (KEYWORD) but
            // converted to flattened in a FROM-source dataset, executing the join would
            // raise a type-incompatibility error. Detect and skip now so the test does
            // not launch a query that will fail for a structural reason unrelated to
            // field_extract coverage.
            Map<String, Set<String>> lookupJoinTargets = EsqlQueryDatasetResolver.extractLookupJoinTargets(originalQuery);
            Set<CsvTestsDataLoader.TestDataset> allQueryDatasets = EsqlQueryDatasetResolver.resolveDatasetsForQuery(
                originalQuery,
                CsvTestsDataLoader.CSV_DATASET
            );
            if (lookupJoinTargets.isEmpty() == false) {
                for (Map.Entry<String, Set<String>> targetEntry : lookupJoinTargets.entrySet()) {
                    String targetIndex = targetEntry.getKey();
                    Set<String> joinFields = targetEntry.getValue();
                    Set<String> protectedInTarget = protectedKeywordPathsByDatasetIndexName.getOrDefault(targetIndex, Set.of());
                    for (String field : joinFields) {
                        if (protectedInTarget.contains(field) == false) {
                            continue;
                        }
                        for (CsvTestsDataLoader.TestDataset ds : allQueryDatasets) {
                            if (lookupJoinTargets.containsKey(ds.indexName())) {
                                continue;
                            }
                            if (keywordPathsByDatasetIndexName.getOrDefault(ds.indexName(), Set.of()).contains(field)) {
                                SILENCED_COUNTS_BY_REASON.computeIfAbsent("lookup_join_source_flattened", k -> new AtomicInteger())
                                    .incrementAndGet();
                                logger.info(
                                    "keyword→flattened: skipping; JOIN field [{}] FLATTENED in source [{}] but KEYWORD in target [{}]",
                                    field,
                                    ds.indexName(),
                                    targetIndex
                                );
                                throw new StacklessAssumptionViolatedException(
                                    "skipping: LOOKUP JOIN field ["
                                        + field
                                        + "] is FLATTENED in FROM-source ["
                                        + ds.indexName()
                                        + "] but KEYWORD in target ["
                                        + targetIndex
                                        + "]"
                                );
                            }
                        }
                    }
                }
            }
            // Lookup join phase 2 - cross-dataset type conflict.
            // If a field is converted to flattened in one FROM-source dataset but is a
            // different, non-keyword type in another FROM-source dataset, executing the
            // query either raises a VerificationException or produces wrong results because
            // scalar functions on an unwrapped flattened field return null. Detect and skip
            // now to avoid spurious failures unrelated to field_extract coverage.
            {
                List<CsvTestsDataLoader.TestDataset> sourceDatasets = new ArrayList<>();
                for (CsvTestsDataLoader.TestDataset d : allQueryDatasets) {
                    if (lookupJoinTargets.containsKey(d.indexName()) == false) {
                        sourceDatasets.add(d);
                    }
                }
                for (int i = 0; i < sourceDatasets.size(); i++) {
                    CsvTestsDataLoader.TestDataset d1 = sourceDatasets.get(i);
                    Set<String> flattenedInD1 = keywordPathsByDatasetIndexName.getOrDefault(d1.indexName(), Set.of());
                    if (flattenedInD1.isEmpty()) {
                        continue;
                    }
                    for (int j = 0; j < sourceDatasets.size(); j++) {
                        if (i == j) {
                            continue;
                        }
                        CsvTestsDataLoader.TestDataset d2 = sourceDatasets.get(j);
                        Set<String> nonKeywordInD2 = nonKeywordPathsByDatasetIndexName.getOrDefault(d2.indexName(), Set.of());
                        for (String field : flattenedInD1) {
                            if (nonKeywordInD2.contains(field)) {
                                SILENCED_COUNTS_BY_REASON.computeIfAbsent("cross_index_type_conflict", k -> new AtomicInteger())
                                    .incrementAndGet();
                                logger.info(
                                    "keyword→flattened: skipping; field [{}] FLATTENED in [{}] but non-keyword type in [{}]",
                                    field,
                                    d1.indexName(),
                                    d2.indexName()
                                );
                                throw new StacklessAssumptionViolatedException(
                                    "skipping: field ["
                                        + field
                                        + "] is FLATTENED in ["
                                        + d1.indexName()
                                        + "] but a different non-keyword type in ["
                                        + d2.indexName()
                                        + "]"
                                );
                            }
                        }
                    }
                }
            }
            // The short "launched" marker is emitted unconditionally so that every launched test has a single,
            // grep-able log line tying the test method (from the JUnit thread context) to the set of fields
            // the rewriter actually wrapped. The multi-line rewritten query itself is gated on
            // LOG_REWRITTEN_QUERIES_PROPERTY because, at full-corpus scale, dumping every rewritten query at
            // INFO produces thousands of multi-line blocks &mdash; useful when analyzing a specific run, but
            // noisy enough to bury the rest of the test output when always on.
            LAUNCHED_COUNT.incrementAndGet();
            logger.info("keyword→flattened: launched; rewrote field references {}", result.rewrittenFieldNames());
            // B-class transparency: emit one INFO line per intentionally-skipped wrap site so the
            // user can grep for "skip-wrap" and inventory exactly which positions in this query
            // the rewriter declined to wrap, and why. Aggregated by (site, field) so a body that
            // mentions the same field twice surfaces a single line.
            logRewriterSkipEvents(result.skipEvents());
            if (Booleans.parseBoolean(System.getProperty(LOG_REWRITTEN_QUERIES_PROPERTY, "false"))) {
                logger.info("keyword→flattened: rewritten query:\n{}", result.rewrittenQuery());
            }

            Settings extraPragmas = Settings.EMPTY;
            return new IndexLoadStrategy.TransformedQuery(result.rewrittenQuery(), extraPragmas);
        }

        /**
         * Rewrites the expected {@code _timeseries} column values so they reflect the
         * dimension-key rename that TSDB applies after the keyword&rarr;flattened conversion.
         */
        @Override
        public CsvTestUtils.ExpectedResults transformExpectedResults(
            String testId,
            CsvSpecReader.CsvTestCase testCase,
            CsvTestUtils.ExpectedResults expected
        ) {
            int tsIdx = expected.columnNames().indexOf("_timeseries");
            if (tsIdx < 0) {
                return expected;
            }
            Set<String> convertedPaths = resolveKeywordPathsForQuery(testCase.query);
            if (convertedPaths.isEmpty()) {
                return expected;
            }
            boolean anyChanged = false;
            List<List<Object>> newValues = new ArrayList<>(expected.values().size());
            for (List<Object> row : expected.values()) {
                Object tsValue = row.get(tsIdx);
                if (tsValue == null) {
                    newValues.add(row);
                    continue;
                }
                String tsJson = tsValue.toString();
                String rewritten = rewriteTimeseriesJson(tsJson, convertedPaths);
                if (rewritten.equals(tsJson)) {
                    newValues.add(row);
                } else {
                    List<Object> newRow = new ArrayList<>(row);
                    newRow.set(tsIdx, rewritten);
                    newValues.add(newRow);
                    anyChanged = true;
                }
            }
            if (anyChanged == false) {
                return expected;
            }
            return new CsvTestUtils.ExpectedResults(expected.columnNames(), expected.columnTypes(), newValues);
        }

        @Override
        public void afterIndexLoaded(CsvTestsDataLoader.TestDataset dataset, Client client) {
            // Workaround for https://github.com/elastic/elasticsearch/issues/151369
            // See BucketColumnMetadataIT#waitForAllTasks for additional context
            assertNoTimeout(client.admin().cluster().prepareHealth(CsvIT.TEST_REQUEST_TIMEOUT).setWaitForEvents(Priority.LANGUID).get());
        }

        /**
         * Parses {@code json} as a JSON object, and for every key that appears in
         * {@code convertedPaths} wraps the value {@code V} as
         * {@code {"v": V}} (where {@code "v"} is
         * {@link KeywordToFlattenedTransformer#WRAPPER_SUBKEY}), then re-serialises the result.
         * This reflects the TSDB dimension representation change: a keyword dimension
         * {@code host:"host-a"} becomes a flattened dimension
         * {@code host:{"v":"host-a"}} after the keyword&rarr;flattened conversion.
         * Returns the original {@code json} string unchanged if parsing fails or no key is found
         * in {@code convertedPaths}.
         */
        private static String rewriteTimeseriesJson(String json, Set<String> convertedPaths) {
            try (
                XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json);
                XContentBuilder builder = XContentFactory.jsonBuilder()
            ) {
                if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                    return json;
                }
                boolean changed = false;
                builder.startObject();
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    String fieldName = parser.currentName();
                    parser.nextToken(); // advance to value token
                    if (convertedPaths.contains(fieldName)) {
                        builder.startObject(fieldName);
                        builder.field(KeywordToFlattenedTransformer.WRAPPER_SUBKEY);
                        builder.copyCurrentStructure(parser);
                        builder.endObject();
                        changed = true;
                    } else {
                        builder.field(fieldName);
                        builder.copyCurrentStructure(parser);
                    }
                }
                builder.endObject();
                if (changed == false) {
                    return json;
                }
                return Strings.toString(builder);
            } catch (IOException e) {
                return json;
            }
        }

        /**
         * Returns {@code true} when {@code events} is non-empty and every recorded site is
         * {@link AstKeywordFieldRewriter.SkipSite#LOOKUP_JOIN_ON}. Used by
         * {@link #transformQuery} to recognise queries whose only field references appear inside
         * a {@code LOOKUP JOIN ... ON ...} body (where {@code field_extract} cannot be
         * substituted) so the test can be marked skipped with a precise reason rather than run
         * as if it exercised {@code field_extract} when it does not.
         */
        private static boolean hasOnlyLookupJoinOnSkips(List<AstKeywordFieldRewriter.SkipEvent> events) {
            if (events.isEmpty()) {
                return false;
            }
            for (AstKeywordFieldRewriter.SkipEvent event : events) {
                if (event.site() != AstKeywordFieldRewriter.SkipSite.LOOKUP_JOIN_ON) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Emits one INFO line per distinct {@code (site, field)} pair in {@code events}, sorted by
         * {@code site} then {@code field} so the output is deterministic across runs (the
         * rewriter's iteration order is otherwise dependent on the in-scope set's iteration
         * order). Each line carries the site, the field name, and a reason string short enough
         * that a {@code grep "site=MV_EXPAND_ARG"} or
         * {@code grep "site=MATCH_OPERATOR_LHS"} produces a usable inventory.
         */
        private static void logRewriterSkipEvents(List<AstKeywordFieldRewriter.SkipEvent> events) {
            if (events.isEmpty()) {
                return;
            }
            Map<AstKeywordFieldRewriter.SkipSite, Set<String>> bySite = new TreeMap<>();
            for (AstKeywordFieldRewriter.SkipEvent event : events) {
                bySite.computeIfAbsent(event.site(), k -> new TreeSet<>()).add(event.field());
            }
            for (Map.Entry<AstKeywordFieldRewriter.SkipSite, Set<String>> entry : bySite.entrySet()) {
                String reason = skipSiteReason(entry.getKey());
                for (String field : entry.getValue()) {
                    logger.info("keyword→flattened: skip-wrap; site={}; field={}; reason={}", entry.getKey(), field, reason);
                }
            }
        }

        /**
         * Human-readable reason string for each {@link AstKeywordFieldRewriter.SkipSite}.
         * Kept as a centralised mapping (rather than as a field on {@code SkipEvent}) so the
         * reason text can evolve with the test variant's documentation without changing the
         * rewriter's API surface.
         */
        private static String skipSiteReason(AstKeywordFieldRewriter.SkipSite site) {
            return switch (site) {
                case MV_EXPAND_ARG -> "MV_EXPAND grammar slot accepts only an attribute, not an expression";
                case ENRICH_BODY -> "ENRICH ON / WITH grammar slots accept only attributes, not expressions";
                case MATCH_OPERATOR_LHS -> "match operator [:] LHS accepts only an attribute, not an expression";
                case LOOKUP_JOIN_ON -> "LOOKUP JOIN ... ON ... accepts only an attribute, not an expression";
                case QUALIFIED_NAME_BRACKETS -> "[<index>].[<field>] qualified-reference brackets accept only an identifier";
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
         * Emits one INFO line per {@code (dataset, field, lookup-target)} triple the variant
         * keeps as {@code keyword} so a csv-spec {@code LOOKUP JOIN ... ON &lt;field&gt;} clause
         * can still type-match. The user can grep for {@code skip-convert} to inventory every
         * field the test variant will never exercise with {@code field_extract}, and grep for
         * {@code reason=LOOKUP JOIN} to narrow down to this subset.
         */
        private static void logLookupJoinFieldExclusions(List<LookupJoinFieldExclusion> exclusions) {
            List<LookupJoinFieldExclusion> sorted = new ArrayList<>(exclusions);
            sorted.sort(
                Comparator.comparing(LookupJoinFieldExclusion::sourceIndex)
                    .thenComparing(LookupJoinFieldExclusion::field)
                    .thenComparing(LookupJoinFieldExclusion::target)
            );
            for (LookupJoinFieldExclusion exclusion : sorted) {
                logger.info(
                    "keyword→flattened: skip-convert; dataset={}; field={}; reason=LOOKUP JOIN match field for [{}]",
                    exclusion.sourceIndex(),
                    exclusion.field(),
                    exclusion.target()
                );
            }
        }

        /**
         * Emits one INFO line per dataset listing which fields (if any) will have junk keys
         * injected into their wrapped flattened objects. An empty junk-fields list means the
         * coin came up tails for that dataset and no junk is injected.
         */
        private static void logJunkConfig(Map<String, FlattenedJunkConfig> junkConfigByDatasetIndexName) {
            List<String> datasets = new ArrayList<>(junkConfigByDatasetIndexName.keySet());
            datasets.sort(Comparator.naturalOrder());
            for (String dataset : datasets) {
                FlattenedJunkConfig cfg = junkConfigByDatasetIndexName.get(dataset);
                List<String> fields = new ArrayList<>(cfg.junkFields());
                fields.sort(Comparator.naturalOrder());
                logger.info("keyword\u2192flattened: junk-config; dataset={}; junk-fields={}", dataset, fields);
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
         * The csv-spec header occasionally writes a column name in double quotes when the name
         * itself is not a bare identifier &mdash; for instance {@code "COUNT(*)":long} for an
         * un-aliased aggregate, or to disambiguate a name that contains a colon. The surrounding
         * quotes are part of the header syntax and not part of the column name in the actual
         * pipeline output, so they are stripped here. The rewriter then re-introduces backticks
         * (the ES|QL parser-level quoting form) when emitting the trailing {@code KEEP}; see
         * {@code quoteIdentifierIfNeeded} on {@link AstKeywordFieldRewriter}.
         * <p>
         * Returns an empty list when {@code expectedResults} is {@code null}, blank, or otherwise
         * cannot be parsed into a header line of {@code name:type} pairs. In that case the
         * rewriter falls back to a bare tail-end EVAL with no column-order restoration; tests
         * whose expected column order matters will then surface a column-order failure rather
         * than silently changing behavior.
         */
        static List<String> parseExpectedColumnOrder(String expectedResults) {
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
                if (name.length() >= 2 && name.charAt(0) == '"' && name.charAt(name.length() - 1) == '"') {
                    name = name.substring(1, name.length() - 1);
                }
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
        Set<String> resolveKeywordPathsForQuery(String query) {
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

        /**
         * An {@link AssumptionViolatedException} that does not carry a stack trace.
         * <p>
         * This variant skips the large majority of the CSV corpus &mdash; thousands of tests &mdash; and JUnit serialises
         * the full stack trace of each thrown exception into the {@code <skipped>} element of the results XML. Those traces
         * are identical from one skip to the next and add nothing beyond the human-readable message, yet at corpus scale
         * they inflate the XML to tens of megabytes. The two overrides below reproduce the effect of constructing a
         * {@link Throwable} with {@code writableStackTrace=false} &mdash; the constructor parameter that
         * {@link AssumptionViolatedException} does not expose: {@link #fillInStackTrace()} never records a trace, and
         * {@link #setStackTrace} ignores the synthetic seed frame the randomized runner would otherwise splice in. Each
         * skip therefore keeps its reason while dropping the redundant trace entirely.
         */
        private static final class StacklessAssumptionViolatedException extends AssumptionViolatedException {
            StacklessAssumptionViolatedException(String message) {
                super(message);
            }

            @Override
            public synchronized Throwable fillInStackTrace() {
                return this;
            }

            @Override
            public void setStackTrace(StackTraceElement[] stackTrace) {
                // Intentionally a no-op: mirrors writableStackTrace=false so the randomized runner cannot re-attach a
                // (single, identical) seed frame to the otherwise empty trace.
            }
        }
    }

    public static final java.util.List<String> EXPECTED_ERRORS = java.util.List.of(
        "ABSENT_OVER_TIME:field is missing",
        "CIDR_MATCH:blockX is missing",
        "CLAMP:field is missing",
        "CLAMP:max is missing",
        "CLAMP:min is missing",
        "CLAMP_MAX:field is missing",
        "CLAMP_MAX:max is missing",
        "CLAMP_MIN:field is missing",
        "CLAMP_MIN:min is missing",
        "COUNT_DISTINCT_OVER_TIME:field is missing",
        "COUNT_OVER_TIME:field is missing",
        "DATE_DIFF:unit is missing",
        "EMBEDDING:value is missing",
        "FIELD_EXTRACT:path is missing",
        "FIRST_OVER_TIME:field is missing",
        "FROM_BASE64:string is missing",
        "GREATEST:first is missing",
        "GREATEST:rest is missing",
        "JSON_EXTRACT:string is missing",
        "KNN:field is missing",
        "KQL:query is missing",
        "LAST_OVER_TIME:field is missing",
        "LEAST:first is missing",
        "LEAST:rest is missing",
        "MATCH:query is missing",
        "MATCH_OPERATOR:field is missing",
        "MATCH_OPERATOR:query is missing",
        "MAX_OVER_TIME:field is missing",
        "MIN_OVER_TIME:field is missing",
        // mv_in_range's bounds are literals in the csv-specs (like the comparison operators below), so its
        // keyword/text parameters are not exercised via flattened-keyword field extraction.
        "MV_IN_RANGE:field is missing",
        "MV_IN_RANGE:lower is missing",
        "MV_IN_RANGE:upper is missing",
        // MV_SORT's order argument is now marked as a CONSTANT hint in the function's docs
        // metadata, so it is excluded from the candidate set entirely (see the "constant".equals(kind)
        // check below) and never appears here as missing.
        "NETWORK_DIRECTION:internal_networks is missing",
        "PRESENT_OVER_TIME:field is missing",
        "QSTR:query is missing",
        "SPARKLINE:from is missing",
        "SPARKLINE:to is missing",
        "TEXT_EMBEDDING:text is missing",
        "TO_CARTESIANPOINT:field is missing",
        "TO_CARTESIANSHAPE:field is missing",
        "TO_DATETIME:field is missing",
        "TO_DATE_NANOS:field is missing",
        "TO_DATE_RANGE:field is missing",
        "TO_DENSE_VECTOR:field is missing",
        "TO_DOUBLE:field is missing",
        "TO_GEOHASH:field is missing",
        "TO_GEOHEX:field is missing",
        "TO_GEOSHAPE:field is missing",
        "TO_GEOTILE:field is missing",
        "TO_UNSIGNED_LONG:field is missing",
        "TO_VERSION:field is missing",
        "WITHOUT:dimension is missing"
    );

    @AfterClass
    @SuppressWarnings("unchecked")
    public static void verifyFieldExtractCoverage() throws Exception {
        if (org.elasticsearch.Build.current().isSnapshot() == false) {
            return;
        }

        String kibanaDirProp = System.getProperty("esql.kibana.docs.dir");
        if (kibanaDirProp == null) {
            throw new IllegalStateException("System property esql.kibana.docs.dir is not set");
        }
        Path kibanaDir = PathUtils.get(kibanaDirProp);
        if (Files.isDirectory(kibanaDir) == false) {
            throw new IllegalStateException("Could not find docs/reference/query-languages/esql/kibana/generated at " + kibanaDir);
        }

        KeywordToFlattenedStrategy strategy = (KeywordToFlattenedStrategy) indexLoadStrategy;

        Set<String> coveredArguments = new HashSet<>();
        for (CsvSpecReader.CsvTestCase testCase : KeywordToFlattenedStrategy.loadAllCsvSpecTestCases()) {
            coveredArguments.addAll(getCoveredArguments(strategy, testCase));
        }

        Set<String> candidates = new HashSet<>();
        Map<String, Set<String>> paramNamesByIndex = new HashMap<>();

        try (Stream<Path> paths = Files.walk(kibanaDir)) {
            paths.filter(Files::isRegularFile).filter(p -> p.toString().endsWith(".json")).forEach(p -> {
                try {
                    try (
                        XContentParser parser = JsonXContent.jsonXContent.createParser(
                            NamedXContentRegistry.EMPTY,
                            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                            Files.newInputStream(p)
                        )
                    ) {
                        Map<String, Object> map = parser.map();
                        String name = (String) map.get("name");
                        if (name == null) return;
                        name = name.toUpperCase(Locale.ROOT);

                        /*
                         * The parser just refuses to build these real looking functions, instead building something
                         * like NOT(IN()). So we skip tracking them here - though we do actually test them. NOT_LIKE
                         * and NOT_RLIKE fall into the same bucket: they parse to Not(WildcardLike(...))/Not(RLike(...)),
                         * so there is no distinct AST node to track coverage against.
                         */
                        boolean rewrittenAwayAtParseTime = switch (name) {
                            case "NOT_EQUALS", "NOT_IN", "NOT_LIKE", "NOT_RLIKE" -> true;
                            default -> false;
                        };
                        if (rewrittenAwayAtParseTime) {
                            return;
                        }

                        List<Map<String, Object>> signatures = (List<Map<String, Object>>) map.get("signatures");
                        if (signatures == null) return;
                        for (Map<String, Object> sig : signatures) {
                            List<Map<String, Object>> params = (List<Map<String, Object>>) sig.get("params");
                            if (params != null) {
                                for (int i = 0; i < params.size(); i++) {
                                    String indexKey = name + ":" + i;
                                    String paramName = (String) params.get(i).get("name");
                                    if (paramName != null) {
                                        paramNamesByIndex.computeIfAbsent(indexKey, k -> new TreeSet<>()).add(paramName);
                                    }

                                    Map<String, Object> hint = (Map<String, Object>) params.get(i).get("hint");
                                    if (hint != null) {
                                        Object kind = hint.get("kind");
                                        if ("entity".equals(kind) || "aggregation".equals(kind) || "constant".equals(kind)) {
                                            continue;
                                        }
                                    }
                                    if (params.get(i).containsKey("mapParams")) {
                                        continue;
                                    }

                                    Object typeObj = params.get(i).get("type");
                                    if (typeObj instanceof String) {
                                        String t = (String) typeObj;
                                        if ("keyword".equals(t) || "text".equals(t)) {
                                            candidates.add(indexKey);
                                        }
                                    }
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Error parsing " + p, e);
                }
            });
        }

        Map<String, String> indexToName = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : paramNamesByIndex.entrySet()) {
            indexToName.put(entry.getKey(), entry.getKey().split(":")[0] + ":" + String.join("|", entry.getValue()));
        }

        Set<String> mappedCandidates = new HashSet<>();
        for (String c : candidates) {
            mappedCandidates.add(indexToName.getOrDefault(c, c));
        }

        Set<String> mappedCovered = new HashSet<>();
        for (String c : coveredArguments) {
            mappedCovered.add(indexToName.getOrDefault(c, c));
        }

        List<String> errors = new ArrayList<>();
        for (String candidate : mappedCandidates) {
            if (mappedCovered.contains(candidate) == false) {
                errors.add(candidate + " is missing");
            }
        }
        for (String covered : mappedCovered) {
            if (mappedCandidates.contains(covered) == false) {
                errors.add(covered + " is unexpected");
            }
        }
        errors.sort(Comparator.naturalOrder());

        assertMap("Missing field_extract coverage for parameters\n", errors, matchesList(EXPECTED_ERRORS));
    }

    private static Set<String> getCoveredArguments(KeywordToFlattenedStrategy strategy, CsvSpecReader.CsvTestCase testCase) {
        for (String cap : testCase.requiredCapabilities) {
            if (cap.equals("ts_info_command") || cap.equals("metrics_info_command")) {
                return Set.of();
            }
        }

        String skipReason = testCase.skipFlattenedRewrite;
        if (skipReason != null && skipReason.isBlank() == false) {
            return Set.of();
        }

        String originalQuery = testCase.query;
        if (originalQuery == null || originalQuery.isBlank()) {
            return Set.of();
        }

        List<String> expectedColumnOrder = KeywordToFlattenedStrategy.parseExpectedColumnOrder(testCase.expectedResults);
        AstKeywordFieldRewriter.RewriteResult result = AstKeywordFieldRewriter.rewrite(
            originalQuery,
            strategy::resolveKeywordPathsForQuery,
            KeywordToFlattenedTransformer.WRAPPER_SUBKEY,
            expectedColumnOrder
        );
        return result.coveredArguments();
    }
}
