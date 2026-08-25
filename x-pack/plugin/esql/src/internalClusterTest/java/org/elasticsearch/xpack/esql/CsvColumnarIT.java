/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.CSV_DATASET;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.VIEW_CONFIGS;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.isLookupDataset;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.isTimeSeries;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;

/**
 * Integration test that re-runs the {@link CsvIT} csv-spec corpus against indices created with
 * {@code index.mode=columnar}, using the corpus's own expected results as the oracle.
 *
 * <p>The goal is to ensure that ES|QL query results are identical between standard and columnar
 * index modes for the large majority of queries, and to surface regressions early by running the
 * full csv-spec body &mdash; thousands of assertions &mdash; against real columnar indices.
 *
 * <h2>Compatibility filtering</h2>
 *
 * <p>Not all datasets or tests can run in columnar mode:
 * <ul>
 *   <li>Datasets in {@link #COLUMNAR_INCOMPATIBLE_DATASETS} are excluded entirely because they
 *       either fail to be created in columnar mode or produce legitimately different results by
 *       design (e.g. {@code index:false} becomes a no-op, geo_point precision differs). Each
 *       entry carries a comment naming the reason.</li>
 *   <li>Datasets with {@code index.mode: time_series} are excluded: the dimension, routing, and
 *       counter field requirements of TSDB cannot simply be re-moded to columnar.</li>
 *   <li>Individual tests may carry a {@code skip_columnar: <reason>} preamble directive (parsed
 *       by {@link CsvSpecReader}) to silence a known per-test limitation without excluding the
 *       whole dataset.</li>
 * </ul>
 *
 * <p>Every exclusion decision is reported in the post-run {@link #logColumnarSummary()} line, so
 * lost coverage is never silent.
 *
 * <h2>Mapping sanitisation</h2>
 *
 * <p>The {@link ColumnarStrategy} removes mapping runtime fields before creating each index because
 * {@code IndexMode.COLUMNAR.validateMapping} calls {@code validateNoMappingRuntimeFields}. Datasets
 * with {@code store: true} fields (currently only {@code hosts} and {@code hosts_ip_is_kwd}) are
 * excluded in {@link #COLUMNAR_INCOMPATIBLE_DATASETS} rather than silently stripped.</p>
 *
 * <p>Lookup-mode datasets ({@code index.mode: lookup}) are deliberately left alone so that
 * {@code LOOKUP JOIN} tests can still execute with a columnar primary index.
 *
 * <h2>Running this test</h2>
 * <pre>{@code
 * ./gradlew :x-pack:plugin:esql:internalClusterTest \
 *     --tests "org.elasticsearch.xpack.esql.CsvColumnarIT"
 * }</pre>
 * To target a single spec file: append {@code .*<filename-fragment>*} to the test pattern.
 * After a run, grep the output for {@code columnar summary:} to see the coverage inventory.
 */
public class CsvColumnarIT extends CsvIT {

    private static final Logger logger = LogManager.getLogger(CsvColumnarIT.class);

    /**
     * Datasets excluded from the columnar variant because they either fail index creation in
     * columnar mode or produce legitimately different results by design. Each entry has a comment
     * naming the reason, cross-referenced to the catalogue in
     * {@code CrossIndexModeGenerativeRestTest.EXCLUDED_DATASETS} and
     * {@code LogsDbSubobjectsFalseVersusLogsDbColumnarRestIT} where applicable.
     *
     * <p>Note: several entries in the generative-test catalogue
     * ({@code addresses_text}, {@code employees_gender_text}, {@code all_types},
     * {@code all_types_no_short}, {@code all_types_short_as_long}, {@code apps_short}) were
     * artifacts of the ref_/cand_ side-by-side wildcard approach and do NOT apply here.
     * They are still included because columnar auto-converts text→keyword and
     * short→long, causing expected column-type headers in the csv-spec entries to mismatch.
     * Revisit once the inventory (via {@code skip_columnar:} directives) is established and
     * transformExpectedResults becomes worth implementing.
     */
    private static final Set<String> COLUMNAR_INCOMPATIBLE_DATASETS = Set.of(
        // index:false / doc_values:false are no-ops in strict columnar mode — every field gets
        // doc values and is searchable — so query results differ by design from a standard index
        // that honours those settings.
        "airports_not_indexed",
        "airports_no_doc_values",
        "airports_not_indexed_nor_doc_values",
        // geo_point fields are stored at different precision in columnar mode: to_string() returns
        // slightly different coordinates (e.g. "POINT (116.072 5.975)" vs "POINT (116.073 5.975)").
        // See CrossIndexModeGenerativeRestTest.EXCLUDED_DATASETS.
        "airports",
        "airports_web",
        // Mapping designed to be type-incompatible with the standard employees dataset; its CSV
        // data contains deliberate duplicates in boolean MV fields (e.g. [false,true,true]).
        // SortedSetDocValues deduplicates those in standard mode while columnar may preserve them.
        "employees_incompatible",
        // Multi-value double / date fields cause COUNT to count documents instead of individual
        // MV values in columnar mode, producing different aggregate results.
        "all_types_mv",
        "mv_decades",
        // Contains semantic_text and dense_vector fields that are absent from columnar field_caps,
        // and has a short-typed field "short" that columnar normalises to long — both cause
        // expected column-type header mismatches vs csv-spec declared types.
        "all_types",
        "all_types_no_short",
        "all_types_short_as_long",
        // id field overridden to short; columnar normalises short→long, causing a type conflict
        // vs the base apps dataset (id: integer) and expected-type mismatches.
        "apps_short",
        // Keyword fields overridden to text; columnar auto-converts text→keyword, so expected
        // column types in csv-spec entries (text) mismatch the actual columnar types (keyword).
        "addresses_text",
        "employees_gender_text",
        // Contains a plain txt:text field with no doc_values; fails index creation in columnar
        // mode because text without doc_values cannot be reconstructed from doc values.
        "text_state_mapped",
        // Contains a plain text field; querying in columnar mode crashes the server.
        // TODO: file an issue and reference it here.
        "json_logs",
        "voyager",
        // cartesian_shape field cannot be stored via doc_values for synthetic source, so bulk
        // indexing fails in columnar mode (zero documents indexed).
        "cartesian_multipolygons",
        // cartesian_shape field with doc_values:false cannot be reconstructed from doc values
        // in columnar mode: "field [shape] cannot reconstruct _source from doc values".
        "cartesian_multipolygons_no_doc_values",
        // 245 000+ documents with MV integer fields; bulk indexing and force-merge can time out
        // in columnar mode or exceed REST client limits.
        "many_numbers",
        // Known columnar bug: STATS output aliases whose names conflict with existing index fields
        // read from the wrong source, producing incorrect aggregate values.
        // TODO: file an issue and reference it here.
        "ul_logs",
        // index.mapping.index_disabled_by_default=true disables the inverted index for fields
        // without an explicit "index: true", so full-text (:) queries return different results
        // between standard and columnar modes.
        "conv_from_keyword",
        // Mappings that disable or exclude _source are rejected by columnar mode:
        // "Failed to parse mapping: _source can not be disabled in index using [columnar] index mode".
        // These datasets test _source-disabled / _source-excluded query behavior, which does not
        // apply in columnar mode (columnar always reconstructs source from doc values).
        "partial_mapping_no_source_sample_data",
        "partial_mapping_mv_no_source_sample_data",
        "partial_mapping_excluded_source_sample_data",
        // LOAD_ALL / LOAD from source loads unmapped fields directly from the stored _source.
        // In columnar mode, _source is synthetic (reconstructed from doc values), so unmapped
        // fields — fields that exist in the stored document but have no mapping entry — are not
        // available. All unmapped-load-all tests and the unmapped-load tests that load fields
        // absent from the mapping therefore produce 0-column / 0-row results in columnar mode.
        "partial_mapping_sample_data",
        "partial_mapping_mv_sample_data",
        // no_mapping_sample_data has no explicit mapping; all its fields are unmapped. When
        // combined with other indices in a multi-index query and LOAD is used to load the
        // unmapped fields, columnar mode returns null for them (synthetic _source cannot
        // reconstruct fields that have no mapping entry). Excluding this dataset removes all
        // type-conflict tests that depend on unmapped-field loading from this source.
        "no_mapping_sample_data",
        // Keyword fields with a normalizer (e.g. test_lowercase) store only the normalised form
        // in doc values, losing the original value. Columnar mode therefore cannot reconstruct
        // the original _source for these fields and rejects index creation with
        // "field [kw] cannot reconstruct _source from doc values".
        "normalized_keyword",
        "normalized_keyword_unmapped",
        // mapping-hosts.json has store:true on one field; strict columnar mode rejects store:true
        // rather than silently ignoring it, so we exclude these datasets instead of stripping the
        // attribute from the mapping.
        "hosts",
        "hosts_ip_is_kwd"
    );

    public CsvColumnarIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions);
    }

    /**
     * Filters the csv-spec corpus to the tests this columnar variant can actually exercise.
     *
     * <p>Drops tests whose resolved dataset set intersects {@link #COLUMNAR_INCOMPATIBLE_DATASETS}
     * or contains a {@code time_series} dataset. Both are diagnosed at generation time (rather than
     * at run time) because {@link CsvIT} loads datasets lazily via {@code assertAcked}: an
     * incompatible dataset would produce a hard test error on whichever test first touches it, not
     * a skip.
     *
     * <p>Tests with a {@code skip_columnar:} preamble directive are intentionally
     * <em>kept</em> so their per-test skip reason remains visible in the JUnit XML.
     *
     * <p>Hides {@link CsvIT#readScriptSpec()} (same signature, so the randomized runner treats
     * this override as the single {@code @ParametersFactory} for the class).
     */
    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s", shuffle = false)
    public static List<Object[]> readScriptSpec() throws Exception {
        List<Object[]> all = CsvIT.readScriptSpec();
        List<Object[]> generated = new ArrayList<>(all.size());
        for (Object[] row : all) {
            if (row[4] instanceof CsvTestCase testCase && shouldExclude(testCase)) {
                // Excluded at generation time; not even generated as a skipped entry
                continue;
            }
            generated.add(row);
        }
        return generated;
    }

    /**
     * Extracts the {@code index=<pattern>} parameter from a {@code PROMQL} source command.
     * {@link EsqlQueryDatasetResolver#extractIndexPatterns} only recognises {@code FROM} and
     * {@code TS}; {@code PROMQL} queries use a {@code key=value} parameter syntax that the shared
     * regex never sees, so they always yield an empty pattern set and slip through
     * {@link #shouldExclude}.
     *
     * <p>The pattern anchors on start-of-input, {@code |}, or {@code (} (same convention as
     * {@code SOURCE_COMMAND} in {@code EsqlQueryDatasetResolver}) then skips optional parameters
     * before the {@code index=} key. The index value is unquoted and delimited by whitespace,
     * comma, semicolon, closing paren, or pipe.
     */
    private static final Pattern PROMQL_INDEX = Pattern.compile(
        "(?:^|\\||\\()\\s*PROMQL\\b[^|)]*?\\bindex=([^\\s,;)|]+)",
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

    /**
     * Extracts double-quoted index names that immediately follow a {@code FROM} or {@code TS}
     * keyword and are terminated by a pipe, closing paren, or end-of-input.
     *
     * <p>{@link EsqlQueryDatasetResolver#maskStringsAndComments} replaces every double-quoted
     * string with spaces of equal length before regex matching. When the index name is
     * immediately followed by a pipe — {@code TS "k8s" | ...} — the lazy {@code ([^|()]+?)+?}
     * quantifier in {@code SOURCE_COMMAND} captures only a single space, and
     * {@code query.substring(start, end)} yields a 1-character string that cannot be
     * de-quoted by {@link #normalizeIndexPattern} (which requires {@code length >= 2}). This
     * pattern operates directly on the unmasked stripped query so the literal quote characters
     * are present and the full name is captured.
     */
    private static final Pattern DOUBLE_QUOTED_SOURCE = Pattern.compile(
        "(?:^|\\||\\()\\s*(?:FROM|TS)\\s+\"([^\"]+)\"",
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

    /**
     * Maps each view name to the {@link CsvTestsDataLoader.TestDataset} entries that back it,
     * derived by running {@link EsqlQueryDatasetResolver#extractIndexPatterns} on the view's
     * {@code .esql} body (loaded from the test-fixtures resources via
     * {@link CsvTestsDataLoader.ViewConfig#loadQuery()}). Views whose bodies reference no
     * resolvable dataset (e.g. pure {@code ROW} or {@code SHOW} views) map to an empty set.
     *
     * <p>Used by {@link #resolveAllDatasets} to expand a view name in a query's index list to the
     * dataset(s) it reads from, so {@link #shouldExclude} can detect incompatible or time-series
     * datasets that are only reachable via a view.
     */
    private static final Map<String, Set<CsvTestsDataLoader.TestDataset>> VIEW_BACKING_DATASETS = VIEW_CONFIGS.entrySet()
        .stream()
        .collect(
            java.util.stream.Collectors.toUnmodifiableMap(
                Map.Entry::getKey,
                e -> EsqlQueryDatasetResolver.resolveDatasets(
                    EsqlQueryDatasetResolver.extractIndexPatterns(e.getValue().loadQuery()),
                    CSV_DATASET
                )
            )
        );

    /**
     * Names of ES views that wrap time-series ({@code TS}) subqueries and are therefore
     * incompatible with columnar mode for the same reason as their backing {@code k8s} dataset:
     * columnar mode rejects {@code index.routing_path} settings, which are required by the
     * underlying time-series indices.
     *
     * <p>Used by {@link #touchesTsBackedView} to catch wildcard patterns like
     * {@code view_k8s_max_*} that would not resolve via {@link #VIEW_BACKING_DATASETS} (the
     * wildcard does not match any single view name key).
     */
    private static final Set<String> TS_BACKED_VIEWS = VIEW_CONFIGS.entrySet()
        .stream()
        .filter(e -> e.getValue().requiredCapabilities().contains(EsqlCapabilities.Cap.SUBQUERY_WITH_TS))
        .map(Map.Entry::getKey)
        .collect(java.util.stream.Collectors.toUnmodifiableSet());

    /**
     * Returns {@code true} when the test references a TS-backed view, either by exact name or
     * via a wildcard pattern (e.g. {@code view_k8s_max_*}) that matches at least one entry in
     * {@link #TS_BACKED_VIEWS}.
     */
    private static boolean touchesTsBackedView(Set<String> patterns) {
        for (String pattern : patterns) {
            if (TS_BACKED_VIEWS.contains(pattern)) {
                return true;
            }
            // Wildcard pattern: convert the Elasticsearch glob to a Java regex and test against
            // every known TS-backed view name. Only '*' is special (no '?' in practice here).
            if (pattern.contains("*")) {
                String regex = "\\Q" + pattern.replace("*", "\\E.*\\Q") + "\\E";
                for (String viewName : TS_BACKED_VIEWS) {
                    if (viewName.matches(regex)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Resolves all {@link CsvTestsDataLoader.TestDataset} entries that the given query touches,
     * combining five extraction paths that {@link EsqlQueryDatasetResolver#extractIndexPatterns}
     * alone would miss:
     * <ol>
     *   <li>{@code FROM} / {@code TS} index patterns — delegated to {@code EsqlQueryDatasetResolver}
     *       after stripping leading {@code SET} statements via {@link #stripLeadingSetStatements}.</li>
     *   <li>{@code PROMQL index=<pattern>} — extracted by {@link #PROMQL_INDEX}.</li>
     *   <li>Double-quoted index names (e.g. {@code TS "k8s" | ...}) — extracted by
     *       {@link #DOUBLE_QUOTED_SOURCE} on the unmasked text, bypassing the masking blind spot.</li>
     *   <li>View names (exact) — each raw pattern is looked up in {@link #VIEW_BACKING_DATASETS}; if
     *       it matches a view the view's backing datasets are added to the result.</li>
     *   <li>Wildcard view patterns (e.g. {@code FROM country_a*}) — glob-matched against all
     *       {@link #VIEW_BACKING_DATASETS} keys so that prefix wildcards expand to the right backing
     *       datasets even when no single view name equals the pattern.</li>
     * </ol>
     *
     * <p>The result is an over-approximation (same contract as
     * {@link EsqlQueryDatasetResolver#resolveDatasets}): unknown patterns are silently skipped and
     * the method never throws.
     */
    private static Set<CsvTestsDataLoader.TestDataset> resolveAllDatasets(String query) {
        String stripped = stripLeadingSetStatements(query);
        // Collect raw patterns from FROM/TS and PROMQL, then normalise each one.
        Set<String> rawPatterns = new LinkedHashSet<>(EsqlQueryDatasetResolver.extractIndexPatterns(stripped));
        Matcher m = PROMQL_INDEX.matcher(stripped);
        while (m.find()) {
            rawPatterns.add(m.group(1).trim());
        }
        // Double-quoted index names (e.g. TS "k8s" | ...) are masked to spaces before the shared
        // SOURCE_COMMAND regex runs, so extractIndexPatterns captures only a 1-char fragment that
        // normalizeIndexPattern cannot de-quote. Match directly on the unmasked stripped text.
        Matcher dq = DOUBLE_QUOTED_SOURCE.matcher(stripped);
        while (dq.find()) {
            rawPatterns.add(dq.group(1).trim());
        }
        Set<String> patterns = new LinkedHashSet<>();
        for (String raw : rawPatterns) {
            patterns.add(normalizeIndexPattern(raw));
        }
        Set<CsvTestsDataLoader.TestDataset> datasets = new LinkedHashSet<>(EsqlQueryDatasetResolver.resolveDatasets(patterns, CSV_DATASET));
        for (String pattern : patterns) {
            // Exact-key view lookup (e.g. FROM country_airports).
            Set<CsvTestsDataLoader.TestDataset> backing = VIEW_BACKING_DATASETS.get(pattern);
            if (backing != null) {
                datasets.addAll(backing);
            }
            // Wildcard patterns (e.g. FROM country_a*) do not match any single view-name key;
            // glob-expand against all VIEW_BACKING_DATASETS keys and union the results.
            if (pattern.contains("*")) {
                String regex = "\\Q" + pattern.replace("*", "\\E.*\\Q") + "\\E";
                for (Map.Entry<String, Set<CsvTestsDataLoader.TestDataset>> entry : VIEW_BACKING_DATASETS.entrySet()) {
                    if (entry.getKey().matches(regex)) {
                        datasets.addAll(entry.getValue());
                    }
                }
            }
        }
        return Set.copyOf(datasets);
    }

    /**
     * Normalises a raw index pattern extracted by {@link EsqlQueryDatasetResolver#extractIndexPatterns}
     * into a canonical form suitable for dataset resolution and view-backing lookup.
     *
     * <p>Two non-canonical forms arise in practice:
     * <ul>
     *   <li><em>Double-quoted identifiers</em>: {@code TS "k8s"} is valid ES|QL syntax.
     *       {@link EsqlQueryDatasetResolver} masks string literals before regex matching, so the
     *       capture group positions span the quoted text in the original query; extracting via
     *       {@code query.substring(...)} yields {@code "k8s"} with literal quote characters. After
     *       stripping the enclosing quotes, {@code k8s} resolves normally against
     *       {@link CsvTestsDataLoader#CSV_DATASET}.</li>
     *   <li><em>View accessor suffixes</em>: {@code FROM country_airports::data} accesses the
     *       underlying data source of a view. The {@code ::data} (and {@code ::metadata}) suffix is
     *       not part of the view name and must be removed before the name can be looked up in
     *       {@link #VIEW_BACKING_DATASETS}.</li>
     * </ul>
     */
    private static String normalizeIndexPattern(String pattern) {
        if (pattern.startsWith("\"") && pattern.endsWith("\"") && pattern.length() >= 2) {
            pattern = pattern.substring(1, pattern.length() - 1);
        }
        int colonColon = pattern.indexOf("::");
        if (colonColon >= 0) {
            pattern = pattern.substring(0, colonColon);
        }
        return pattern;
    }

    /**
     * Returns {@code query} with any leading {@code SET <name>=<value>;\r\n} lines removed.
     *
     * <p>{@link EsqlQueryDatasetResolver#extractIndexPatterns} uses a regex that anchors on
     * start-of-input, {@code |}, or {@code (} (deliberately, to avoid widening
     * {@code CsvFlattenedKeywordIT}'s keyword-path scoping). When a csv-spec query opens with one or
     * more {@code SET} lines — which {@link CsvSpecReader} appends to the query as {@code SET
     * name=value;\r\n} — the subsequent {@code FROM} sits after a newline and the regex returns no
     * patterns, causing {@link #shouldExclude} to miss time-series or incompatible-dataset sources.
     * Stripping the {@code SET} lines here restores the extraction without touching the shared
     * resolver.
     *
     * <p>Only strips lines whose first non-whitespace token is {@code SET} (case-insensitive) followed
     * by a non-whitespace identifier, an equals sign, and a value ending in {@code ;} — the exact
     * form {@code CsvSpecReader} emits. Lines that do not match are left intact; the first non-{@code
     * SET} line and all subsequent lines are returned unchanged.
     */
    private static String stripLeadingSetStatements(String query) {
        // CsvSpecReader joins lines with "\r\n"; split on any line ending and reconstruct.
        String[] lines = query.split("\r\n|\r|\n", -1);
        int first = 0;
        while (first < lines.length && lines[first].stripLeading().toUpperCase(Locale.ROOT).startsWith("SET ")) {
            first++;
        }
        if (first == 0) {
            return query;
        }
        return String.join("\r\n", java.util.Arrays.copyOfRange(lines, first, lines.length));
    }

    private static boolean shouldExclude(CsvTestCase testCase) throws IOException {
        // Tests with a per-test skip directive are kept so the reason shows up in JUnit XML.
        if (testCase.skipColumnar != null) {
            return false;
        }
        // Wildcard view patterns (e.g. view_k8s_max_*) are not resolvable via the exact-key
        // VIEW_BACKING_DATASETS lookup in resolveAllDatasets; check them first against the known
        // TS-backed view set via glob matching. Normalise (strip quotes / ::suffix) so that
        // patterns like "view_k8s_max_*" or view_k8s_max_*::data still match TS_BACKED_VIEWS.
        Set<String> rawPatterns = EsqlQueryDatasetResolver.extractIndexPatterns(stripLeadingSetStatements(testCase.query));
        Set<String> normalizedForViewCheck = new LinkedHashSet<>();
        for (String raw : rawPatterns) {
            normalizedForViewCheck.add(normalizeIndexPattern(raw));
        }
        if (touchesTsBackedView(normalizedForViewCheck)) {
            EXCLUDED_TIME_SERIES_COUNT.incrementAndGet();
            return true;
        }
        // Full dataset resolution: FROM/TS patterns, PROMQL index=<pattern>, and view backing.
        for (CsvTestsDataLoader.TestDataset dataset : resolveAllDatasets(testCase.query)) {
            if (COLUMNAR_INCOMPATIBLE_DATASETS.contains(dataset.indexName())) {
                EXCLUDED_INCOMPATIBLE_COUNT.incrementAndGet();
                return true;
            }
            if (isTimeSeries(dataset)) {
                EXCLUDED_TIME_SERIES_COUNT.incrementAndGet();
                return true;
            }
        }
        return false;
    }

    // -----------------------------------------------------------------------------------------
    // Per-JVM counters for the post-run coverage summary
    // -----------------------------------------------------------------------------------------

    private static final AtomicInteger LAUNCHED_COUNT = new AtomicInteger();
    private static final AtomicInteger SILENCED_COUNT = new AtomicInteger();
    private static final AtomicInteger EXCLUDED_INCOMPATIBLE_COUNT = new AtomicInteger();
    private static final AtomicInteger EXCLUDED_TIME_SERIES_COUNT = new AtomicInteger();

    /**
     * Per-reason silenced counter, keyed by the verbatim {@code skip_columnar:} value.
     * Built up lazily as silenced tests are encountered so the summary only reports reasons
     * that were actually seen by this JVM.
     */
    private static final ConcurrentMap<String, AtomicInteger> SILENCED_COUNTS_BY_REASON = new ConcurrentHashMap<>();

    /**
     * Datasets that reached {@link ColumnarStrategy#transformSettings} but could not safely be
     * columnar-ized — because they are time-series or in {@link #COLUMNAR_INCOMPATIBLE_DATASETS} —
     * and were therefore loaded in their original mode. These represent coverage gaps where
     * {@link #shouldExclude} failed to exclude the test at generation time.
     *
     * <p>The historical causes (now fixed) were {@code PROMQL index=<pattern>} queries and queries
     * that reference a view name not present in {@link CsvTestsDataLoader#CSV_DATASET}; both are
     * now handled by {@link #resolveAllDatasets}. A non-empty set means a future query shape has
     * appeared that neither {@link EsqlQueryDatasetResolver} nor {@link #resolveAllDatasets}
     * recognises. {@link #logColumnarSummary} fails the suite when this set is non-empty so the
     * gap cannot silently degrade coverage.
     */
    private static final Set<String> FORCED_STANDARD_DATASETS = ConcurrentHashMap.newKeySet();

    // -----------------------------------------------------------------------------------------
    // Strategy installation
    // -----------------------------------------------------------------------------------------

    /**
     * Returns the extra index settings stamped on top of each dataset's own settings when creating
     * its index. Default: {@code index.mode=columnar}. Subclasses (e.g. a future
     * {@code CsvLogsdbColumnarIT}) may override to choose a different mode.
     */
    protected static Settings modeSettings() {
        return Settings.builder().put("index.mode", "columnar").build();
    }

    /**
     * Installs the columnar index-load strategy.
     *
     * <p>Runs after {@link CsvIT#setupCluster()} (JUnit guarantees the superclass
     * {@code @BeforeClass} runs first) and replaces the identity strategy with one that creates
     * every dataset in {@link #modeSettings()} after sanitising the mapping.
     *
     * <p>No build-type {@code assumeTrue} gate is needed: unlike the {@code flattened} datatype,
     * columnar index modes are not snapshot-gated. {@code CreateIndexCapabilities} adds
     * {@code columnar_index_modes} unconditionally and BWC is handled by transport version
     * {@code IndexMode.COLUMNAR_INDEX_MODES_ADDED}.
     */
    @BeforeClass
    public static void installColumnarStrategy() {
        indexLoadStrategy = new ColumnarStrategy(modeSettings());
    }

    @AfterClass
    public static void logColumnarSummary() {
        int silenced = SILENCED_COUNTS_BY_REASON.values().stream().mapToInt(AtomicInteger::get).sum();
        int launched = LAUNCHED_COUNT.get();
        int incompatible = EXCLUDED_INCOMPATIBLE_COUNT.get();
        int timeSeries = EXCLUDED_TIME_SERIES_COUNT.get();
        logger.info(
            "columnar summary: launched={} silenced={} incompatible-dataset={} time-series={} forced-standard={}",
            launched,
            silenced,
            incompatible,
            timeSeries,
            FORCED_STANDARD_DATASETS.size()
        );
        SILENCED_COUNTS_BY_REASON.entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(e -> logger.info("columnar summary: silenced[{}]={}", e.getKey(), e.getValue().get()));
        if (FORCED_STANDARD_DATASETS.isEmpty() == false) {
            logger.warn(
                "columnar summary: {} dataset(s) forced to standard mode at load time (shouldExclude missed them): {}",
                FORCED_STANDARD_DATASETS.size(),
                FORCED_STANDARD_DATASETS.stream().sorted().toList()
            );
        }
        assertThat(
            "datasets ran in standard mode inside the columnar suite; "
                + "add the query shape to resolveAllDatasets or exclude the test. "
                + "Affected: "
                + FORCED_STANDARD_DATASETS.stream().sorted().toList(),
            FORCED_STANDARD_DATASETS,
            empty()
        );
    }

    // -----------------------------------------------------------------------------------------
    // ColumnarStrategy
    // -----------------------------------------------------------------------------------------

    /**
     * {@link CsvIT.IndexLoadStrategy} that creates each dataset in columnar index mode.
     *
     * <p>The strategy:
     * <ul>
     *   <li>Stamps {@link #modeSettings()} on top of the dataset's own index settings — except for
     *       lookup-mode datasets (must stay in {@code lookup} for LOOKUP JOIN), time-series datasets
     *       (ES|QL's {@code TS} source command hard-filters on {@code _index_mode == time_series} at
     *       field-caps resolution, so a columnar-ized {@code k8s} would fail every {@code TS k8s}
     *       test with "is not a time series index"), and datasets in
     *       {@link #COLUMNAR_INCOMPATIBLE_DATASETS} (they produce wrong results by design, unrelated
     *       to {@code routing_path}).</li>
     *   <li>Removes mapping-level runtime fields (strict columnar requires all fields to be
     *       reconstructable from doc values, and runtime fields are not).</li>
     *   <li>Honours the {@code skip_columnar:} preamble directive to silence individual tests.</li>
     * </ul>
     */
    private static final class ColumnarStrategy implements IndexLoadStrategy {

        private final Settings extraSettings;

        ColumnarStrategy(Settings extraSettings) {
            this.extraSettings = extraSettings;
        }

        @Override
        public String transformMapping(CsvTestsDataLoader.TestDataset dataset, String originalMapping) throws IOException {
            return sanitizeMapping(originalMapping);
        }

        @Override
        public Settings transformSettings(CsvTestsDataLoader.TestDataset dataset, Settings settings) {
            try {
                if (isLookupDataset(dataset)) {
                    // Lookup mode is required for LOOKUP JOIN; leave untouched so join tests still run.
                    logger.debug("columnar: keeping lookup mode for dataset [{}]", dataset.indexName());
                    return settings;
                }
                if (isTimeSeries(dataset)) {
                    // IndexMode.COLUMNAR rejects index.routing_path — that is the only setting-level
                    // blocker. But removing routing_path deliberately is not done: ES|QL's TS source
                    // command hard-filters field-caps resolution on _index_mode == time_series
                    // (EsqlSession.createQueryFilter). A columnar-ized k8s would fail every
                    // "TS k8s" test ("is not a time series index; use FROM instead") and silently
                    // return empty for wildcard forms. Keeping the original settings is the only safe
                    // choice. Both categories below are excluded at generation time by shouldExclude,
                    // but a query not matched by the index-pattern extractor may slip through; the guard
                    // keeps any such miss from poisoning the cluster and FORCED_STANDARD_DATASETS makes
                    // the coverage loss visible.
                    FORCED_STANDARD_DATASETS.add(dataset.indexName());
                    logger.warn("columnar: [{}] is time-series; keeping original mode", dataset.indexName());
                    return settings;
                }
                if (COLUMNAR_INCOMPATIBLE_DATASETS.contains(dataset.indexName())) {
                    // These datasets produce wrong results by design (geo_point precision, store:true,
                    // keyword normalizers, etc.) and are excluded at generation time. routing_path is
                    // not involved. See COLUMNAR_INCOMPATIBLE_DATASETS for per-dataset reasons.
                    FORCED_STANDARD_DATASETS.add(dataset.indexName());
                    logger.warn("columnar: [{}] is incompatible; keeping original mode", dataset.indexName());
                    return settings;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return Settings.builder().put(settings).put(extraSettings).build();
        }

        @Override
        public String transformDocument(CsvTestsDataLoader.TestDataset dataset, String originalDocumentJson) {
            return originalDocumentJson;
        }

        @Override
        public TransformedQuery transformQuery(String testId, CsvTestCase testCase) {
            String skipReason = testCase.skipColumnar;
            if (skipReason != null && skipReason.isBlank() == false) {
                SILENCED_COUNTS_BY_REASON.computeIfAbsent(skipReason, k -> new AtomicInteger()).incrementAndGet();
                SILENCED_COUNT.incrementAndGet();
                logger.info("columnar: silenced [{}]: {}", testId, skipReason);
                throw new StacklessAssumptionViolatedException(
                    String.format(Locale.ROOT, "columnar known limitation [%s]: %s", testId, skipReason)
                );
            }
            LAUNCHED_COUNT.incrementAndGet();
            return new TransformedQuery(testCase.query, Settings.EMPTY);
        }

        @Override
        public CsvTestUtils.ExpectedResults transformExpectedResults(
            String testId,
            CsvTestCase testCase,
            CsvTestUtils.ExpectedResults expected
        ) {
            return expected;
        }

        /**
         * Multi-value fields in columnar mode are returned in source insertion order rather than
         * the doc-values order (sorted) that standard mode produces. Since ESQL does not guarantee
         * multi-value ordering, we compare MV result lists as unordered sets so that the columnar
         * variant validates value presence without being sensitive to this storage-layer difference.
         */
        @Override
        public boolean ignoreValueOrder() {
            return true;
        }

        /**
         * Sanitizes a mapping JSON string for columnar index mode.
         *
         * <p>Performs three adjustments in a single parse-serialize pass:
         * <ol>
         *   <li>Removes the top-level {@code "runtime"} section.
         *       {@code IndexMode.COLUMNAR.validateMapping} calls
         *       {@code validateNoMappingRuntimeFields}, which rejects any mapping that declares
         *       runtime fields. The csv-spec fixtures do not currently use mapping runtime fields,
         *       but removing the section defensively ensures this variant stays robust as the
         *       fixtures evolve.</li>
         *   <li>Injects {@code "index": true} into every {@code dense_vector} field that declares
         *       {@code "similarity"} but omits {@code "index"}.
         *       Columnar mode defaults {@code index.mapping.index_disabled_by_default} to
         *       {@code true} (via {@code IndexSettings.java:1062} and
         *       {@code IndexMode.isStrictColumnar()}), which flips the dense_vector
         *       {@code "index"} default from {@code true} to {@code false}. A mapping that
         *       explicitly declares {@code "similarity"} (which is only legal when the field is
         *       indexed) then fails validation with "Field [similarity] can only be specified for
         *       a field of type [dense_vector] when it is indexed".  Restoring the standard-mode
         *       default explicitly keeps the mapping semantically equivalent to the one
         *       {@link CsvIT} creates, so the csv-spec oracle results remain valid.</li>
         *   <li>Injects {@code "norms": true} into every {@code text} field that omits
         *       {@code "norms"}.
         *       Columnar mode defaults norms off for text fields
         *       ({@code TextFieldMapper.java:338-345}, gated on
         *       {@code IndexMode.isColumnar()}), which causes BM25 length normalization to
         *       collapse to a constant. That produces different relevance scores for the same
         *       query, invalidating the csv-spec oracle. Restoring the standard-mode default
         *       makes BM25 scores bit-identical so all score-asserting specs remain valid.</li>
         * </ol>
         */
        private static String sanitizeMapping(String mapping) throws IOException {
            Map<String, Object> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, mapping, false);
            map.remove("runtime");
            fixDenseVectorIndexDefault(map);
            fixTextNormsDefault(map);
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                builder.map(map);
                return Strings.toString(builder);
            }
        }

        /**
         * Recursively walks every field definition reachable from {@code mappingObject} — via
         * {@code "properties"} (top-level and object/nested fields) and {@code "fields"}
         * (multi-fields) — and applies {@code fieldVisitor} to each one.
         */
        @SuppressWarnings("unchecked")
        private static void walkFieldDefs(
            Map<String, Object> mappingObject,
            java.util.function.Consumer<Map<String, Object>> fieldVisitor
        ) {
            for (String key : new String[] { "properties", "fields" }) {
                Object raw = mappingObject.get(key);
                if (raw instanceof Map<?, ?> == false) {
                    continue;
                }
                Map<String, Object> container = (Map<String, Object>) raw;
                for (Object fieldDefRaw : container.values()) {
                    if (fieldDefRaw instanceof Map<?, ?> == false) {
                        continue;
                    }
                    Map<String, Object> fieldDef = (Map<String, Object>) fieldDefRaw;
                    fieldVisitor.accept(fieldDef);
                    walkFieldDefs(fieldDef, fieldVisitor);
                }
            }
        }

        /**
         * Recursively walks the mapping and injects {@code "index": true} into every
         * {@code dense_vector} field that declares {@code "similarity"} without an explicit
         * {@code "index"} key.
         */
        private static void fixDenseVectorIndexDefault(Map<String, Object> mappingObject) {
            walkFieldDefs(mappingObject, fieldDef -> {
                if ("dense_vector".equals(fieldDef.get("type"))
                    && fieldDef.containsKey("similarity")
                    && fieldDef.get("similarity") != null
                    && fieldDef.containsKey("index") == false) {
                    fieldDef.put("index", true);
                }
            });
        }

        /**
         * Recursively walks the mapping and injects {@code "norms": true} into every
         * {@code text} field that does not already declare {@code "norms"}.
         *
         * <p>Columnar mode disables norms on text fields by default
         * ({@code TextFieldMapper.java:338-345}). That breaks BM25 scoring relative to the
         * csv-spec oracle. Restoring the standard-mode default keeps scores identical so
         * score-asserting specs remain valid. Multi-fields (reachable via {@code "fields"}) are
         * covered through {@link #walkFieldDefs}.
         */
        private static void fixTextNormsDefault(Map<String, Object> mappingObject) {
            walkFieldDefs(mappingObject, fieldDef -> {
                if ("text".equals(fieldDef.get("type")) && fieldDef.containsKey("norms") == false) {
                    fieldDef.put("norms", true);
                }
            });
        }
    }
}
