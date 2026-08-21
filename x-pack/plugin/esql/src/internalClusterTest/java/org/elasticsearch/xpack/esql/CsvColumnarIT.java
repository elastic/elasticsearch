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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.CSV_DATASET;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.VIEW_CONFIGS;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.isLookupDataset;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.isTimeSeries;

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
        // dense_vector fields with a "similarity" attribute require the field to be indexed
        // (index:true), but columnar mode rejects indexed dense_vector fields because they cannot
        // be reconstructed from doc values — MapperParsingException: "Field [similarity] can only
        // be specified for a field of type [dense_vector] when it is indexed".
        "dense_vector",
        "dense_vector_unmapped",
        "dense_vector_text",
        "dense_vector_coalesce",
        "dense_vector_bfloat16",
        "dense_vector_arithmetic",
        // color datasets use dense_vector fields with similarity (rgb_vector field).
        "colors",
        "colors_with_slice",
        "colors_unmapped",
        // Uses dense_vector field with similarity for MMR re-ranking queries.
        "mmr_text_vector_keyword",
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
     * Names of ES views that wrap time-series ({@code TS}) subqueries and are therefore
     * incompatible with columnar mode for the same reason as their backing {@code k8s} dataset:
     * columnar mode rejects {@code index.routing_path} settings, which are required by the
     * underlying time-series indices.
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

    private static boolean shouldExclude(CsvTestCase testCase) throws IOException {
        // Tests with a per-test skip directive are kept so the reason shows up in JUnit XML.
        if (testCase.skipColumnar != null) {
            return false;
        }
        // Check extracted patterns for: known-incompatible datasets, time-series datasets,
        // and time-series-backed views (which EsqlQueryDatasetResolver cannot resolve as datasets).
        // The view check runs first because wildcard patterns (e.g. view_k8s_max_*) are not
        // resolvable to TestDataset entries and must be matched here against the known-view set.
        Set<String> patterns = EsqlQueryDatasetResolver.extractIndexPatterns(testCase.query);
        if (touchesTsBackedView(patterns)) {
            EXCLUDED_TIME_SERIES_COUNT.incrementAndGet();
            return true;
        }
        Set<CsvTestsDataLoader.TestDataset> datasets = EsqlQueryDatasetResolver.resolveDatasets(patterns, CSV_DATASET);
        for (CsvTestsDataLoader.TestDataset dataset : datasets) {
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
            "columnar summary: launched={} silenced={} incompatible-dataset={} time-series={}",
            launched,
            silenced,
            incompatible,
            timeSeries
        );
        SILENCED_COUNTS_BY_REASON.entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(e -> logger.info("columnar summary: silenced[{}]={}", e.getKey(), e.getValue().get()));
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
     *       lookup-mode datasets, which must stay in {@code lookup} mode for LOOKUP JOIN to work.</li>
     *   <li>Strips {@code store: true} from mappings (strict columnar rejects stored fields).</li>
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
            // Strict columnar modes reject mapping runtime fields.
            return stripRuntimeFields(originalMapping);
        }

        @Override
        public Settings transformSettings(CsvTestsDataLoader.TestDataset dataset, Settings settings) {
            try {
                if (isLookupDataset(dataset)) {
                    // Lookup mode is required for LOOKUP JOIN; leave untouched so join tests still run.
                    logger.debug("columnar: keeping lookup mode for dataset [{}]", dataset.indexName());
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
         * Removes the top-level {@code "runtime"} section from a mapping JSON string.
         *
         * <p>{@code IndexMode.COLUMNAR.validateMapping} calls
         * {@code validateNoMappingRuntimeFields}, which rejects any mapping that declares runtime
         * fields. The csv-spec fixtures do not currently use mapping runtime fields, but removing
         * the section defensively ensures this variant stays robust as the fixtures evolve.
         */
        private static String stripRuntimeFields(String mapping) throws IOException {
            Map<String, Object> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, mapping, false);
            // The runtime section lives at the top level of the mapping object.
            map.remove("runtime");
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                builder.map(map);
                return Strings.toString(builder);
            }
        }
    }
}
