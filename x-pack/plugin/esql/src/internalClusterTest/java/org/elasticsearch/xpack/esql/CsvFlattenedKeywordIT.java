/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.junit.AssumptionViolatedException;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Integration test that runs the {@link CsvIT} csv-spec corpus against indices where every
 * field declared as {@code keyword} in the dataset's mapping has been rewritten to {@code flattened},
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
 * Opt-in: this test does not run by default. Pass {@code -Dtests.run_keyword_flattened_poc=true}
 * to enable it.
 *
 * <p>Limitations of this POC:</p>
 * <ul>
 *   <li>Only top-level and {@code properties}-nested keyword fields are converted; multi-field
 *       sub-fields under {@code fields} are left alone (see {@link KeywordToFlattenedTransformer}).</li>
 *   <li>Datasets whose keyword fields are referenced by special index modes (e.g. time-series
 *       dimensions) may fail at index creation time.</li>
 *   <li>Query rewriting is regex-driven; ES|QL grammar surfaces that take an attribute but not an
 *       arbitrary expression (e.g. {@code DISSECT &lt;field&gt;}, {@code LOOKUP JOIN ... ON &lt;field&gt;})
 *       will fail when the field is wrapped in a function call. Those failures are the intended
 *       output of the POC. See {@link EsqlQueryKeywordFieldRewriter} for the full list.</li>
 *   <li>The keyword-field set passed to the rewriter is the union of keyword paths across every
 *       dataset, not the set of keyword fields in the indices that this particular query actually
 *       reads from. False positives are possible if a non-keyword field happens to share its name
 *       with a keyword field from another dataset; for the POC this is an acceptable simplification.</li>
 *   <li>Output column types: {@code field_extract} is only injected in expression contexts, so a
 *       converted keyword field that is projected directly (e.g. {@code KEEP first_name},
 *       {@code SORT first_name}, or appearing untouched in the output of a STATS-less query)
 *       comes through with type {@code flattened}, while csv-spec expected results declare the
 *       column as {@code keyword}. These tests will fail with a column-type mismatch &mdash; that
 *       failure is the intended signal that those particular surfaces require an additional
 *       projection step (e.g. {@code EVAL field = field_extract(field, "v")} immediately before
 *       the projection) to fully recover keyword semantics. The POC stops short of synthesizing
 *       that extra projection.</li>
 * </ul>
 */
public class CsvFlattenedKeywordIT extends CsvIT {

    private static final Logger logger = LogManager.getLogger(CsvFlattenedKeywordIT.class);

    public static final String OPT_IN_PROPERTY = "tests.run_keyword_flattened_poc";

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
            "Set -D" + OPT_IN_PROPERTY + "=true to run the keyword→flattened POC variant of csv-spec tests",
            Boolean.getBoolean(OPT_IN_PROPERTY)
        );
    }

    /**
     * Strategy implementation that delegates the actual JSON manipulation to
     * {@link KeywordToFlattenedTransformer} and the query rewriting to
     * {@link EsqlQueryKeywordFieldRewriter}.
     * <p>
     * Two pieces of state are kept:
     * <ul>
     *   <li>{@code keywordPathsByIndex} &mdash; populated lazily as each index is loaded by
     *       {@link CsvIT}. Used by {@link #transformDocument} to know which keys to wrap in the
     *       source document of the index currently being indexed.</li>
     *   <li>{@code allKeywordPaths} &mdash; the union of keyword field paths across every
     *       {@link CsvTestsDataLoader#CSV_DATASET} entry, computed eagerly at construction time.
     *       This is the set passed to the query rewriter, because the query is rewritten before
     *       any index is necessarily loaded (and the rewriter does not parse the {@code FROM}
     *       clause to map back to specific indices).</li>
     * </ul>
     * The per-index map uses a {@link ConcurrentHashMap} because the test cluster may bulk-index
     * documents from multiple threads.
     */
    private static final class KeywordToFlattenedStrategy implements IndexLoadStrategy {
        private final ConcurrentMap<String, Set<String>> keywordPathsByIndex = new ConcurrentHashMap<>();
        private final Set<String> allKeywordPaths;

        KeywordToFlattenedStrategy() {
            this.allKeywordPaths = computeAllKeywordPaths();
        }

        /**
         * Walks every dataset's mapping resource, reuses
         * {@link KeywordToFlattenedTransformer#transformMapping(String)} purely to extract the
         * keyword field paths it would rewrite, and returns their union. The transformed mapping
         * itself is discarded here &mdash; the per-index transformation still runs lazily through
         * {@link #transformMapping} when {@link CsvIT} actually loads each index.
         */
        private static Set<String> computeAllKeywordPaths() {
            Set<String> all = new HashSet<>();
            for (CsvTestsDataLoader.TestDataset dataset : CsvTestsDataLoader.CSV_DATASET.values()) {
                if (dataset.mappingFileName() == null) {
                    continue;
                }
                try {
                    String mapping = CsvTestsDataLoader.readMappingFile(dataset);
                    KeywordToFlattenedTransformer.MappingResult result = KeywordToFlattenedTransformer.transformMapping(mapping);
                    all.addAll(result.keywordFieldPaths());
                } catch (IOException e) {
                    throw new UncheckedIOException("failed to read mapping for dataset [" + dataset.indexName() + "]", e);
                }
            }
            return Set.copyOf(all);
        }

        @Override
        public String transformMapping(CsvTestsDataLoader.TestDataset dataset, String originalMapping) throws IOException {
            KeywordToFlattenedTransformer.MappingResult result = KeywordToFlattenedTransformer.transformMapping(originalMapping);
            keywordPathsByIndex.put(dataset.indexName(), result.keywordFieldPaths());
            return result.transformedMapping();
        }

        @Override
        public String transformDocument(CsvTestsDataLoader.TestDataset dataset, String originalDocumentJson) throws IOException {
            Set<String> paths = keywordPathsByIndex.getOrDefault(dataset.indexName(), Set.of());
            return KeywordToFlattenedTransformer.wrapKeywordValuesAsFlattened(originalDocumentJson, paths);
        }

        @Override
        public String transformQuery(String originalQuery) {
            EsqlQueryKeywordFieldRewriter.RewriteResult result = EsqlQueryKeywordFieldRewriter.rewrite(
                originalQuery,
                allKeywordPaths,
                KeywordToFlattenedTransformer.WRAPPER_SUBKEY
            );
            if (result.modified() == false) {
                // Logged at INFO so the launched/skipped split is visible in the test JVM stdout for the POC,
                // alongside the assumption-violation message that surfaces in the JUnit XML <skipped> element.
                logger.info("keyword→flattened POC: skipping; no keyword field references in query");
                throw new AssumptionViolatedException(
                    "skipping: query references no keyword field that this variant rewrites to flattened, "
                        + "so re-running the spec would only re-test the unmodified behavior"
                );
            }
            // Same INFO level rationale: this line is the per-test "this test was launched" marker for the POC.
            // The thread context (added by the test runner) already carries the test method name, so a single
            // logger.info call here is enough to correlate the rewrite back to its csv-spec test.
            logger.info("keyword→flattened POC: launched; rewrote field references {}", result.rewrittenFieldNames());
            return result.rewrittenQuery();
        }
    }
}
