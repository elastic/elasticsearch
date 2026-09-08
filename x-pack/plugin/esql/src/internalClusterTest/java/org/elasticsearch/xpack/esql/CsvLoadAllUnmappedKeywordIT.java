/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.esql.CsvTestUtils.ExpectedResults;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.BeforeClass;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Verifies that removing one {@code keyword} field from an index mapping ({@code dynamic=false}) and
 * prepending {@code SET unmapped_fields = "LOAD_ALL"} yields the same results as the original query,
 * because LOAD_ALL reads values from {@code _source}. Ten tests are selected randomly on every run.
 */
@TestLogging(value = "org.elasticsearch.xpack.esql.CsvLoadAllUnmappedKeywordIT:DEBUG", reason = "debug")
public class CsvLoadAllUnmappedKeywordIT extends CsvIT {

    private static final Logger logger = LogManager.getLogger(CsvLoadAllUnmappedKeywordIT.class);

    private static final int TEST_COUNT = 10;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Set by {@link #readScriptSpec()} before any {@code @BeforeClass} runs.
     * Maps index name → the keyword field name removed from that index for this run.
     */
    private static volatile Map<String, String> REMOVED_FIELDS_BY_INDEX = Map.of();

    // Anchored at start-of-statement (^ or after ;); captures the index list up to |, METADATA, or line end.
    private static final Pattern FROM_PATTERN = Pattern.compile(
        "(?i)(?:^|;)\\s*FROM\\s+([\\w*,\\s]+?)(?:\\s*(?:\\||METADATA|$))",
        Pattern.MULTILINE
    );

    // Detects SET unmapped_fields = "<value>" anywhere in the query.
    private static final Pattern UNMAPPED_FIELDS_PATTERN = Pattern.compile(
        "(?i)\\bSET\\s+unmapped_fields\\s*=\\s*\"?([\\w]+)\"?",
        Pattern.MULTILINE
    );

    // Detects an IN-subquery expression: IN ( FROM|ROW|SHOW ... ) — these produce SemiJoin / ROW nodes
    // that are not yet supported by unmapped_fields="LOAD_ALL".
    private static final Pattern IN_SUBQUERY_PATTERN = Pattern.compile("(?i)\\bIN\\s*\\(\\s*(FROM|ROW|SHOW)\\b");

    // ES|QL pipeline commands supported when unmapped_fields="LOAD_ALL" (FROM is always the source, not piped).
    private static final Set<String> LOAD_ALL_SUPPORTED_PIPE_COMMANDS = Set.of(
        "KEEP",
        "DROP",
        "RENAME",
        "EVAL",
        "WHERE",
        "SORT",
        "LIMIT",
        "STATS",
        "INLINE STATS",
        "FORK",
        "ENRICH",
        "LOOKUP JOIN"
    );

    private static final Pattern PIPE_COMMAND_PATTERN = Pattern.compile("(?i)\\|\\s*(\\w+)");

    public CsvLoadAllUnmappedKeywordIT(
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
     * Scans the full csv-spec corpus for eligible tests, selects {@value #TEST_COUNT} at random,
     * and populates {@link #REMOVED_FIELDS_BY_INDEX} with one keyword field to remove per index.
     *
     * <p>Eligibility criteria for a test:
     * <ul>
     *   <li>Uses a single {@code FROM} index with a known mapping that has keyword fields.</li>
     *   <li>Does not already set {@code unmapped_fields} to a non-DEFAULT value.</li>
     *   <li>Does not use the {@code optional_fields_load_all} capability (would be circular).</li>
     *   <li>Does not use external dataset sources.</li>
     *   <li>Has at least one keyword field in the index that can be safely removed — meaning the
     *       field either does not appear in the expected output, or appears with at least one
     *       non-null expected value (so LOAD_ALL can infer the type from {@code _source}).</li>
     * </ul>
     */
    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s", shuffle = false)
    public static List<Object[]> readScriptSpec() throws Exception {
        // LuceneTestCase#random() is not yet initialized at @ParametersFactory time; use a fixed seed
        // so the selection is deterministic and reproducible (same pattern as CsvIT.readScriptSpec).
        Random rng = new Random(0);
        List<Object[]> allSpecs = CsvIT.readScriptSpec();

        // eligible specs grouped by index name; LinkedHashMap preserves deterministic insertion order
        Map<String, List<SpecWithCandidates>> byIndex = new LinkedHashMap<>();

        for (Object[] spec : allSpecs) {
            CsvSpecReader.CsvTestCase testCase = (CsvSpecReader.CsvTestCase) spec[4];

            if (testCase.datasetSources.isEmpty() == false) {
                continue;
            }
            if (testCase.requiredCapabilities.contains("optional_fields_load_all")) {
                continue;
            }
            if (hasNonDefaultUnmappedFields(testCase)) {
                continue;
            }
            if (usesUnsupportedCommand(testCase.query)) {
                continue;
            }
            if (testCase.expectedResults == null || testCase.expectedResults.isBlank()) {
                continue;
            }

            String indexName = extractSingleFromIndex(testCase.query);
            if (indexName == null) {
                continue;
            }

            CsvTestsDataLoader.TestDataset dataset = CsvTestsDataLoader.CSV_DATASET.get(indexName);
            if (dataset == null || dataset.mappingFileName() == null) {
                continue;
            }

            String mappingJson;
            Set<String> keywordFields;
            try {
                // Skip datasets whose mapping files natively carry dynamic=false.
                // These are "partial mapping" datasets intentionally leaving many source
                // fields unmapped; applying LOAD_ALL loads all those extra fields and
                // produces more columns than the csv-spec expects.
                String rawMapping = dataset.loadMappings();
                JsonNode rawRoot = MAPPER.readTree(rawMapping);
                JsonNode rawDynamic = rawRoot.path("dynamic");
                boolean mappingFileDynamicFalse = (rawDynamic.isBoolean() && rawDynamic.asBoolean() == false)
                    || "false".equalsIgnoreCase(rawDynamic.asText(null));
                if (mappingFileDynamicFalse) {
                    continue;
                }

                mappingJson = CsvTestsDataLoader.readMappingFile(dataset);

                // Skip datasets whose CSV data contains top-level fields absent from the
                // explicit mapping. Our transformMapping sets dynamic=false; any field in
                // the CSV that is not explicitly mapped would become unmapped and LOAD_ALL
                // would infer an incorrect type for it (e.g., a long "id" field → keyword).
                if (dataset.dataFileName() != null && allCsvFieldsAreMapped(dataset, mappingJson) == false) {
                    continue;
                }

                keywordFields = extractKeywordFields(mappingJson);
            } catch (IOException e) {
                continue;
            }
            if (keywordFields.isEmpty()) {
                continue;
            }

            Set<String> candidateFields = new HashSet<>();
            try {
                ExpectedResults er = CsvTestUtils.loadCsvSpecValues(testCase.expectedResults);
                for (String field : keywordFields) {
                    if (isSafeToRemove(field, er)) {
                        candidateFields.add(field);
                    }
                }
            } catch (Exception e) {
                continue;
            }
            if (candidateFields.isEmpty()) {
                continue;
            }

            byIndex.computeIfAbsent(indexName, k -> new ArrayList<>()).add(new SpecWithCandidates(spec, indexName, candidateFields));
        }

        if (byIndex.isEmpty()) {
            throw new IllegalStateException("No eligible tests found for load_all unmapped keyword testing");
        }

        // For each index pick a random keyword field, then keep only specs compatible with that choice.
        List<SpecWithCandidates> pool = new ArrayList<>();
        Map<String, String> removedFields = new HashMap<>();

        for (Map.Entry<String, List<SpecWithCandidates>> entry : byIndex.entrySet()) {
            String indexName = entry.getKey();
            List<SpecWithCandidates> candidates = entry.getValue();

            Set<String> allFields = new HashSet<>();
            for (SpecWithCandidates sc : candidates) {
                allFields.addAll(sc.candidateFields);
            }

            // Sort before selecting so the rng gives reproducible results for the same seed.
            List<String> sortedFields = new ArrayList<>(allFields);
            Collections.sort(sortedFields);
            String chosenField = sortedFields.get(rng.nextInt(sortedFields.size()));

            List<SpecWithCandidates> compatible = candidates.stream().filter(sc -> sc.candidateFields.contains(chosenField)).toList();
            if (compatible.isEmpty() == false) {
                pool.addAll(compatible);
                removedFields.put(indexName, chosenField);
            }
        }

        if (pool.isEmpty()) {
            throw new IllegalStateException("No eligible tests remain after field selection");
        }

        Collections.shuffle(pool, rng);
        List<SpecWithCandidates> selected = pool.subList(0, Math.min(TEST_COUNT, pool.size()));

        // Trim to only the indices actually used by the selected tests.
        Set<String> selectedIndices = new HashSet<>();
        for (SpecWithCandidates sc : selected) {
            selectedIndices.add(sc.indexName);
        }
        Map<String, String> trimmed = new HashMap<>();
        for (Map.Entry<String, String> e : removedFields.entrySet()) {
            if (selectedIndices.contains(e.getKey())) {
                trimmed.put(e.getKey(), e.getValue());
            }
        }
        REMOVED_FIELDS_BY_INDEX = Collections.unmodifiableMap(trimmed);

        return selected.stream().map(sc -> sc.spec).toList();
    }

    @BeforeClass
    public static void setupLoadAllStrategy() {
        assumeTrue("Requires OPTIONAL_FIELDS_LOAD_ALL (snapshot-only)", EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL.isEnabled());
        logger.info("CsvLoadAllUnmappedKeywordIT: removed fields by index = {}", REMOVED_FIELDS_BY_INDEX);
        indexLoadStrategy = new LoadAllKeywordFieldStrategy();
    }

    @Override
    public boolean logResults() {
        return true;
    }

    // ---- Eligibility helpers ----

    /** Returns true if the query uses any pipeline command unsupported by {@code unmapped_fields="LOAD_ALL"}. */
    private static boolean usesUnsupportedCommand(String query) {
        // Reject IN-subquery expressions: they produce SemiJoin / ROW nodes not yet supported by LOAD_ALL.
        if (IN_SUBQUERY_PATTERN.matcher(query).find()) {
            return true;
        }
        Matcher m = PIPE_COMMAND_PATTERN.matcher(query);
        while (m.find()) {
            if (LOAD_ALL_SUPPORTED_PIPE_COMMANDS.contains(m.group(1).toUpperCase(Locale.ROOT)) == false) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasNonDefaultUnmappedFields(CsvSpecReader.CsvTestCase testCase) {
        String pragmaVal = testCase.pragmas.get("unmapped_fields");
        if (pragmaVal != null && "default".equals(pragmaVal) == false) {
            return true;
        }
        Matcher m = UNMAPPED_FIELDS_PATTERN.matcher(testCase.query);
        while (m.find()) {
            if ("default".equalsIgnoreCase(m.group(1)) == false) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the single index name from the query's {@code FROM} clause, or {@code null} if the
     * query has no FROM, uses multiple indices, uses a wildcard, or the index is not in
     * {@link CsvTestsDataLoader#CSV_DATASET}.
     */
    private static String extractSingleFromIndex(String query) {
        Matcher m = FROM_PATTERN.matcher(query);
        if (m.find() == false) {
            return null;
        }
        String indexList = m.group(1).trim();
        if (indexList.contains(",") || indexList.contains("*")) {
            return null;
        }
        return CsvTestsDataLoader.CSV_DATASET.containsKey(indexList) ? indexList : null;
    }

    /** Collects the names of top-level properties whose type is {@code keyword}. */
    private static Set<String> extractKeywordFields(String mappingJson) throws IOException {
        JsonNode root = MAPPER.readTree(mappingJson);
        JsonNode props = root.path("properties");
        if (props.isMissingNode()) {
            return Set.of();
        }
        Set<String> result = new HashSet<>();
        props.fields().forEachRemaining(entry -> {
            if ("keyword".equals(entry.getValue().path("type").asText(""))) {
                result.add(entry.getKey());
            }
        });
        return result;
    }

    /**
     * Returns {@code true} when every top-level field name found in the dataset's CSV header is also
     * present as a top-level property in {@code mappingJson}.
     *
     * <p>If any CSV field is absent from the mapping, setting {@code dynamic=false} in
     * {@link LoadAllKeywordFieldStrategy#transformMapping} would leave it unmapped, and LOAD_ALL
     * would then infer an incorrect type for it from {@code _source} (e.g., a {@code long} field
     * stored in the CSV as a bare integer is inferred as {@code keyword} by LOAD_ALL).
     */
    private static boolean allCsvFieldsAreMapped(CsvTestsDataLoader.TestDataset dataset, String mappingJson) {
        Set<String> mappingProps;
        try {
            JsonNode props = MAPPER.readTree(mappingJson).path("properties");
            if (props.isMissingNode()) {
                return true; // no properties → nothing to compare
            }
            mappingProps = new HashSet<>();
            props.fieldNames().forEachRemaining(mappingProps::add);
        } catch (IOException e) {
            return true; // can't parse → optimistically allow
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(dataset.streamData(), StandardCharsets.UTF_8))) {
            String headerLine = reader.readLine();
            if (headerLine == null) {
                return true;
            }
            // CSV data files are comma-separated; each cell is "fieldName:type".
            // Sub-fields use dot-notation (e.g., "languages.long:long"); extract only
            // the top-level name (the part before the first dot, before the colon).
            for (String cell : headerLine.split(",")) {
                String name = cell.trim();
                int colon = name.indexOf(':');
                if (colon >= 0) {
                    name = name.substring(0, colon).trim();
                }
                int dot = name.indexOf('.');
                if (dot >= 0) {
                    name = name.substring(0, dot);
                }
                if (name.isEmpty() == false && mappingProps.contains(name) == false) {
                    return false;
                }
            }
            return true;
        } catch (IOException e) {
            return true; // can't read → optimistically allow
        }
    }

    /**
     * Returns {@code true} if removing {@code fieldName} from the index mapping is safe for this test.
     * Safe means: the field does not appear in the expected output, OR it appears as a keyword column
     * with at least one non-null value so that LOAD_ALL can infer the type from {@code _source}.
     */
    private static boolean isSafeToRemove(String fieldName, ExpectedResults er) {
        int colIdx = er.columnNames().indexOf(fieldName);
        if (colIdx < 0) {
            return true;
        }
        if (er.columnTypes().get(colIdx) != CsvTestUtils.Type.KEYWORD) {
            return false;
        }
        for (List<Object> row : er.values()) {
            if (colIdx < row.size() && row.get(colIdx) != null) {
                return true;
            }
        }
        return false;
    }

    /**
     * Prepends {@code SET unmapped_fields = "LOAD_ALL"; } on the same line as the query to preserve
     * warning line numbers.  If the query already contains an {@code unmapped_fields} SET option, that
     * option is replaced in-place.  When the query has an unrelated leading SET clause the new statement
     * is simply prepended — ES|QL does not support comma-separated multi-setting SET syntax.
     */
    private static String buildQueryWithLoadAll(String originalQuery) {
        Matcher umfMatcher = UNMAPPED_FIELDS_PATTERN.matcher(originalQuery);
        if (umfMatcher.find()) {
            return originalQuery.substring(0, umfMatcher.start())
                + "SET unmapped_fields = \"LOAD_ALL\""
                + originalQuery.substring(umfMatcher.end());
        }
        return "SET unmapped_fields = \"LOAD_ALL\"; " + originalQuery.stripLeading();
    }

    // ---- Inner types ----

    private record SpecWithCandidates(Object[] spec, String indexName, Set<String> candidateFields) {}

    private static final class LoadAllKeywordFieldStrategy implements IndexLoadStrategy {

        @Override
        public String transformMapping(CsvTestsDataLoader.TestDataset dataset, String originalMapping) throws IOException {
            String removedField = REMOVED_FIELDS_BY_INDEX.get(dataset.indexName());
            if (removedField == null) {
                return originalMapping;
            }
            ObjectNode root = (ObjectNode) MAPPER.readTree(originalMapping);
            ObjectNode props = (ObjectNode) root.get("properties");
            if (props != null) {
                props.remove(removedField);
            }
            root.put("dynamic", false);
            return MAPPER.writeValueAsString(root);
        }

        @Override
        public Settings transformSettings(CsvTestsDataLoader.TestDataset dataset, Settings settings) {
            return settings;
        }

        @Override
        public String transformDocument(CsvTestsDataLoader.TestDataset dataset, String originalDocumentJson) {
            return originalDocumentJson;
        }

        @Override
        public IndexLoadStrategy.TransformedQuery transformQuery(String testId, CsvSpecReader.CsvTestCase testCase) {
            String modifiedQuery = buildQueryWithLoadAll(testCase.query);
            logger.debug("[{}] original query: {}", testId, testCase.query);
            logger.debug("[{}] load_all query: {}", testId, modifiedQuery);
            return new IndexLoadStrategy.TransformedQuery(modifiedQuery, Settings.EMPTY);
        }

        @Override
        public ExpectedResults transformExpectedResults(String testId, CsvSpecReader.CsvTestCase testCase, ExpectedResults expected) {
            logger.info("[{}] expected results (csv-spec):", testId);
            CsvTestUtils.logMetaData(expected.columnNames(), expected.columnTypes(), logger);
            CsvTestUtils.logData(expected.values().stream().map(row -> (Iterator<Object>) row.iterator()).iterator(), logger);
            return expected;
        }
    }
}
