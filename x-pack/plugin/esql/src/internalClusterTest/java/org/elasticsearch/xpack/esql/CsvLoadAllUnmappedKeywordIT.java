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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Verifies that removing one keyword field from an index mapping ({@code dynamic=false}) and prepending
 * {@code SET unmapped_fields = "LOAD_ALL"} yields the same results as the original query.
 */
@TestLogging(value = "org.elasticsearch.xpack.esql.CsvLoadAllUnmappedKeywordIT:DEBUG", reason = "debug")
public class CsvLoadAllUnmappedKeywordIT extends CsvIT {

    private static final Logger logger = LogManager.getLogger(CsvLoadAllUnmappedKeywordIT.class);

    private static final int TEST_COUNT = 300;
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static volatile Map<String, String> REMOVED_FIELDS_BY_INDEX = Map.of();
    private static volatile Set<String> SORT_SOURCE_INDICES_FOR_MV_FUNCTIONS_TESTS = Set.of();

    // captures the index list up to |, METADATA, or line end.
    private static final Pattern FROM_PATTERN = Pattern.compile(
        "(?i)(?:^|;)\\s*FROM\\s+([\\w*,\\s]+?)(?:\\s*(?:\\||METADATA|$))",
        Pattern.MULTILINE
    );
    private static final Pattern UNMAPPED_FIELDS_PATTERN = Pattern.compile(
        "(?i)\\bSET\\s+unmapped_fields\\s*=\\s*\"?([\\w]+)\"?",
        Pattern.MULTILINE
    );
    private static final Pattern IN_SUBQUERY_PATTERN = Pattern.compile("(?i)\\bIN\\s*\\(\\s*(FROM|ROW|SHOW)\\b");
    private static final Set<String> LOAD_ALL_SUPPORTED_PIPE_COMMANDS = Set.of("KEEP", "DROP", "RENAME", "EVAL", "WHERE", "SORT", "LIMIT");
    private static final Pattern PIPE_COMMAND_PATTERN = Pattern.compile("(?i)\\|\\s*(\\w+)");
    private static final Pattern LIMIT_BY_PATTERN = Pattern.compile("(?i)\\bLIMIT\\s+\\d+\\s+BY\\b");

    // Full-text search functions and the colon match operator fail on keyword-typed unmapped fields.
    // TODO: investigate further
    private static final Pattern FULL_TEXT_FUNCTION_PATTERN = Pattern.compile(
        "(?i)\\b(KQL|QSTR|MATCH_PHRASE|MATCH)\\s*\\(|\\bMATCH\\b|\\w+\\s*:\\s*\""
    );
    private static final Pattern ORDER_DEPENDENT_MV_PATTERN = Pattern.compile("(?i)\\bMV_(FIRST|LAST|SLICE|ZIP|CONCAT)\\s*\\(");
    private static final Pattern WARNING_POSITION_PATTERN = Pattern.compile("^Line \\S+:\\S+:");

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
     * Scans csv-spec files corpus for eligible tests, selects {@value #TEST_COUNT} at random,
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
        Random rng = new Random(0);
        List<Object[]> allSpecs = CsvIT.readScriptSpec();
        Map<String, List<SpecWithCandidates>> byIndex = new LinkedHashMap<>();

        for (Object[] spec : allSpecs) {
            CsvSpecReader.CsvTestCase testCase = (CsvSpecReader.CsvTestCase) spec[4];

            if (testCase.datasetSources.isEmpty() == false) {
                continue;
            }
            if (testCase.requiredCapabilities.contains("optional_fields_load_all_v2")) {
                continue;
            }
            if (hasNonDefaultUnmappedFields(testCase)) {
                continue;
            }
            if (usesUnsupportedCommand(testCase.query)) {
                continue;
            }
            // Full-text functions fail when the searched field is unmapped (LOAD_ALL makes it keyword-typed).
            // TODO: further investigation
            if (FULL_TEXT_FUNCTION_PATTERN.matcher(testCase.query).find()) {
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
                // Skip datasets whose mapping files natively carry dynamic=false
                String rawMapping = dataset.loadMappings();
                JsonNode rawRoot = MAPPER.readTree(rawMapping);
                JsonNode rawDynamic = rawRoot.path("dynamic");
                boolean mappingFileDynamicFalse = (rawDynamic.isBoolean() && rawDynamic.asBoolean() == false)
                    || "false".equalsIgnoreCase(rawDynamic.asText(null));
                if (mappingFileDynamicFalse) {
                    continue;
                }

                mappingJson = CsvTestsDataLoader.readMappingFile(dataset);
                if (dataset.dataFileName() != null && allCsvFieldsAreMapped(dataset, mappingJson) == false) {
                    continue;
                }

                keywordFields = extractKeywordFields(mappingJson);

                // Geo/shape/range fields store JSON objects in _source; LOAD_ALL expands each sub-key into a separate column,
                // producing more columns than the csv-spec expects.
                if (hasComplexTypeFields(mappingJson)) {
                    continue;
                }
            } catch (IOException e) {
                continue;
            }
            if (keywordFields.isEmpty()) {
                continue;
            }

            Set<String> candidateFields = new HashSet<>();
            try {
                ExpectedResults er = CsvTestUtils.loadCsvSpecValues(testCase.expectedResults);
                // Without a KEEP clause, all fields are returned in mapping-defined order. LOAD_ALL
                // may place the re-added field at a different position, causing column-name mismatches.
                boolean hasKeep = false;
                Matcher keepMatcher = PIPE_COMMAND_PATTERN.matcher(testCase.query);
                while (keepMatcher.find()) {
                    if ("KEEP".equalsIgnoreCase(keepMatcher.group(1))) {
                        hasKeep = true;
                        break;
                    }
                }
                for (String field : keywordFields) {
                    if (isSafeToRemove(field, er, hasKeep)) {
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

        // Only indices with at least one MV_ positional-function test need _source pre-sorting to match Lucene's keyword doc-values order.
        Set<String> sortIndices = new HashSet<>();
        for (SpecWithCandidates sc : selected) {
            CsvSpecReader.CsvTestCase tc = (CsvSpecReader.CsvTestCase) sc.spec[4];
            if (ORDER_DEPENDENT_MV_PATTERN.matcher(tc.query).find()) {
                sortIndices.add(sc.indexName);
            }
        }
        SORT_SOURCE_INDICES_FOR_MV_FUNCTIONS_TESTS = Collections.unmodifiableSet(sortIndices);

        return selected.stream().map(sc -> sc.spec).toList();
    }

    @BeforeClass
    public static void setupLoadAllStrategy() {
        assumeTrue("Requires OPTIONAL_FIELDS_LOAD_ALL (snapshot-only)", EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL_V2.isEnabled());
        logger.info("CsvLoadAllUnmappedKeywordIT: removed fields by index = {}", REMOVED_FIELDS_BY_INDEX);
        indexLoadStrategy = new LoadAllKeywordFieldStrategy();
    }

    @Override
    public boolean logResults() {
        return true;
    }

    /** Returns true if the query uses any pipeline command unsupported by {@code unmapped_fields="LOAD_ALL"}. */
    private static boolean usesUnsupportedCommand(String query) {
        if (IN_SUBQUERY_PATTERN.matcher(query).find()) {
            return true;
        }
        if (LIMIT_BY_PATTERN.matcher(query).find()) {
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

    private static boolean allCsvFieldsAreMapped(CsvTestsDataLoader.TestDataset dataset, String mappingJson) {
        Set<String> mappingProps;
        try {
            JsonNode props = MAPPER.readTree(mappingJson).path("properties");
            if (props.isMissingNode()) {
                return true;
            }
            mappingProps = new HashSet<>();
            props.fieldNames().forEachRemaining(mappingProps::add);
        } catch (IOException e) {
            return true;
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(dataset.streamData(), StandardCharsets.UTF_8))) {
            String headerLine = reader.readLine();
            if (headerLine == null) {
                return true;
            }

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

    private static boolean isSafeToRemove(String fieldName, ExpectedResults er, boolean hasKeep) {
        int colIdx = er.columnNames().indexOf(fieldName);
        if (colIdx < 0) {
            return true;
        }
        if (hasKeep == false) {
            return false;
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

    private static String buildQueryWithLoadAll(String originalQuery) {
        Matcher umfMatcher = UNMAPPED_FIELDS_PATTERN.matcher(originalQuery);
        if (umfMatcher.find()) {
            return originalQuery.substring(0, umfMatcher.start())
                + "SET unmapped_fields = \"LOAD_ALL\""
                + originalQuery.substring(umfMatcher.end());
        }
        return "SET unmapped_fields = \"LOAD_ALL\"; " + originalQuery.stripLeading();
    }

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
            if (SORT_SOURCE_INDICES_FOR_MV_FUNCTIONS_TESTS.contains(dataset.indexName()) == false) {
                return originalDocumentJson;
            }
            String chosenField = REMOVED_FIELDS_BY_INDEX.get(dataset.indexName());
            if (chosenField == null) {
                return originalDocumentJson;
            }
            try {
                if (MAPPER.readTree(originalDocumentJson) instanceof ObjectNode root) {
                    JsonNode fieldNode = root.get(chosenField);
                    if (fieldNode != null && fieldNode.isArray() && fieldNode.size() > 1) {
                        List<String> values = new ArrayList<>(fieldNode.size());
                        fieldNode.forEach(n -> {
                            String v = n.textValue();
                            if (v != null) values.add(v);
                        });
                        values.sort(
                            (a, b) -> Arrays.compareUnsigned(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8))
                        );
                        var arr = root.putArray(chosenField);
                        values.forEach(arr::add);
                        return MAPPER.writeValueAsString(root);
                    }
                }
            } catch (IOException e) {
                logger.debug("transformDocument: failed to sort _source array for field [{}]", chosenField, e);
            }
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
            CsvTestUtils.logData(expected.values().stream().map(row -> row.iterator()).iterator(), logger);
            List<List<Object>> sorted = sortKeywordMultiValues(expected.columnNames(), expected.columnTypes(), expected.values());
            return sorted == expected.values() ? expected : new ExpectedResults(expected.columnNames(), expected.columnTypes(), sorted);
        }

        @Override
        public List<List<Object>> transformActualResults(
            String testId,
            CsvSpecReader.CsvTestCase testCase,
            ExpectedResults expected,
            List<List<Object>> actualValues
        ) {
            return sortKeywordMultiValues(expected.columnNames(), expected.columnTypes(), actualValues);
        }

        @Override
        public String normalizeWarning(String warning) {
            return WARNING_POSITION_PATTERN.matcher(warning).replaceFirst("");
        }

        @Override
        public void adjustTestCaseForWarnings(CsvSpecReader.CsvTestCase testCase) {
            if (testCase.expectedWarnings.isEmpty() && testCase.expectedWarningsRegex.isEmpty()) {
                testCase.expectedWarningsRegexString.add(".*");
                testCase.expectedWarningsRegex.add(Pattern.compile(".*", Pattern.DOTALL));
            }
        }
    }

    /**
     * Returns a new row list where multi-value lists in keyword-type columns are sorted to canonical lexicographic order.
     * When LOAD_ALL reads from {@code _source}, values arrive in ingestion order, which may differ from the Lucene doc-values
     * order the csv-spec expectations were written against.
     */
    private static List<List<Object>> sortKeywordMultiValues(
        List<String> columnNames,
        List<CsvTestUtils.Type> columnTypes,
        List<List<Object>> rows
    ) {
        List<Integer> keywordColIndices = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); i++) {
            CsvTestUtils.Type type = i < columnTypes.size() ? columnTypes.get(i) : null;
            if (type == CsvTestUtils.Type.KEYWORD || type == CsvTestUtils.Type.TEXT || type == CsvTestUtils.Type.SEMANTIC_TEXT) {
                keywordColIndices.add(i);
            }
        }
        if (keywordColIndices.isEmpty()) {
            return rows;
        }

        Comparator<Object> elementOrder = Comparator.nullsFirst(Comparator.comparing(Object::toString));
        List<List<Object>> result = new ArrayList<>(rows.size());
        for (List<Object> row : rows) {
            List<Object> newRow = null;
            for (int colIdx : keywordColIndices) {
                if (colIdx >= row.size()) {
                    continue;
                }
                Object value = row.get(colIdx);
                if (value instanceof List<?> list && list.size() > 1) {
                    if (newRow == null) {
                        newRow = new ArrayList<>(row);
                    }
                    @SuppressWarnings("unchecked")
                    List<Object> sortable = new ArrayList<>((List<Object>) list);
                    sortable.sort(elementOrder);
                    newRow.set(colIdx, sortable);
                }
            }
            result.add(newRow != null ? newRow : row);
        }
        return result;
    }

    private static boolean hasComplexTypeFields(String mappingJson) throws IOException {
        JsonNode props = MAPPER.readTree(mappingJson).path("properties");
        if (props.isMissingNode()) {
            return false;
        }
        for (JsonNode fieldDef : props) {
            String type = fieldDef.path("type").asText("");
            if (type.startsWith("geo_")
                || type.startsWith("cartesian_")
                || type.endsWith("_range")
                || "flattened".equals(type)
                || "nested".equals(type)) {
                return true;
            }
        }
        return false;
    }
}
