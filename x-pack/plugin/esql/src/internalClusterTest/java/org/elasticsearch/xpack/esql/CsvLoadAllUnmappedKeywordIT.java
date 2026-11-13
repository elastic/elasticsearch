/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import com.carrotsearch.randomizedtesting.SeedUtils;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.CsvTestUtils.TestResults;
import org.junit.BeforeClass;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Runs csv-spec queries over an index whose mapping lost one keyword field ({@code dynamic: false}) with
 * {@code SET unmapped_fields="LOAD_ALL"}, expecting the results of the original query.
 */
public class CsvLoadAllUnmappedKeywordIT extends CsvIT {

    private static final Logger logger = LogManager.getLogger(CsvLoadAllUnmappedKeywordIT.class);

    private static final int TEST_COUNT = 300;
    private static final String UNMAPPED_FIELDS_COLUMN = "_unmapped_fields";
    private static final ObjectMapper MAPPER = JsonMapper.builder()
        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
        .disable(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES)
        .build();
    private static volatile Map<String, String> removedFieldsByIndex = Map.of();
    private static final Set<String> ENRICH_SOURCE_INDICES = CsvTestsDataLoader.ENRICH_POLICIES.values()
        .stream()
        .map(CsvTestsDataLoader.EnrichConfig::index)
        .collect(Collectors.toSet());

    private static final Pattern MAIN_FROM_PATTERN = Pattern.compile(
        "(?i)^\\s*(?:SET\\s[^;]*;\\s*)*FROM\\s+([\\w*,\\s]+?)\\s*(?:\\||METADATA|$)"
    );
    private static final Pattern UNMAPPED_FIELDS_PATTERN = Pattern.compile(
        "(?i)\\bSET\\s+unmapped_fields\\s*=\\s*\"?([\\w]+)\"?",
        Pattern.MULTILINE
    );
    private static final Pattern BRANCH_FROM_PATTERN = Pattern.compile("(?i)\\(\\s*FROM\\s+([\\w.*-]+)");
    private static final Pattern PROJECTING_COMMAND_PATTERN = Pattern.compile("(?i)[|(]\\s*(?:KEEP\\b(?!\\s*\\*\\s*(?:[|)]|$))|STATS\\b)");
    private static final Pattern UNSUPPORTED_COMMAND_PROBLEM_PATTERN = Pattern.compile(
        "line \\d+:\\d+(?: \\(in view \\[[^\\]]*]\\))?: unmapped_fields=\"LOAD_ALL\" only supports the FROM, .* and subquery commands; "
            + "\\[([^\\]]+)] is not supported yet"
    );
    private static final Pattern FULL_TEXT_FUNCTION_PATTERN = Pattern.compile(
        "(?i)\\b(KQL|QSTR|MATCH_PHRASE|MATCH)\\s*\\(|\\bMATCH\\b|\\w+\\s*:\\s*\""
    );
    private static final Pattern SOURCE_OR_UNMAPPED_COLUMN_PATTERN = Pattern.compile("\\b_source\\b|\\b_unmapped_fields\\b");
    private static final Pattern WARNING_POSITION_PATTERN = Pattern.compile("(?i)\\bline (?:\\d+|\\\\d\\+):(?:\\d+|\\\\d\\+)");

    /**
     * A failure tracked by a GitHub issue, ignored only while the test, index, removed field, exception type and whole message all match.
     */
    private record KnownError(
        String issue,
        String index,
        String removedField,
        Set<String> tests,
        Class<? extends Throwable> type,
        Pattern message
    ) {
        boolean matches(String testId, String testIndex, String testRemovedField, Throwable failure) {
            return tests.contains(testId)
                && index.equals(testIndex)
                && removedField.equals(testRemovedField)
                && failure.getClass() == type
                && failure.getMessage() != null
                && message.matcher(failure.getMessage()).matches();
        }
    }

    private record KnownWrongResult(
        String issue,
        String index,
        String removedField,
        String test,
        List<String> columns,
        List<List<Object>> rows
    ) {
        boolean matches(String testId, String testIndex, String testRemovedField, TestResults results) {
            return test.equals(testId)
                && index.equals(testIndex)
                && removedField.equals(testRemovedField)
                && columns.equals(results.columnNames())
                && rows.equals(results.values());
        }
    }

    private static final List<KnownError> KNOWN_ERRORS = List.of(
        // Remove when fixed: STATS ... BY an alias named like the unmapped field it reads fails with "Unknown column"
        new KnownError(
            "https://github.com/elastic/elasticsearch/issues/165752",
            "employees",
            "job_positions",
            Set.of("string.groupByMvMin", "string.groupByMvMax"),
            VerificationException.class,
            Pattern.compile("Found 1 problem\\nline \\d+:\\d+: Unknown column \\[job_positions]")
        ),
        // Remove when fixed: STATS grouping twice by the same name, one of them an unmapped field, fails with "optimized incorrectly"
        new KnownError(
            "https://github.com/elastic/elasticsearch/issues/165753",
            "employees",
            "gender",
            Set.of("stats.shadowingInternalWithGroup2"),
            IllegalStateException.class,
            Pattern.compile(
                "Found 1 problem\\nline \\d+:\\d+: Plan \\[Aggregate\\[\\[gender\\{f}#\\d+],\\[COUNT\\(x\\{r}#\\d+,true\\[BOOLEAN],"
                    + "PT0S\\[TIME_DURATION]\\) AS y#\\d+, x\\{r}#\\d+]]] optimized incorrectly due to missing references \\[x\\{r}#\\d+]"
            )
        ),
        // Remove when fixed: LOOKUP JOIN whose left join key is unmapped fails with "ambiguous reference"
        new KnownError(
            "https://github.com/elastic/elasticsearch/issues/165755",
            "multi_column_joinable",
            "name_str",
            Set.of(
                "lookup-join-expression.lookupJoinOnSameLeftFieldTwice",
                "lookup-join-expression.lookupJoinOnSameFieldTwiceWithOrNot",
                "lookup-join-expression.lookupJoinOnSameFieldWithPushableRightFilterAfter"
            ),
            VerificationException.class,
            Pattern.compile(
                "Found ambiguous reference to \\[other2]; matches any of \\[line \\d+:\\d+ \\[other2], line \\d+:\\d+ \\[other2]]"
            )
        )
    );

    private static final List<KnownWrongResult> KNOWN_WRONG_RESULTS = List.of(
        // Remove when fixed: WHERE (f IS NOT NULL OR ...) AND f IS NULL over an unmapped field returns every row whose field is null
        new KnownWrongResult(
            "https://github.com/elastic/elasticsearch/issues/165754",
            "employees",
            "gender",
            "null.isNullWithOrThreeBranches",
            List.of("emp_no", "gender", "salary"),
            employeesWithNullGender()
        )
    );

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

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s", shuffle = false)
    public static List<Object[]> readScriptSpec() throws Exception {
        Random rng = new Random(
            Optional.ofNullable(System.getProperty("tests.seed")).map(SeedUtils::parseSeed).orElseGet(System::nanoTime)
        );
        Map<String, Set<String>> removableFieldsByIndex = new HashMap<>();
        Map<String, List<Object[]>> specsByIndex = new TreeMap<>();
        Map<String, Integer> excluded = new TreeMap<>();
        int read = 0;

        for (Object[] spec : CsvIT.readScriptSpec()) {
            read++;
            CsvSpecReader.CsvTestCase testCase = (CsvSpecReader.CsvTestCase) spec[4];
            String exclusion = exclusionReason(testCase);
            String indexName = exclusion == null ? extractSingleFromIndex(testCase.query) : null;
            if (exclusion == null && indexName == null) {
                exclusion = "not a single FROM of a csv dataset";
            }
            if (exclusion == null
                && removableFieldsByIndex.computeIfAbsent(indexName, CsvLoadAllUnmappedKeywordIT::removableKeywordFields).isEmpty()) {
                exclusion = "index without a removable keyword field";
            }
            if (exclusion != null) {
                excluded.merge(exclusion, 1, Integer::sum);
                continue;
            }
            specsByIndex.computeIfAbsent(indexName, k -> new ArrayList<>()).add(spec);
        }

        Map<String, String> removedFields = new HashMap<>();
        List<Object[]> pool = new ArrayList<>();
        for (Map.Entry<String, List<Object[]>> entry : specsByIndex.entrySet()) {
            List<String> sortedFields = new ArrayList<>(removableFieldsByIndex.get(entry.getKey()));
            Collections.sort(sortedFields);
            removedFields.put(entry.getKey(), sortedFields.get(rng.nextInt(sortedFields.size())));
            pool.addAll(entry.getValue());
        }
        if (pool.isEmpty()) {
            throw new IllegalStateException(
                "No eligible tests found for load_all unmapped keyword testing, excluded by reason " + excluded
            );
        }

        pool.sort(Comparator.comparing(spec -> spec[1] + "." + spec[2]));
        Collections.shuffle(pool, rng);
        List<Object[]> selected = pool.subList(0, Math.min(TEST_COUNT, pool.size()));

        Map<String, String> used = new HashMap<>();
        for (Object[] spec : selected) {
            String indexName = extractSingleFromIndex(((CsvSpecReader.CsvTestCase) spec[4]).query);
            used.put(indexName, removedFields.get(indexName));
        }
        removedFieldsByIndex = Collections.unmodifiableMap(used);
        logger.info(
            "csv-spec tests read [{}], pool [{}], selected [{}]; excluded by reason {}",
            read,
            pool.size(),
            selected.size(),
            excluded
        );
        return List.copyOf(selected);
    }

    @BeforeClass
    public static void setupLoadAllStrategy() {
        logger.info("removed fields by index = {}", removedFieldsByIndex);
        differentialTestStrategy = new LoadAllKeywordFieldStrategy();
    }

    private static String exclusionReason(CsvSpecReader.CsvTestCase testCase) {
        if (testCase.datasetSources.isEmpty() == false) {
            return "dataset sources";
        }
        if (testCase.requiredCapabilities.stream().anyMatch(c -> c.startsWith("optional_fields_load_all"))) {
            return "optional_fields_load_all capability";
        }
        if (hasNonDefaultUnmappedFields(testCase)) {
            return "non-default unmapped_fields";
        }
        if (readsNonDatasetInBranch(testCase.query)) {
            return "branch FROM that is not a csv dataset";
        }
        if (FULL_TEXT_FUNCTION_PATTERN.matcher(testCase.query).find()) {
            return "full-text function";
        }
        if (SOURCE_OR_UNMAPPED_COLUMN_PATTERN.matcher(testCase.query).find()) {
            return "_source or _unmapped_fields in the query";
        }
        if (testCase.expectedResults == null || testCase.expectedResults.isBlank()) {
            return "no expected results";
        }
        return null;
    }

    private static boolean readsNonDatasetInBranch(String query) {
        Matcher branch = BRANCH_FROM_PATTERN.matcher(query);
        while (branch.find()) {
            if (CsvTestsDataLoader.CSV_DATASET.containsKey(branch.group(1)) == false) {
                return true;
            }
        }
        return false;
    }

    private static String unsupportedCommand(Throwable failure) {
        if (failure.getClass() != VerificationException.class || failure.getMessage() == null) {
            return null;
        }
        List<String> lines = failure.getMessage().lines().toList();
        if (lines.size() < 2 || lines.getFirst().matches("Found \\d+ problems?") == false) {
            return null;
        }
        String command = null;
        for (String problem : lines.subList(1, lines.size())) {
            Matcher unsupported = UNSUPPORTED_COMMAND_PROBLEM_PATTERN.matcher(problem);
            if (unsupported.matches() == false) {
                return null;
            }
            command = command == null ? unsupported.group(1) : command;
        }
        return command;
    }

    private static boolean projectsColumns(String query) {
        return PROJECTING_COMMAND_PATTERN.matcher(query).find();
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
        Matcher m = MAIN_FROM_PATTERN.matcher(query);
        if (m.find() == false) {
            return null;
        }
        String indexList = m.group(1).trim();
        if (indexList.contains(",") || indexList.contains("*")) {
            return null;
        }
        return CsvTestsDataLoader.CSV_DATASET.containsKey(indexList) ? indexList : null;
    }

    // Only a plain keyword nothing else in the mapping refers to has doc values equal to its _source once multi-values are sorted and
    // deduplicated; ignore_above, normalizers, multi-fields, aliases or copy_to break that equivalence.
    private static Set<String> removableKeywordFields(String indexName) {
        CsvTestsDataLoader.TestDataset dataset = CsvTestsDataLoader.CSV_DATASET.get(indexName);
        if (dataset == null
            || dataset.mappingFileName() == null
            || dataset.dataFileName() == null
            || ENRICH_SOURCE_INDICES.contains(indexName)) {
            return Set.of();
        }
        try {
            if (CsvTestsDataLoader.isLookupDataset(dataset)) {
                return Set.of();
            }
            JsonNode rawDynamic = MAPPER.readTree(dataset.loadMappings()).path("dynamic");
            if (rawDynamic.isMissingNode() == false && "false".equalsIgnoreCase(rawDynamic.asText())) {
                return Set.of();
            }
            JsonNode root = MAPPER.readTree(CsvTestsDataLoader.readMappingFile(dataset));
            JsonNode props = root.path("properties");
            if (props.isMissingNode() || root.has("_source") || allCsvFieldsAreMapped(dataset, props) == false) {
                return Set.of();
            }
            Set<String> referenced = new HashSet<>();
            collectTextValues(root, referenced);
            Set<String> result = new HashSet<>();
            props.fields().forEachRemaining(entry -> {
                JsonNode def = entry.getValue();
                if (def.size() == 1 && "keyword".equals(def.path("type").asText()) && referenced.contains(entry.getKey()) == false) {
                    result.add(entry.getKey());
                }
            });
            return result;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void collectTextValues(JsonNode node, Set<String> values) {
        if (node.isTextual()) {
            values.add(node.textValue());
        }
        node.forEach(child -> collectTextValues(child, values));
    }

    private static boolean allCsvFieldsAreMapped(CsvTestsDataLoader.TestDataset dataset, JsonNode props) throws IOException {
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
                if (name.isEmpty() == false && props.has(name) == false) {
                    return false;
                }
            }
            return true;
        }
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

    private static final class LoadAllKeywordFieldStrategy implements DifferentialTestStrategy {

        @Override
        public String transformMapping(CsvTestsDataLoader.TestDataset dataset, String originalMapping) throws IOException {
            String removedField = removedFieldsByIndex.get(dataset.indexName());
            if (removedField == null) {
                return originalMapping;
            }
            ObjectNode root = (ObjectNode) MAPPER.readTree(originalMapping);
            ((ObjectNode) root.get("properties")).remove(removedField);
            root.put("dynamic", false);
            return MAPPER.writeValueAsString(root);
        }

        @Override
        public Settings transformSettings(CsvTestsDataLoader.TestDataset dataset, Settings settings) {
            return settings;
        }

        @Override
        public String transformDocument(CsvTestsDataLoader.TestDataset dataset, String originalDocumentJson) throws IOException {
            String removedField = removedFieldsByIndex.get(dataset.indexName());
            if (removedField == null) {
                return originalDocumentJson;
            }
            ObjectNode root = (ObjectNode) MAPPER.readTree(originalDocumentJson);
            JsonNode value = root.get(removedField);
            if (value instanceof ArrayNode values && values.size() > 1) {
                TreeSet<String> sorted = new TreeSet<>(
                    (a, b) -> Arrays.compareUnsigned(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8))
                );
                values.forEach(v -> {
                    if (v.isNull() == false) {
                        sorted.add(v.asText());
                    }
                });
                ArrayNode replacement = root.putArray(removedField);
                sorted.forEach(replacement::add);
                return MAPPER.writeValueAsString(root);
            }
            if (value != null && (value.isNumber() || value.isBoolean())) {
                root.put(removedField, value.asText());
                return MAPPER.writeValueAsString(root);
            }
            return originalDocumentJson;
        }

        @Override
        public TransformedQuery transformQuery(String testId, CsvSpecReader.CsvTestCase testCase) {
            String modifiedQuery = buildQueryWithLoadAll(testCase.query);
            logger.debug("[{}] load_all query: {}", testId, modifiedQuery);
            return new TransformedQuery(modifiedQuery, Settings.EMPTY);
        }

        @Override
        public TestResults transformExpectedResults(String testId, CsvSpecReader.CsvTestCase testCase, TestResults expected) {
            return expected;
        }

        @Override
        public TestResults transformActualResults(
            String testId,
            CsvSpecReader.CsvTestCase testCase,
            TestResults expected,
            TestResults actual
        ) {
            TestResults rewritten = rewriteActualResults(testCase, expected, actual);
            String index = extractSingleFromIndex(testCase.query);
            String removedField = removedFieldsByIndex.get(index);
            for (KnownWrongResult known : KNOWN_WRONG_RESULTS) {
                if (known.matches(testId, index, removedField, rewritten)) {
                    throw knownIssue(known.issue(), testId, index, removedField, "returns wrong results");
                }
            }
            return rewritten;
        }

        @Override
        public void onFailure(String testId, CsvSpecReader.CsvTestCase testCase, Throwable failure) {
            String unsupported = unsupportedCommand(failure);
            if (unsupported != null) {
                throw new StacklessAssumptionViolatedException(
                    "skipping: unmapped_fields=\"LOAD_ALL\" does not support [" + unsupported + "] yet"
                );
            }
            String index = extractSingleFromIndex(testCase.query);
            String removedField = removedFieldsByIndex.get(index);
            for (KnownError known : KNOWN_ERRORS) {
                if (known.matches(testId, index, removedField, failure)) {
                    throw knownIssue(known.issue(), testId, index, removedField, "fails with " + failure.getClass().getSimpleName());
                }
            }
        }

        private TestResults rewriteActualResults(CsvSpecReader.CsvTestCase testCase, TestResults expected, TestResults actual) {
            String removedField = removedFieldsByIndex.get(extractSingleFromIndex(testCase.query));
            Pattern named = Pattern.compile("(?<![\\w*])" + Pattern.quote(removedField) + "(?![\\w*])");
            boolean namedInQuery = named.matcher(testCase.query).find();
            boolean rebuilt = false;
            List<String> names = new ArrayList<>(actual.columnNames());
            List<CsvTestUtils.Type> types = new ArrayList<>(actual.columnTypes());
            List<List<Object>> rows = new ArrayList<>(actual.values().size());
            actual.values().forEach(row -> rows.add(new ArrayList<>(row)));

            int unmappedColumn = names.indexOf(UNMAPPED_FIELDS_COLUMN);
            if (unmappedColumn >= 0 && types.get(unmappedColumn) == CsvTestUtils.Type.SOURCE) {
                List<Object> removedValues = new ArrayList<>(rows.size());
                boolean anyRemovedValue = false;
                for (List<Object> row : rows) {
                    Map<?, ?> object = (Map<?, ?>) row.get(unmappedColumn);
                    if (object != null && (object.size() > 1 || (object.size() == 1 && object.containsKey(removedField) == false))) {
                        return actual;
                    }
                    Object value = object == null ? null : asKeyword(object.get(removedField));
                    anyRemovedValue |= value != null;
                    removedValues.add(value);
                }
                boolean expectedColumn = expected.columnNames().contains(removedField);
                boolean alreadyAColumn = names.contains(removedField);
                if (anyRemovedValue && (expectedColumn == false || alreadyAColumn)) {
                    return actual;
                }
                names.remove(unmappedColumn);
                types.remove(unmappedColumn);
                rows.forEach(row -> row.remove(unmappedColumn));
                if (expectedColumn && alreadyAColumn == false) {
                    if (namedInQuery) {
                        return actual;
                    }
                    rebuilt = true;
                    names.add(removedField);
                    types.add(CsvTestUtils.Type.KEYWORD);
                    for (int i = 0; i < rows.size(); i++) {
                        rows.get(i).add(removedValues.get(i));
                    }
                }
            }

            int from = names.indexOf(removedField);
            int to = expected.columnNames().indexOf(removedField);
            if ((rebuilt || projectsColumns(testCase.query) == false) && from >= 0 && to >= 0 && from != to && to < names.size()) {
                names.add(to, names.remove(from));
                types.add(to, types.remove(from));
                rows.forEach(row -> row.add(to, row.remove(from)));
            }
            return new TestResults(names, types, rows);
        }

        @Override
        public String normalizeWarning(String warning) {
            return WARNING_POSITION_PATTERN.matcher(warning).replaceAll("line");
        }
    }

    private static List<Object> row(Object... cells) {
        return Arrays.asList(cells);
    }

    private static List<List<Object>> employeesWithNullGender() {
        CsvTestsDataLoader.TestDataset dataset = CsvTestsDataLoader.CSV_DATASET.get("employees");
        List<List<Object>> rows = new ArrayList<>();
        try {
            var documents = CsvTestsDataLoader.readCsvDocuments(dataset.streamData(), dataset.allowSubFields());
            for (CsvTestsDataLoader.Document document : documents) {
                JsonNode source = MAPPER.readTree(document.json().toString());
                if (source.has("gender") == false) {
                    rows.add(row(source.get("emp_no").intValue(), null, source.get("salary").intValue()));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        rows.sort(Comparator.comparing((List<Object> r) -> (Integer) r.get(0)));
        return rows;
    }

    private static StacklessAssumptionViolatedException knownIssue(
        String issue,
        String testId,
        String index,
        String removedField,
        String what
    ) {
        return new StacklessAssumptionViolatedException(
            "known issue " + issue + ": [" + testId + "] " + what + " when [" + index + "." + removedField + "] is removed from the mapping"
        );
    }

    private static Object asKeyword(Object sourceValue) {
        if (sourceValue instanceof List<?> list) {
            return list.size() == 1 ? String.valueOf(list.getFirst()) : list.stream().map(String::valueOf).toList();
        }
        return sourceValue == null ? null : String.valueOf(sourceValue);
    }
}
