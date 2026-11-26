/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import com.carrotsearch.randomizedtesting.SeedUtils;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.CsvTestUtils.TestResults;
import org.elasticsearch.xpack.esql.CsvTestUtils.Type;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import static org.junit.Assume.assumeTrue;

/**
 * Appends {@code | FILLNULL <value> ON *} to a sample of the csv-spec queries and checks the result against the one the spec expects,
 * adjusted by what {@code FILLNULL} must do to it (elastic/elasticsearch#163195). Each sampled query runs with one of two fills:
 * <ul>
 *     <li>{@code NULL}, the explicit no-op. Nothing may change in the values, the column types or the warnings, whether or not the
 *     result has nulls.</li>
 *     <li>{@code DEFAULT}. A null in a column of a type that has a default ({@code integer}, {@code long}, {@code unsigned_long},
 *     {@code double}, {@code keyword}, {@code text} and {@code boolean}) becomes that default, a null in any other column stays, and
 *     a result with no nulls is unchanged. The columns without a default are listed in one added warning.</li>
 * </ul>
 * The query is never expected to fail, so any exception from {@code FILLNULL} fails the test.
 */
public class CsvFillNullIT extends CsvIT {

    private static final Logger logger = LogManager.getLogger(CsvFillNullIT.class);

    private static final int TEST_COUNT = 300;

    /** The first command of the query, after any {@code SET}. Others, such as {@code SHOW} and {@code EXPLAIN}, do not take a pipe. */
    private static final Pattern SOURCE_COMMAND_PATTERN = Pattern.compile("(?is)^\\s*(?:SET\\s[^;]*;\\s*)*(?:FROM|ROW|TS)\\b.*");

    /** An {@code INTO} command, which must be the last command, so {@code FILLNULL} cannot be appended after it. */
    private static final Pattern INTO_COMMAND_PATTERN = Pattern.compile("(?is)\\|\\s*INTO\\b");

    /** The most columns that one {@code FILLNULL} warning names, see {@code WarnUnfillableFillNull.MAX_FIELDS_IN_WARNING}. */
    private static final int MAX_FIELDS_IN_WARNING = 10;

    private enum Fill {
        NULL_NO_OP("NULL"),
        DEFAULT("DEFAULT");

        private final String keyword;

        Fill(String keyword) {
            this.keyword = keyword;
        }
    }

    /** The fill each sampled test runs with, by test id. */
    private static volatile Map<String, Fill> fillByTest = Map.of();

    public CsvFillNullIT(
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
        Map<String, Integer> excluded = new TreeMap<>();
        List<Object[]> pool = new ArrayList<>();
        int read = 0;
        for (Object[] spec : CsvIT.readScriptSpec()) {
            read++;
            String exclusion = exclusionReason((CsvSpecReader.CsvTestCase) spec[4]);
            if (exclusion != null) {
                excluded.merge(exclusion, 1, Integer::sum);
                continue;
            }
            pool.add(spec);
        }
        if (pool.isEmpty()) {
            throw new IllegalStateException("No eligible tests found for FILLNULL testing, excluded by reason " + excluded);
        }

        pool.sort(Comparator.comparing(spec -> spec[1] + "." + spec[2]));
        Collections.shuffle(pool, rng);
        List<Object[]> selected = pool.subList(0, Math.min(TEST_COUNT, pool.size()));

        Map<String, Fill> fills = new ConcurrentHashMap<>();
        for (Object[] spec : selected) {
            fills.put(spec[1] + "." + spec[2], Fill.values()[rng.nextInt(Fill.values().length)]);
        }
        fillByTest = fills;
        logger.info(
            "csv-spec tests read [{}], pool [{}], selected [{}]; excluded by reason {}",
            read,
            pool.size(),
            selected.size(),
            excluded
        );
        return List.copyOf(selected);
    }

    private static String exclusionReason(CsvSpecReader.CsvTestCase testCase) {
        if (testCase.datasetSources.isEmpty() == false) {
            return "dataset sources";
        }
        if (SOURCE_COMMAND_PATTERN.matcher(testCase.query).matches() == false) {
            return "does not start with FROM, ROW or TS";
        }
        if (testCase.query.toLowerCase(java.util.Locale.ROOT).contains("fillnull")) {
            return "already uses FILLNULL";
        }
        if (INTO_COMMAND_PATTERN.matcher(testCase.query).find()) {
            // INTO must be the last command, so appending "| FILLNULL ... ON *" after it would fail verification.
            return "uses INTO (must be the last command)";
        }
        if (testCase.expectedResults == null || testCase.expectedResults.isBlank()) {
            return "no expected results";
        }
        if (testCase.query.toLowerCase(java.util.Locale.ROOT).contains("approximation")) {
            // The confidence interval and certified columns are derived from the final value of the aggregate, so a FILLNULL that
            // changes a null count changes them too.
            return "uses query approximation";
        }
        if (hasMetadataColumn(testCase)) {
            // Whether a * fills a metadata column depends on the shape of the query: one that a subquery or a FORK declares is
            // filled and one that nothing declares is not, so the expected result cannot be derived from the spec alone.
            return "result has a metadata column";
        }
        return null;
    }

    private static boolean hasMetadataColumn(CsvSpecReader.CsvTestCase testCase) {
        String header = testCase.expectedResults.lines().findFirst().orElseThrow();
        for (String column : header.split("\\|")) {
            int separator = column.lastIndexOf(':');
            if (MetadataAttribute.isSupported((separator < 0 ? column : column.substring(0, separator)).trim())) {
                return true;
            }
        }
        return false;
    }

    @BeforeClass
    public static void setupFillNullStrategy() {
        assumeTrue("requires FILLNULL capability", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        differentialTestStrategy = new FillNullStrategy();
    }

    /**
     * The column types as the spec writes them. The loaded {@link Type} is not enough: it widens {@code counter_long} to {@code long},
     * but a counter has no default.
     */
    private static List<String> rawColumnTypes(CsvSpecReader.CsvTestCase testCase) {
        String header = testCase.expectedResults.lines().findFirst().orElseThrow();
        List<String> types = new ArrayList<>();
        for (String column : header.split("\\|")) {
            int separator = column.lastIndexOf(':');
            types.add(separator < 0 ? "" : column.substring(separator + 1).trim().toLowerCase(java.util.Locale.ROOT));
        }
        return types;
    }

    /** The type whose default {@code DEFAULT} fills a column of this spec type with, or {@code null} if the type has no default. */
    private static Type defaultedType(String rawType) {
        return switch (rawType) {
            case "integer", "short", "byte", "i" -> Type.INTEGER;
            case "long", "l" -> Type.LONG;
            case "unsigned_long", "ul" -> Type.UNSIGNED_LONG;
            case "double", "d" -> Type.DOUBLE;
            case "keyword", "k", "s", "string" -> Type.KEYWORD;
            case "text", "txt" -> Type.TEXT;
            case "boolean", "bool" -> Type.BOOLEAN;
            default -> null;
        };
    }

    /**
     * Whether a bare {@code *} leaves the column alone: the metadata columns and the synthetic {@code _unmapped_fields} column
     * that {@code unmapped_fields="LOAD_ALL"} adds are not user columns. That synthetic column is always of type {@code _source};
     * a user column that merely shares the name (for example from {@code CHANGE_POINT ... AS _unmapped_fields}, which has type
     * {@code keyword}) is an ordinary column that {@code *} does fill, so it is distinguished by type rather than name alone.
     */
    private static boolean leftAloneByAllFields(String name, String rawType) {
        return MetadataAttribute.isSupported(name) || (name.equals("_unmapped_fields") && rawType.equals("_source"));
    }

    private static Object defaultValue(Type type) {
        return switch (type) {
            case INTEGER, LONG, UNSIGNED_LONG, DOUBLE -> type.convert("0");
            case KEYWORD, TEXT -> type.convert("");
            case BOOLEAN -> type.convert("false");
            default -> throw new IllegalArgumentException("no default for [" + type + "]");
        };
    }

    private static final class FillNullStrategy implements DifferentialTestStrategy {
        @Override
        public String transformMapping(CsvTestsDataLoader.TestDataset dataset, String originalMapping) {
            return originalMapping;
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
        public TransformedQuery transformQuery(String testId, CsvSpecReader.CsvTestCase testCase) {
            Fill fill = fillByTest.get(testId);
            assumeTrue("not sampled for FILLNULL", fill != null);
            if (fill == Fill.DEFAULT) {
                expectUnfillableWarning(testCase);
            }
            return new TransformedQuery(testCase.query + "\n| FILLNULL " + fill.keyword + " ON *", Settings.EMPTY);
        }

        /**
         * {@code DEFAULT ON *} leaves the columns whose type has no default unchanged and says so in one warning, whether or not they
         * hold nulls. The metadata columns are not targeted by {@code *}.
         */
        private static void expectUnfillableWarning(CsvSpecReader.CsvTestCase testCase) {
            TestResults loaded = CsvTestUtils.loadCsvSpecValues(testCase.expectedResults);
            List<String> rawTypes = rawColumnTypes(testCase);
            assumeTrue("could not read the column types of the spec", rawTypes.size() == loaded.columnNames().size());
            List<String> unfillable = new ArrayList<>();
            for (int i = 0; i < rawTypes.size(); i++) {
                String name = loaded.columnNames().get(i);
                // A column of type unsupported, as when the indices disagree on its type, is left unchanged without a warning, see
                // https://github.com/elastic/elasticsearch/issues/167575.
                if (defaultedType(rawTypes.get(i)) == null
                    && leftAloneByAllFields(name, rawTypes.get(i)) == false
                    && rawTypes.get(i).equals("unsupported") == false) {
                    unfillable.add(name);
                }
            }
            if (unfillable.isEmpty()) {
                return;
            }
            StringJoiner names = new StringJoiner(", ");
            unfillable.stream().limit(MAX_FIELDS_IN_WARNING).forEach(names::add);
            String suffix = unfillable.size() > MAX_FIELDS_IN_WARNING
                ? "; only the first " + MAX_FIELDS_IN_WARNING + " of " + unfillable.size() + " fields are shown"
                : "";
            // FILLNULL is on the line after the original query, and starts after the "| " that precedes it.
            int line = (int) testCase.query.chars().filter(c -> c == '\n').count() + 2;
            String warning = "Line "
                + line
                + ":3: [FILLNULL] the following fields have no default fill value for their type and were left unchanged: ["
                + names
                + "]; provide an explicit value"
                + suffix;
            if (testCase.expectedWarningsRegex.isEmpty() == false) {
                // A spec that declares regex warnings is asserted as an allow-list (AssertWarnings.AllowedRegexes): an added
                // pattern is permitted but not required to appear, so a regression that dropped the FILLNULL warning would
                // still pass. The harness offers no way to also require one extra warning, so skip this combination rather
                // than assert it weakly; the DEFAULT warning is still asserted exactly on the many specs without regex warnings.
                assumeTrue("cannot require the FILLNULL warning on a spec that declares regex warnings", false);
            }
            // transformQuery runs once per iteration on the shared CsvTestCase, so guard against adding the same warning twice
            // under -Dtests.iters, which would make the exact-warning assertion expect two copies while the query emits one.
            if (testCase.expectedWarnings.contains(warning) == false) {
                testCase.expectedWarnings.add(warning);
            }
        }

        @Override
        public TestResults transformExpectedResults(String testId, CsvSpecReader.CsvTestCase testCase, TestResults expected) {
            if (fillByTest.get(testId) != Fill.DEFAULT) {
                return expected;
            }
            List<String> rawTypes = rawColumnTypes(testCase);
            assumeTrue("could not read the column types of the spec", rawTypes.size() == expected.columnNames().size());
            List<List<Object>> rows = new ArrayList<>(expected.values().size());
            for (List<Object> row : expected.values()) {
                List<Object> filled = new ArrayList<>(row);
                for (int i = 0; i < filled.size(); i++) {
                    Type type = leftAloneByAllFields(expected.columnNames().get(i), rawTypes.get(i))
                        ? null
                        : defaultedType(rawTypes.get(i));
                    if (filled.get(i) == null && type != null) {
                        filled.set(i, defaultValue(type));
                    }
                }
                rows.add(filled);
            }
            return new TestResults(expected.columnNames(), expected.columnTypes(), rows);
        }
    }
}
