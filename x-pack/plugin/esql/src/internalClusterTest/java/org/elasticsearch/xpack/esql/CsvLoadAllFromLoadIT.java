/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.CsvTestUtils.TestResults;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.Assume.assumeTrue;

/**
 * Runs every csv-spec query that sets {@code unmapped_fields="load"} with {@code unmapped_fields="LOAD_ALL"} instead, expecting the
 * results of the original query.
 * <p>
 * {@code LOAD_ALL} does what {@code LOAD} does for the fields a query names, and returns the others, which {@code LOAD} leaves out, in
 * the extra {@code _unmapped_fields} column. So the query is not changed apart from the setting, and the column is ignored when the
 * results are compared.
 * <p>
 * A test that is skipped is listed below with the reason. Remove it from its list when {@code LOAD_ALL} supports what it needs, so that
 * it runs: the lists show what is left to support.
 */
public class CsvLoadAllFromLoadIT extends CsvIT {

    /**
     * Tests with {@code ROW}, which the command allow-list of {@code LOAD_ALL} does not have yet.
     * See <a href="https://github.com/elastic/elasticsearch/issues/164812">#164812</a>.
     */
    private static final Set<String> NEEDS_ROW = Set.of(
        "in_subquery.forkFoldableLHSUnmappedEvalLoadShouldNotLeakConst",
        "in_subquery.forkMarkJoinUnmappedEvalLoadShouldNotLeakMark",
        "in_subquery.unionAllFoldableLHSUnmappedEvalLoadShouldNotLeakConst",
        "in_subquery.unionAllMarkJoinUnmappedEvalLoadShouldNotLeakMark",
        "unmapped-load.loadUnmappedFieldReferencedOnlyAboveIndexAndRowUnion",
        "unmapped-load.loadViewRowConstantsLoadMode",
        "unmapped-load.loadViewRowEvalLoadMode"
    );

    /**
     * Tests with {@code TS}, which {@code LOAD_ALL} does not support.
     * See <a href="https://github.com/elastic/elasticsearch/issues/157190">#157190</a>.
     */
    private static final Set<String> NEEDS_TS = Set.of(
        "unmapped-load.evalOnMaxOverTime",
        "unmapped-load.explicitLastOverTimeUnmappedTsIndex",
        "unmapped-load.loadViewTsBranchingStatsLoadMode",
        "unmapped-load.loadViewTsMaxBytesLoadMode",
        "unmapped-load.maxOverTimeWithFiltering",
        "unmapped-load.unmappedNumericFromK8sSyntheticTsIndex",
        "unmapped-type-conflicts.tsPartiallyMappedNonKeywordFieldWithCast"
    );

    /**
     * Tests where {@code LOAD_ALL} is meant to differ: a field that one branch of a subquery names is loaded in that branch only by
     * {@code LOAD}, which null-fills it in the others, and in all of them by {@code LOAD_ALL}.
     */
    private static final Set<String> DIFFERS_BY_DESIGN = Set.of(
        "unmapped-load.fillNullAllColumnsInSubqueryBranchWithDifferentColumnsUnderLoad",
        "unmapped-load.loadSubquerySortRefInOneBranchTrailingWildcardKeepSurfacesUnmapped",
        "unmapped-load.loadUnmappedFieldInOneSubqueryBranchNoOuterKeep"
    );

    private static final String UNMAPPED_FIELDS_COLUMN = "_unmapped_fields";
    private static final Pattern LOAD_SETTING = Pattern.compile("(?i)(unmapped_fields\\s*=\\s*)\"load\"");
    /** The setting as a warning spells it, which may escape the quotes: it names the mode, which the rewrite changes. */
    private static final Pattern MODE_IN_WARNING = Pattern.compile("(?i)unmapped_fields\\s*=\\s*\\\\?\"(?:load_all|load)\\\\?\"");

    public CsvLoadAllFromLoadIT(
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
        return CsvIT.readScriptSpec()
            .stream()
            .filter(spec -> LOAD_SETTING.matcher(((CsvSpecReader.CsvTestCase) spec[4]).query).find())
            .toList();
    }

    @BeforeClass
    public static void useLoadAll() {
        differentialTestStrategy = new LoadAllStrategy();
    }

    private static final class LoadAllStrategy implements DifferentialTestStrategy {
        @Override
        public String transformMapping(CsvTestsDataLoader.TestDataset dataset, String originalMapping) throws IOException {
            return originalMapping;
        }

        @Override
        public Settings transformSettings(CsvTestsDataLoader.TestDataset dataset, Settings settings) {
            return settings;
        }

        @Override
        public String transformDocument(CsvTestsDataLoader.TestDataset dataset, String originalDocumentJson) throws IOException {
            return originalDocumentJson;
        }

        @Override
        public TransformedQuery transformQuery(String testId, CsvSpecReader.CsvTestCase testCase) {
            assumeTrue("LOAD_ALL does not support ROW yet", NEEDS_ROW.contains(testId) == false);
            assumeTrue("LOAD_ALL does not support TS", NEEDS_TS.contains(testId) == false);
            assumeTrue(
                "LOAD_ALL differs from LOAD by design here: it loads the field in the other branches too",
                DIFFERS_BY_DESIGN.contains(testId) == false
            );
            return new TransformedQuery(LOAD_SETTING.matcher(testCase.query).replaceAll("$1\"LOAD_ALL\""), Settings.EMPTY);
        }

        @Override
        public TestResults transformExpectedResults(String testId, CsvSpecReader.CsvTestCase testCase, TestResults expected) {
            return expected;
        }

        /**
         * A warning that names the setting says {@code "load"} in the spec and {@code "LOAD_ALL"} once the query is rewritten.
         */
        @Override
        public String normalizeWarning(String warning) {
            return MODE_IN_WARNING.matcher(warning).replaceAll("unmapped_fields=<mode>");
        }

        @Override
        public TestResults transformActualResults(
            String testId,
            CsvSpecReader.CsvTestCase testCase,
            TestResults expected,
            TestResults actual
        ) {
            int column = actual.columnNames().indexOf(UNMAPPED_FIELDS_COLUMN);
            if (column < 0 || expected.columnNames().contains(UNMAPPED_FIELDS_COLUMN)) {
                return actual;
            }
            List<String> names = new ArrayList<>(actual.columnNames());
            List<CsvTestUtils.Type> types = new ArrayList<>(actual.columnTypes());
            List<List<Object>> rows = new ArrayList<>(actual.values().size());
            names.remove(column);
            types.remove(column);
            for (List<Object> row : actual.values()) {
                List<Object> copy = new ArrayList<>(row);
                copy.remove(column);
                rows.add(copy);
            }
            return new TestResults(names, types, rows);
        }
    }
}
