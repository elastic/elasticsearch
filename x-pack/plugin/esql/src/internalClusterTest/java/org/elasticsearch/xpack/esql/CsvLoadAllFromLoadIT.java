/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.CsvTestUtils.ExpectedResults;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assume.assumeTrue;

/**
 * Runs every csv-spec query that sets {@code unmapped_fields="load"} with {@code unmapped_fields="LOAD_ALL"} instead, expecting the
 * results of the original query. See {@link LoadAllFromLoad}, which also lists the tests that are skipped.
 */
public class CsvLoadAllFromLoadIT extends CsvIT {

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
            .filter(spec -> LoadAllFromLoad.usesLoad(((CsvSpecReader.CsvTestCase) spec[4]).query))
            .toList();
    }

    @BeforeClass
    public static void useLoadAll() {
        indexLoadStrategy = new LoadAllStrategy();
    }

    private static final class LoadAllStrategy implements IndexLoadStrategy {
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
            String skipReason = LoadAllFromLoad.skipReason(testId);
            assumeTrue(skipReason, skipReason == null);
            return new TransformedQuery(LoadAllFromLoad.toLoadAll(testCase.query), Settings.EMPTY);
        }

        @Override
        public ExpectedResults transformExpectedResults(String testId, CsvSpecReader.CsvTestCase testCase, ExpectedResults expected) {
            return expected;
        }

        /**
         * A warning that names the setting says {@code "load"} in the spec and {@code "LOAD_ALL"} once the query is rewritten.
         */
        @Override
        public String normalizeWarning(String warning) {
            return LoadAllFromLoad.withoutMode(warning);
        }

        @Override
        public ExpectedResults transformActualResults(
            String testId,
            CsvSpecReader.CsvTestCase testCase,
            ExpectedResults expected,
            ExpectedResults actual
        ) {
            int column = actual.columnNames().indexOf(LoadAllFromLoad.UNMAPPED_FIELDS_COLUMN);
            if (column < 0 || expected.columnNames().contains(LoadAllFromLoad.UNMAPPED_FIELDS_COLUMN)) {
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
            return new ExpectedResults(names, types, rows);
        }
    }
}
