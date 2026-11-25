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
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assume.assumeTrue;

/**
 * Runs every csv-spec query that reads a {@code flattened} field with {@code SET map_types=true}, expecting the results of the
 * original query.
 * <p>
 * The specs were written without the setting, so on their own they say nothing about map types. Run this way they are a differential
 * test: with the setting a query returns what it returns without it, unless the contract for map types says otherwise. Every spec
 * that differs is listed in {@link #DIFFERS_BY_RULE} with the rule of {@code MapKeys} it differs by, and has a spec of its own in
 * {@code flattened-map-types.csv-spec} for what it returns with the setting. A spec that differs and is not listed fails here: that
 * is a bug or missing work, not an expected difference.
 */
public class CsvMapTypesIT extends CsvIT {

    private static final String SET_MAP_TYPES = "SET map_types=true;";

    /**
     * The specs whose result with the setting differs from their result without it, and why. Keep the reason to the rule: a
     * difference that no rule explains does not belong here.
     */
    private static final Map<String, String> DIFFERS_BY_RULE = Map.ofEntries(
        // Rule 3: the join writes a map at [attributes], which replaces what was under that name. Without the setting the name
        // keeps reading the left side and a lookup index contributes no keys. With it: joinReplacesTheSubtreeOfAMap and
        // keysOfAMapFromALookupIndexKept in flattened-map-types.csv-spec.
        Map.entry("flattened-keys.keywordWithLookupJoin", "rule 3: the map from the lookup index replaces the column under its name"),
        Map.entry("flattened-keys.keyWithLookupJoin", "rule 3: the map from the lookup index replaces the keys under its name"),
        Map.entry("flattened-keys.keyOnlyTheLookupIndexHolds", "rule 1: a lookup index is a source and answers the name")
    );

    public CsvMapTypesIT(
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
        return CsvIT.readScriptSpec().stream().filter(spec -> readsFlattened(((CsvSpecReader.CsvTestCase) spec[4]).query)).toList();
    }

    private static boolean readsFlattened(String query) {
        return query.contains("flattened") && query.contains("map_types") == false;
    }

    @BeforeClass
    public static void useMapTypes() {
        assumeTrue("requires SET map_types", EsqlCapabilities.Cap.MAP_TYPES_SETTING.isEnabled());
        differentialTestStrategy = new MapTypesStrategy();
    }

    private static final class MapTypesStrategy implements DifferentialTestStrategy {
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
            String rule = DIFFERS_BY_RULE.get(testId);
            assumeTrue("differs with map types: " + rule, rule == null);
            return new TransformedQuery(SET_MAP_TYPES + " " + testCase.query, Settings.EMPTY);
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
            return actual;
        }
    }
}
