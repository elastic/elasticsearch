/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.CsvSpecReader;
import org.elasticsearch.xpack.esql.datasources.fixtures.FixtureDimensions;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class AbstractExternalSourceSpecTestCaseTests extends ESTestCase {

    /**
     * A resource naming a template whose rows ARE column-aligned. The tests below exercise where the key
     * lands in the JSON, not whether the dataset needs it, so they name a padded template to keep the
     * injection switched on; {@link #testTrimSpacesIsNotInjectedForADatasetThatPadsNothing} covers the
     * per-source decision itself.
     */
    private static final String PADDED = "{{employees}}.csv";

    public void testResolveLocalUriHandlesLiteralPath() {
        Path base = Paths.get("/tmp/fixtures").toAbsolutePath();
        String uri = AbstractExternalSourceSpecTestCase.resolveLocalUri(base, "standalone/employees.csv");
        assertTrue("expected file:// URI, was: " + uri, uri.startsWith("file:"));
        assertTrue("expected path tail in URI, was: " + uri, uri.endsWith("/standalone/employees.csv"));
    }

    public void testResolveLocalUriHandlesGlobInLeafSegment() {
        Path base = Paths.get("/tmp/fixtures").toAbsolutePath();
        String uri = AbstractExternalSourceSpecTestCase.resolveLocalUri(base, "multifile/*.csv");
        assertTrue("expected file:// URI, was: " + uri, uri.startsWith("file:"));
        assertTrue("expected glob to be preserved in URI, was: " + uri, uri.endsWith("/multifile/*.csv"));
    }

    public void testResolveLocalUriHandlesDoubleGlob() {
        Path base = Paths.get("/tmp/fixtures").toAbsolutePath();
        String uri = AbstractExternalSourceSpecTestCase.resolveLocalUri(base, "hive-partitioned/**/*.csv");
        assertTrue("expected file:// URI, was: " + uri, uri.startsWith("file:"));
        assertTrue("expected glob to be preserved in URI, was: " + uri, uri.endsWith("/hive-partitioned/**/*.csv"));
    }

    public void testResolveLocalUriHandlesGlobInFirstSegment() {
        Path base = Paths.get("/tmp/fixtures").toAbsolutePath();
        String uri = AbstractExternalSourceSpecTestCase.resolveLocalUri(base, "*.csv");
        assertTrue("expected file:// URI, was: " + uri, uri.startsWith("file:"));
        assertTrue("expected glob to be preserved in URI, was: " + uri, uri.endsWith("/*.csv"));
    }

    public void testResolveLocalUriHandlesQuestionMarkGlob() {
        Path base = Paths.get("/tmp/fixtures").toAbsolutePath();
        String uri = AbstractExternalSourceSpecTestCase.resolveLocalUri(base, "multifile/file?.csv");
        assertTrue("expected file:// URI, was: " + uri, uri.startsWith("file:"));
        assertTrue("expected glob to be preserved in URI, was: " + uri, uri.endsWith("/multifile/file?.csv"));
    }

    public void testInjectTrimSpacesAddsToNullWith() {
        assertEquals("{\"trim_spaces\": true}", AbstractExternalSourceSpecTestCase.injectTrimSpaces(null, PADDED, "csv"));
    }

    public void testInjectTrimSpacesAddsToEmptyObject() {
        assertEquals("{\"trim_spaces\": true}", AbstractExternalSourceSpecTestCase.injectTrimSpaces("{}", PADDED, "csv"));
        assertEquals("{\"trim_spaces\": true}", AbstractExternalSourceSpecTestCase.injectTrimSpaces("{ }", PADDED, "csv"));
    }

    public void testInjectTrimSpacesMergesIntoExistingOptions() {
        assertEquals(
            "{\"header_row\": false, \"trim_spaces\": true}",
            AbstractExternalSourceSpecTestCase.injectTrimSpaces("{\"header_row\": false}", PADDED, "csv")
        );
    }

    public void testInjectTrimSpacesLeavesExplicitTrimSpacesUntouched() {
        String withJson = "{\"trim_spaces\": false}";
        assertEquals(withJson, AbstractExternalSourceSpecTestCase.injectTrimSpaces(withJson, PADDED, "csv"));
    }

    public void testInjectTrimSpacesDoesNotFalseMatchAValue() {
        // "trim_spaces" appears only as a value here, so the injection must still fire.
        assertEquals(
            "{\"null_value\": \"trim_spaces\", \"trim_spaces\": true}",
            AbstractExternalSourceSpecTestCase.injectTrimSpaces("{\"null_value\": \"trim_spaces\"}", PADDED, "csv")
        );
    }

    /**
     * A declared schema is nested objects deep, so the trailing entry of a csv/tsv directive can be an OBJECT.
     * The injection walks back from the last brace, which is the outermost closer for a parser-guaranteed
     * single object -- so the key must land beside the declaration, never inside it, where the dataset PUT's
     * mappings parser would reject it as an unknown mappings field.
     */
    public void testInjectTrimSpacesLandsOutsideATrailingNestedObject() {
        assertEquals(
            "{\"mappings\": {\"properties\": {\"a\": {\"type\": \"keyword\"}}}, \"trim_spaces\": true}",
            AbstractExternalSourceSpecTestCase.injectTrimSpaces(
                "{\"mappings\": {\"properties\": {\"a\": {\"type\": \"keyword\"}}}}",
                PADDED,
                "csv"
            )
        );
    }

    /**
     * A declared column may be NAMED trim_spaces. Deciding the already-set check by matching the raw text would
     * see that nested key and skip the injection, reading the column-aligned csv/tsv fixtures untrimmed -- values
     * wrong, with nothing pointing at the cause. The setting is absent here, so the injection must fire.
     */
    public void testInjectTrimSpacesIgnoresASameNamedDeclaredColumn() {
        assertEquals(
            "{\"mappings\": {\"properties\": {\"trim_spaces\": {\"type\": \"keyword\"}}}, \"trim_spaces\": true}",
            AbstractExternalSourceSpecTestCase.injectTrimSpaces(
                "{\"mappings\": {\"properties\": {\"trim_spaces\": {\"type\": \"keyword\"}}}}",
                PADDED,
                "csv"
            )
        );
    }

    /** An explicitly-set trim_spaces SETTING is still left untouched, alongside a declared schema. */
    public void testInjectTrimSpacesLeavesAnExplicitSettingUntouchedBesideADeclaration() {
        String withJson = "{\"trim_spaces\": false, \"mappings\": {\"dynamic\": \"false\"}}";
        assertEquals(withJson, AbstractExternalSourceSpecTestCase.injectTrimSpaces(withJson, PADDED, "csv"));
    }

    /**
     * The read-key pin, which is the one guard whose failure is a GREEN test.
     *
     * <p>A fixture-bound slot announces itself to the reader through a read key: {@code mv_syntax} writes
     * bracket bytes and declares {@code multi_value_syntax}. Both the directive keys and the read keys are
     * injected, and injection leaves a key the case already declares untouched -- so a case pinning
     * {@code multi_value_syntax} crossed with a vector pinning brackets took the vector's BYTES and kept
     * the case's ANNOUNCEMENT. The reader parsed brackets while being told {@code none}, and passed.
     *
     * <p>No integration suite can catch that, because the symptom is a pass. The second assertion is the
     * regression proof: the directive settings ALONE do not pin this case, which is exactly the state the
     * fix moved away from, so reverting {@code vectorInjectedSettings} to {@code directiveSettings} turns
     * the first assertion red.
     */
    public void testTheReadKeyOfAFixtureBoundSlotPinsACaseThatDeclaresIt() {
        FixtureDimensions dimensions = FixtureDimensions.get();
        Map<String, String> vector = new LinkedHashMap<>();
        vector.put("format", "csv");
        vector.put("mv_syntax", "brackets");

        Map<String, String> injected = AbstractExternalSourceSpecTestCase.vectorInjectedSettings(dimensions, vector);
        assertThat("the read key is what reaches the dataset", injected, hasKey("multi_value_syntax"));
        assertThat("and it is not a directive key", dimensions.directiveSettings(vector), not(hasKey("multi_value_syntax")));

        Object[] baseTest = baseTestDeclaring("{\"multi_value_syntax\": \"none\"}");
        assertTrue(
            "a case declaring the read key must be filtered out of a vector that pins it",
            AbstractExternalSourceSpecTestCase.directivePins(baseTest, injected)
        );
        assertFalse(
            "regression proof: the directive settings alone do not see it, which is the bug this closed",
            AbstractExternalSourceSpecTestCase.directivePins(baseTest, dimensions.directiveSettings(vector))
        );
    }

    /**
     * The partition filter must ask {@link org.elasticsearch.xpack.esql.datasources.PartitionConfig#validate}
     * for every value of the dimension, not merely for the ones that reach the merged-config loop.
     *
     * <p>A case declaring only a {@code partition_path}, crossed with a {@code partition_detection=none}
     * vector, is rejected at registration -- the validator says the path is set while detection is
     * disabled. The {@code none} arm used to return before building the merged config, so the filter
     * answered "can carry" and the pair registered and 400'd at PUT.
     */
    public void testTheNoneArmStillAsksTheValidator() {
        Map<String, String> vector = new LinkedHashMap<>();
        vector.put("format", "csv");
        vector.put("partition_detection", "none");

        // The vector's own slot reaches the dataset through injection, so the filter must be handed the
        // set that actually gets injected -- the same set the crossing hands it.
        Map<String, String> injected = AbstractExternalSourceSpecTestCase.vectorInjectedSettings(FixtureDimensions.get(), vector);
        assertThat("partition_detection=none is injected, not implicit", injected, hasKey("partition_detection"));

        Object[] baseTest = baseTestDeclaring("{\"partition_path\": \"{bucket}\"}");
        assertTrue(
            "partition_path with detection disabled cannot carry, and only the validator knows that",
            AbstractExternalSourceSpecTestCase.partitionDetectionCannotCarry(baseTest, vector, injected)
        );
    }

    /** A case declaring nothing relevant carries fine under the same vector -- the filter is not blanket. */
    public void testTheNoneArmDoesNotFilterACaseItHasNoQuarrelWith() {
        Map<String, String> vector = new LinkedHashMap<>();
        vector.put("format", "csv");
        vector.put("partition_detection", "none");

        Map<String, String> injected = AbstractExternalSourceSpecTestCase.vectorInjectedSettings(FixtureDimensions.get(), vector);
        assertFalse(AbstractExternalSourceSpecTestCase.partitionDetectionCannotCarry(baseTestDeclaring("{}"), vector, injected));
    }

    private static Object[] baseTestDeclaring(String withJson) {
        CsvSpecReader.CsvTestCase testCase = new CsvSpecReader.CsvTestCase();
        testCase.datasetSources = List.of(new CsvSpecReader.DatasetSource("ds", "standalone/employees.csv", withJson));
        return new Object[] { "spec", "name", null, null, testCase };
    }

    /**
     * The pragma seam, which resolves the wrong way round and so needs the same gate as the directive seam.
     *
     * <p>{@code EsqlSpecTestCase.addPragmas} applies the vector's pragmas and then the case's, so a case
     * declaring the same key wins. Without this filter the pair registers and runs the case's value under
     * a vector name announcing the vector's -- green, and wrong about what ran.
     */
    public void testACaseDeclaringAPragmaIsFilteredFromAVectorThatPinsIt() {
        FixtureDimensions dimensions = FixtureDimensions.get();
        Map<String, String> vector = new LinkedHashMap<>();
        vector.put("format", "csv");
        vector.put("distribution", "round_robin");

        Map<String, String> pragmas = dimensions.pragmaSettings(vector, "csv");
        assertThat("the vector pins a pragma", pragmas, hasKey("external_distribution"));

        CsvSpecReader.CsvTestCase pinning = new CsvSpecReader.CsvTestCase();
        pinning.pragmas = Map.of("external_distribution", "coordinator_only");
        assertTrue(
            "a case pinning the same pragma must not run under this vector",
            AbstractExternalSourceSpecTestCase.pragmaPins(new Object[] { "spec", "name", null, null, pinning }, pragmas)
        );

        CsvSpecReader.CsvTestCase silent = new CsvSpecReader.CsvTestCase();
        assertFalse(
            "a case declaring no pragma carries fine",
            AbstractExternalSourceSpecTestCase.pragmaPins(new Object[] { "spec", "name", null, null, silent }, pragmas)
        );
    }

    /**
     * A dataset whose rows pad nothing gets no {@code trim_spaces}, and that is what reopens the glob cell.
     *
     * <p>The key is FORMAT-SPECIFIC, and elastic/esql-planning#1841 makes any dataset carrying one
     * unregisterable under a `?` glob -- so injecting it blanket cost csv and tsv the whole
     * {@code path_shape=glob} cell, for seven of ten datasets that never needed it. Which three pad is a
     * fact about the authored bytes, not a judgement: {@code checkFixturePadding} reads every canonical
     * CSV and fails when a declaration disagrees with the leading or trailing space a field actually
     * carries.
     *
     * <p>Paired with the padded control, because "nothing is injected anywhere" would pass the first
     * assertion alone while silently misparsing the three datasets that do pad.
     */
    public void testTrimSpacesIsNotInjectedForADatasetThatPadsNothing() {
        assertEquals(
            "an unpadded dataset must carry no format-specific key, or it cannot be registered under a glob",
            "{}",
            AbstractExternalSourceSpecTestCase.injectTrimSpaces("{}", "{{apps}}.csv", "csv")
        );
        assertEquals(
            "a padded dataset still needs it, or its rows misparse",
            "{\"trim_spaces\": true}",
            AbstractExternalSourceSpecTestCase.injectTrimSpaces("{}", "{{employees}}.csv", "csv")
        );
    }

    /** A resource naming no template at all keeps the injection, since nothing declares it unpadded. */
    public void testTrimSpacesIsInjectedWhenTheResourceNamesNoTemplate() {
        assertEquals(
            "{\"trim_spaces\": true}",
            AbstractExternalSourceSpecTestCase.injectTrimSpaces("{}", "s3://bucket/loose/file.csv", "csv")
        );
    }

    /**
     * The glob filter and the injector must answer the same question the same way, including where the
     * answer is awkward.
     *
     * <p>A resource naming no template gets {@code trim_spaces} -- nothing declares it unpadded, so the
     * injector falls through and adds the key. The filter used to re-derive that decision from the template
     * name and read a missing template as "nothing injected", which registered a pair the product then
     * rejects at CRUD. Every routed text resource names a template, so nothing went red; the disagreement
     * was invisible, which is the shape this whole gate exists to catch.
     *
     * <p>Asserted against the columnar arm too, because the fix must not close the cell for a format that
     * has no per-source injection at all -- that would trade a silent pass for silently lost coverage.
     */
    public void testTheGlobFilterAgreesWithTheInjectorOnATemplatelessResource() {
        FixtureDimensions dimensions = FixtureDimensions.get();
        Object[] templateless = baseTestDeclaring("{}");

        Map<String, String> text = new LinkedHashMap<>();
        text.put("format", "csv");
        text.put("path_shape", "glob");
        assertTrue(
            "the injector adds trim_spaces here, so the glob pair cannot register",
            AbstractExternalSourceSpecTestCase.globCannotCarryAFormatKey(dimensions, templateless, text, Map.of())
        );

        Map<String, String> columnar = new LinkedHashMap<>();
        columnar.put("format", "parquet");
        columnar.put("path_shape", "glob");
        assertFalse(
            "parquet injects nothing per source, so the same case carries fine",
            AbstractExternalSourceSpecTestCase.globCannotCarryAFormatKey(dimensions, templateless, columnar, Map.of())
        );

        Map<String, String> exact = new LinkedHashMap<>();
        exact.put("format", "csv");
        exact.put("path_shape", "exact");
        assertFalse(
            "and nothing is filtered away from a vector that is not a glob at all",
            AbstractExternalSourceSpecTestCase.globCannotCarryAFormatKey(dimensions, templateless, exact, Map.of())
        );
    }

    /**
     * A {@code path_shape} the case's sources cannot express must not register under a name that claims it.
     *
     * <p>{@code resolveTemplatePath} applies {@code pathShaped} on the standalone branch only; a multifile
     * or hive template resolves through the layout's own glob and never reads the dimension. Without this
     * filter the pair runs the exact-path bytes and reports {@code path_shape=glob} -- a silent pass whose
     * announcement is the false half, rather than the configuration.
     *
     * <p>The mixed case is the one that matters: a case reading one standalone source and one multifile
     * source DOES differ from its exact twin, because the standalone half reaches the listing path. A
     * filter keyed on "any non-standalone source" would discard it and call that a fix.
     */
    public void testAShapeNoSourceCanExpressDoesNotRegister() {
        FixtureDimensions dimensions = FixtureDimensions.get();
        Map<String, String> glob = new LinkedHashMap<>();
        glob.put("format", "csv");
        glob.put("path_shape", "glob");

        assertTrue(
            "a multifile source resolves through its layout glob and never reads path_shape",
            AbstractExternalSourceSpecTestCase.pathShapeCannotCarry(dimensions, baseTestReading("{{employees_multifile}}.csv"), glob)
        );
        assertFalse(
            "a standalone source is genuinely reshaped, so the pair is a real test",
            AbstractExternalSourceSpecTestCase.pathShapeCannotCarry(dimensions, baseTestReading("{{employees}}.csv"), glob)
        );
        assertFalse(
            "one standalone source among several is enough -- that half reaches the listing path",
            AbstractExternalSourceSpecTestCase.pathShapeCannotCarry(
                dimensions,
                baseTestReading("{{employees_multifile}}.csv", "{{employees}}.csv"),
                glob
            )
        );

        Map<String, String> exact = new LinkedHashMap<>();
        exact.put("format", "csv");
        exact.put("path_shape", "exact");
        assertFalse(
            "the default shape filters nothing -- it is what every unvaried case already runs",
            AbstractExternalSourceSpecTestCase.pathShapeCannotCarry(dimensions, baseTestReading("{{employees_multifile}}.csv"), exact)
        );
    }

    private static Object[] baseTestReading(String... resources) {
        CsvSpecReader.CsvTestCase testCase = new CsvSpecReader.CsvTestCase();
        List<CsvSpecReader.DatasetSource> sources = new ArrayList<>();
        for (int i = 0; i < resources.length; i++) {
            sources.add(new CsvSpecReader.DatasetSource("ds" + i, resources[i], "{}"));
        }
        testCase.datasetSources = sources;
        return new Object[] { "spec", "name", null, null, testCase };
    }

    /** Padding is a csv fact: every other format is re-rendered from trimmed values, so nothing pads. */
    public void testTrimSpacesIsNotInjectedOnTsvEvenForAPaddedDataset() {
        assertEquals(
            "employees.tsv is re-rendered from trimmed values and has no padding to trim",
            "{}",
            AbstractExternalSourceSpecTestCase.injectTrimSpaces("{}", "{{employees}}.tsv", "tsv")
        );
        assertEquals(
            "the same dataset on csv keeps it, because the authored bytes are padded",
            "{\"trim_spaces\": true}",
            AbstractExternalSourceSpecTestCase.injectTrimSpaces("{}", "{{employees}}.csv", "csv")
        );
    }
}
