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
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import static org.hamcrest.Matchers.containsString;
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
    private static final List<String> BWC_PROPERTIES = List.of(
        EsqlDataSourceMixedClusterTestSupport.BWC_TEST_PROPERTY,
        EsqlDataSourceMixedClusterTestSupport.COORDINATOR_PROPERTY,
        EsqlDataSourceMixedClusterTestSupport.CURRENT_SNAPSHOT_PROPERTY,
        EsqlDataSourceMixedClusterTestSupport.OLD_SNAPSHOT_PROPERTY,
        EsqlDataSourceMixedClusterTestSupport.DETACHED_BWC_REFSPEC_PROPERTY,
        "tests.old_cluster_version"
    );

    @Before
    @After
    public void clearBwcProperties() {
        BWC_PROPERTIES.forEach(System::clearProperty);
        AbstractExternalSourceSpecTestCase.clearBwcReaderProfileGuardStateForTests();
    }

    public void testCurrentVersionRunPreservesCartesianProduct() {
        List<Object[]> expanded = AbstractExternalSourceSpecTestCase.expandExternalSpecTests(
            baseTests(),
            List.of("csv.gz", "csv.zst"),
            List.of(AbstractExternalSourceSpecTestCase.StorageBackend.S3, AbstractExternalSourceSpecTestCase.StorageBackend.HTTP)
        );

        assertEquals(8, expanded.size());
        assertEquals(
            Set.of(
                "a.csv-spec:testA:csv.gz:S3",
                "a.csv-spec:testA:csv.gz:HTTP",
                "a.csv-spec:testA:csv.zst:S3",
                "a.csv-spec:testA:csv.zst:HTTP",
                "b.csv-spec:testB:csv.gz:S3",
                "b.csv-spec:testB:csv.gz:HTTP",
                "b.csv-spec:testB:csv.zst:S3",
                "b.csv-spec:testB:csv.zst:HTTP"
            ),
            tupleIds(expanded)
        );
    }

    public void testBwcRunKeepsEveryBaseAndAddsBackendAndCodecRepresentatives() {
        configureBwcRun();
        BwcMatrixPolicy policy = compressedPolicy("gzip");
        List<Object[]> expanded = AbstractExternalSourceSpecTestCase.expandExternalSpecTests(
            baseTests(),
            List.of("csv.gz", "csv.zst"),
            List.of(AbstractExternalSourceSpecTestCase.StorageBackend.S3, AbstractExternalSourceSpecTestCase.StorageBackend.HTTP),
            policy
        );

        assertEquals(
            Set.of(
                "a.csv-spec:testA:csv.gz:S3",
                "b.csv-spec:testB:csv.gz:S3",
                "a.csv-spec:testA:csv.gz:HTTP",
                "a.csv-spec:testA:csv.zst:S3"
            ),
            tupleIds(expanded)
        );
    }

    public void testBwcRunWithoutCodecKeepsEveryBaseAndAddsBackendRepresentative() {
        configureBwcRun();
        List<Object[]> expanded = AbstractExternalSourceSpecTestCase.expandExternalSpecTests(
            baseTests(),
            List.of(),
            List.of(AbstractExternalSourceSpecTestCase.StorageBackend.S3, AbstractExternalSourceSpecTestCase.StorageBackend.HTTP),
            uncompressedPolicy()
        );

        assertEquals(3, expanded.size());
        assertEquals("a.csv-spec", expanded.get(0)[0]);
        assertEquals(AbstractExternalSourceSpecTestCase.StorageBackend.S3, expanded.get(0)[6]);
        assertEquals("b.csv-spec", expanded.get(1)[0]);
        assertEquals(AbstractExternalSourceSpecTestCase.StorageBackend.S3, expanded.get(1)[6]);
        assertEquals("a.csv-spec", expanded.get(2)[0]);
        assertEquals(AbstractExternalSourceSpecTestCase.StorageBackend.HTTP, expanded.get(2)[6]);
    }

    public void testPolicyShapeMatchesParameterColumnsIncludingOrcStyleNullCodec() {
        BwcMatrixPolicy uncompressed = uncompressedPolicy();
        AbstractExternalSourceSpecTestCase.expandExternalSpecTests(
            baseTests(),
            List.of(),
            List.of(AbstractExternalSourceSpecTestCase.StorageBackend.S3),
            uncompressed
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> AbstractExternalSourceSpecTestCase.expandExternalSpecTests(
                baseTests(),
                List.of("gzip"),
                List.of(AbstractExternalSourceSpecTestCase.StorageBackend.S3),
                uncompressed
            )
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> AbstractExternalSourceSpecTestCase.expandExternalSpecTests(
                baseTests(),
                List.of(),
                List.of(AbstractExternalSourceSpecTestCase.StorageBackend.S3),
                compressedPolicy("gzip")
            )
        );
    }

    public void testPolicyValidationAndDefensiveCopies() {
        BwcMatrixPolicy.BwcTestId representative = representative();
        expectThrows(
            IllegalArgumentException.class,
            () -> BwcMatrixPolicy.uncompressed(AbstractExternalSourceSpecTestCase.StorageBackend.S3)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> BwcMatrixPolicy.compressed(AbstractExternalSourceSpecTestCase.StorageBackend.S3, " ", representative)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> compressedPolicy("gzip").withGuardBackends(Set.of(AbstractExternalSourceSpecTestCase.StorageBackend.HTTP))
        );

        List<BwcMatrixPolicy.BwcTestId> representatives = new ArrayList<>(List.of(representative));
        Set<AbstractExternalSourceSpecTestCase.StorageBackend> guards = new java.util.HashSet<>(
            Set.of(AbstractExternalSourceSpecTestCase.StorageBackend.S3)
        );
        BwcMatrixPolicy policy = new BwcMatrixPolicy(AbstractExternalSourceSpecTestCase.StorageBackend.S3, "GZ", representatives, guards);
        representatives.clear();
        guards.clear();
        assertEquals(List.of(representative), policy.representatives());
        assertEquals(Set.of(AbstractExternalSourceSpecTestCase.StorageBackend.S3), policy.guardBackends());
        assertEquals("gzip", policy.corpusCodec());
    }

    public void testCompressedGuardCellsMatchCorpusCodecBackendFormula() {
        BwcMatrixPolicy policy = compressedPolicy("gzip");
        List<AbstractExternalSourceSpecTestCase.StorageBackend> availableBackends = List.of(
            AbstractExternalSourceSpecTestCase.StorageBackend.values()
        );
        List<String> eligibleCodecs = List.of("gzip", "zstd", "bzip2");
        Set<String> actual = availableBackends.stream()
            .flatMap(backend -> eligibleCodecs.stream().map(codec -> new Object[] { backend, codec }))
            .filter(cell -> policy.isGuardCell((AbstractExternalSourceSpecTestCase.StorageBackend) cell[0], (String) cell[1]))
            .map(cell -> cell[0] + "/" + cell[1])
            .collect(Collectors.toSet());

        assertEquals(Set.of("S3/gzip", "S3/zstd", "S3/bzip2", "GCS/gzip", "AZURE/gzip", "LOCAL/gzip"), actual);
    }

    public void testUncompressedGuardCellsExcludeHttp() {
        BwcMatrixPolicy policy = uncompressedPolicy();
        Set<AbstractExternalSourceSpecTestCase.StorageBackend> actual = List.of(AbstractExternalSourceSpecTestCase.StorageBackend.values())
            .stream()
            .filter(backend -> policy.isGuardCell(backend, "none"))
            .collect(Collectors.toSet());
        assertEquals(
            Set.of(
                AbstractExternalSourceSpecTestCase.StorageBackend.S3,
                AbstractExternalSourceSpecTestCase.StorageBackend.GCS,
                AbstractExternalSourceSpecTestCase.StorageBackend.AZURE,
                AbstractExternalSourceSpecTestCase.StorageBackend.LOCAL
            ),
            actual
        );
    }

    /**
     * A policy that reaches no guard cell must be rejected rather than expanded: set equality alone
     * passes vacuously when both sides are empty. Uncompressed with an emptied guard-backend set is the
     * only shape that can reach none -- a compressed policy guards every cell on its corpus backend
     * whatever the guard backends are, and corpus-backend tuples are always emitted.
     */
    public void testBwcExpansionRejectsAPolicyThatReachesNoGuardCell() {
        configureBwcRun();
        BwcMatrixPolicy policy = uncompressedPolicy().withGuardBackends(Set.of());
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> AbstractExternalSourceSpecTestCase.expandExternalSpecTests(
                baseTests(),
                List.of(),
                List.of(AbstractExternalSourceSpecTestCase.StorageBackend.S3, AbstractExternalSourceSpecTestCase.StorageBackend.HTTP),
                policy
            )
        );
        assertThat(e.getMessage(), containsString("reaches no guard cell"));
    }

    /**
     * The other half of the invariant: an expansion that stopped emitting the representative on a guard
     * backend must be named. Only reachable through the check's own entry point -- the factory cannot
     * produce such a matrix, which is why the check is separable from it.
     */
    public void testGuardCellCoverageNamesADroppedBackendRepresentative() {
        configureBwcRun();
        BwcMatrixPolicy policy = uncompressedPolicy();
        List<AbstractExternalSourceSpecTestCase.StorageBackend> backends = List.of(
            AbstractExternalSourceSpecTestCase.StorageBackend.S3,
            AbstractExternalSourceSpecTestCase.StorageBackend.GCS
        );
        List<Object[]> expanded = AbstractExternalSourceSpecTestCase.expandExternalSpecTests(baseTests(), List.of(), backends, policy);
        List<Object[]> withoutGcs = expanded.stream()
            .filter(tuple -> tuple[6] != AbstractExternalSourceSpecTestCase.StorageBackend.GCS)
            .toList();

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> AbstractExternalSourceSpecTestCase.verifyGuardCellCoverage(withoutGcs, List.of(), backends, policy)
        );
        assertThat(e.getMessage(), containsString("missing [GCS:none]"));
    }

    /**
     * The factory and the test instance must agree on a cell's codec identity, or the factory would
     * verify a guard cell the instance never recognises. The Parquet compressed suites are why the
     * instance side stays overridable: their codec column is a bare internal codec name, which has no
     * extension for {@code textCodecIdentity} to read.
     */
    public void testMatrixCodecIdentityAgreesWithTheInstanceSideIdentity() {
        assertEquals("gzip", AbstractExternalSourceSpecTestCase.matrixCodecIdentity("csv.gz"));
        assertEquals("gzip", EsqlDataSourceCodecEligibility.normalizeCodecToken("csv.gz"));
        assertEquals("gzip", EsqlDataSourceCodecEligibility.textCodecIdentity("csv.gz"));

        assertEquals("snappy", AbstractExternalSourceSpecTestCase.matrixCodecIdentity("snappy"));
        assertEquals("snappy", EsqlDataSourceCodecEligibility.normalizeCodecToken("snappy"));
        assertEquals("none", EsqlDataSourceCodecEligibility.textCodecIdentity("snappy"));

        assertEquals("none", AbstractExternalSourceSpecTestCase.matrixCodecIdentity(null));
        assertEquals("none", EsqlDataSourceCodecEligibility.textCodecIdentity("parquet"));
    }

    public void testCodecIdentitiesAndStableAliasTieBreak() {
        for (String format : List.of("csv", "tsv", "ndjson", "parquet", "orc")) {
            assertEquals("none", EsqlDataSourceCodecEligibility.textCodecIdentity(format));
            assertEquals("none", EsqlDataSourceCodecEligibility.normalizeCodecToken(format));
        }
        assertEquals("none", uncompressedPolicy().corpusGuardCodecIdentity());
        assertEquals("gzip", EsqlDataSourceCodecEligibility.normalizeCodecToken("csv.gz"));
        assertEquals("gzip", EsqlDataSourceCodecEligibility.normalizeCodecToken("ndjson.gz"));
        assertEquals("gzip", EsqlDataSourceCodecEligibility.normalizeCodecToken("gzip"));
        assertEquals("zstd", EsqlDataSourceCodecEligibility.normalizeCodecToken(".zst"));
        assertEquals("zstd", EsqlDataSourceCodecEligibility.normalizeCodecToken(".zstd"));
        assertEquals("zstd", EsqlDataSourceCodecEligibility.normalizeCodecToken("zstd"));
        assertEquals("bzip2", EsqlDataSourceCodecEligibility.normalizeCodecToken(".bz"));
        assertEquals("bzip2", EsqlDataSourceCodecEligibility.normalizeCodecToken(".bz2"));
        assertEquals("bzip2", EsqlDataSourceCodecEligibility.normalizeCodecToken("bzip2"));
        assertEquals("snappy", EsqlDataSourceCodecEligibility.normalizeCodecToken("snappy"));
        assertEquals("lz4raw", EsqlDataSourceCodecEligibility.normalizeCodecToken("lz4raw"));

        configureBwcRun();
        List<Object[]> expanded = AbstractExternalSourceSpecTestCase.expandExternalSpecTests(
            baseTests(),
            List.of("csv.zst", "csv.zstd"),
            List.of(AbstractExternalSourceSpecTestCase.StorageBackend.S3, AbstractExternalSourceSpecTestCase.StorageBackend.GCS),
            compressedPolicy("zstd")
        );
        assertTrue(tupleIds(expanded).contains("b.csv-spec:testB:csv.zst:S3"));
        assertFalse(tupleIds(expanded).contains("b.csv-spec:testB:csv.zstd:S3"));
        assertTrue(tupleIds(expanded).contains("a.csv-spec:testA:csv.zstd:S3"));
    }

    public void testGuardPredicateIsBackendNeutralAndOldCoordinatorOnly() {
        configureBwcRun();
        BwcMatrixPolicy policy = compressedPolicy("gzip");
        assertTrue(
            AbstractExternalSourceSpecTestCase.shouldRunBwcReaderProfileGuard(
                policy,
                AbstractExternalSourceSpecTestCase.StorageBackend.GCS,
                "gzip"
            )
        );

        System.setProperty(EsqlDataSourceMixedClusterTestSupport.COORDINATOR_PROPERTY, "current");
        for (AbstractExternalSourceSpecTestCase.StorageBackend backend : AbstractExternalSourceSpecTestCase.StorageBackend.values()) {
            assertFalse(AbstractExternalSourceSpecTestCase.shouldRunBwcReaderProfileGuard(policy, backend, "gzip"));
        }
    }

    public void testDetachedBuildExcludesGuardAndOrdinaryLocalTuple() {
        configureBwcRun();
        System.setProperty(EsqlDataSourceMixedClusterTestSupport.DETACHED_BWC_REFSPEC_PROPERTY, "main");
        assertFalse(
            AbstractExternalSourceSpecTestCase.shouldRunBwcReaderProfileGuard(
                uncompressedPolicy(),
                AbstractExternalSourceSpecTestCase.StorageBackend.LOCAL,
                "none"
            )
        );
        assertTrue(
            AbstractExternalSourceSpecTestCase.shouldSkipDetachedBwcLocalTuple(AbstractExternalSourceSpecTestCase.StorageBackend.LOCAL)
        );
        assertFalse(
            AbstractExternalSourceSpecTestCase.shouldSkipDetachedBwcLocalTuple(AbstractExternalSourceSpecTestCase.StorageBackend.S3)
        );
    }

    public void testCurrentRunShortCircuitsAbsentPolicyAndCoordinator() {
        assertFalse(
            AbstractExternalSourceSpecTestCase.shouldRunBwcReaderProfileGuard(
                null,
                AbstractExternalSourceSpecTestCase.StorageBackend.S3,
                "gzip"
            )
        );
    }

    public void testGuardExceptionReportsClusterHealthBeforeRethrowing() {
        boolean[] checked = new boolean[1];
        IllegalStateException failure = new IllegalStateException("guard failed");
        IllegalStateException thrown = expectThrows(
            IllegalStateException.class,
            () -> AbstractExternalSourceSpecTestCase.runGuardReportingClusterHealth(() -> {
                throw failure;
            }, e -> {
                assertSame(failure, e);
                checked[0] = true;
            })
        );
        assertSame(failure, thrown);
        assertTrue(checked[0]);
    }

    public void testGuardAssertionDoesNotReportClusterHealth() {
        boolean[] checked = new boolean[1];
        AssertionError failure = new AssertionError("guard assertion");
        AssertionError thrown = expectThrows(
            AssertionError.class,
            () -> AbstractExternalSourceSpecTestCase.runGuardReportingClusterHealth(() -> {
                throw failure;
            }, e -> checked[0] = true)
        );
        assertSame(failure, thrown);
        assertFalse(checked[0]);
    }

    public void testTextCodecEligibilityUsesBothBuildModes() {
        System.setProperty(EsqlDataSourceMixedClusterTestSupport.BWC_TEST_PROPERTY, "true");
        System.setProperty(EsqlDataSourceMixedClusterTestSupport.CURRENT_SNAPSHOT_PROPERTY, "true");
        System.setProperty(EsqlDataSourceMixedClusterTestSupport.OLD_SNAPSHOT_PROPERTY, "false");
        assertEquals(List.of("csv.gz", "csv.zst", "csv.zstd"), EsqlDataSourceCodecEligibility.textCompressionFormats("csv"));

        System.setProperty(EsqlDataSourceMixedClusterTestSupport.OLD_SNAPSHOT_PROPERTY, "true");
        assertEquals(
            List.of("csv.gz", "csv.zst", "csv.zstd", "csv.bz2", "csv.bz"),
            EsqlDataSourceCodecEligibility.textCompressionFormats("csv")
        );
    }

    public void testParquetCodecEligibilityUsesSharedIntroductionVersions() {
        System.setProperty(EsqlDataSourceMixedClusterTestSupport.BWC_TEST_PROPERTY, "true");
        System.setProperty("tests.old_cluster_version", "9.5.4");
        assertEquals(
            List.of("snappy", "gzip", "zstd", "lz4raw"),
            EsqlDataSourceCodecEligibility.parquetCodecs("snappy", "gzip", "zstd", "lz4raw")
        );
    }

    /**
     * An all-ineligible codec list must fail rather than drop the codec column from the parameter
     * tuples, which would surface as a constructor-arity error in the owning suite.
     */
    public void testParquetCodecEligibilityRejectsAnEmptyResult() {
        System.setProperty(EsqlDataSourceMixedClusterTestSupport.BWC_TEST_PROPERTY, "true");
        System.setProperty("tests.old_cluster_version", "9.4.9");
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> EsqlDataSourceCodecEligibility.parquetCodecs("snappy", "gzip", "zstd", "lz4raw")
        );
        assertThat(e.getMessage(), containsString("is supported on 9.4.9"));
    }

    private static void configureBwcRun() {
        System.setProperty(EsqlDataSourceMixedClusterTestSupport.BWC_TEST_PROPERTY, "true");
        System.setProperty(EsqlDataSourceMixedClusterTestSupport.COORDINATOR_PROPERTY, "old");
    }

    private static BwcMatrixPolicy uncompressedPolicy() {
        return BwcMatrixPolicy.uncompressed(AbstractExternalSourceSpecTestCase.StorageBackend.S3, representative());
    }

    private static BwcMatrixPolicy compressedPolicy(String codec) {
        return BwcMatrixPolicy.compressed(AbstractExternalSourceSpecTestCase.StorageBackend.S3, codec, representative());
    }

    private static BwcMatrixPolicy.BwcTestId representative() {
        return new BwcMatrixPolicy.BwcTestId("a.csv-spec", "testA");
    }

    private static List<Object[]> baseTests() {
        return List.of(new Object[] { "a.csv-spec", "a", "testA", 1, null, "" }, new Object[] { "b.csv-spec", "b", "testB", 2, null, "" });
    }

    private static Set<String> tupleIds(List<Object[]> tuples) {
        return tuples.stream().map(tuple -> tuple[0] + ":" + tuple[2] + ":" + tuple[6] + ":" + tuple[7]).collect(Collectors.toSet());
    }

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
     * <p>Which three pad is a fact about the authored bytes, not a judgement: {@code checkFixturePadding}
     * reads every canonical CSV and fails when a declaration disagrees with the leading or trailing space
     * a field actually carries. Injecting the key blanket would assert padding on seven datasets that
     * carry none, and it once cost csv and tsv their whole {@code path_shape=glob} cell besides.
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
