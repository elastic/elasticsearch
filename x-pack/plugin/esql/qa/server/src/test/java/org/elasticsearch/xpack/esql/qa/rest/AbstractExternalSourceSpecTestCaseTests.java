/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest;

import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;

/**
 * Unit tests for path resolution helpers in {@link AbstractExternalSourceSpecTestCase}.
 * The interesting case is glob path resolution on Windows: {@link Path#resolve(String)}
 * rejects {@code *} on NTFS because it is a reserved filename character, so multi-file
 * fixture paths like {@code multifile/*.csv} must not be round-tripped through
 * {@link Path}.
 */
public class AbstractExternalSourceSpecTestCaseTests extends ESTestCase {

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
        assertEquals("{\"trim_spaces\": true}", AbstractExternalSourceSpecTestCase.injectTrimSpaces(null));
    }

    public void testInjectTrimSpacesAddsToEmptyObject() {
        assertEquals("{\"trim_spaces\": true}", AbstractExternalSourceSpecTestCase.injectTrimSpaces("{}"));
        assertEquals("{\"trim_spaces\": true}", AbstractExternalSourceSpecTestCase.injectTrimSpaces("{ }"));
    }

    public void testInjectTrimSpacesMergesIntoExistingOptions() {
        assertEquals(
            "{\"header_row\": false, \"trim_spaces\": true}",
            AbstractExternalSourceSpecTestCase.injectTrimSpaces("{\"header_row\": false}")
        );
    }

    public void testInjectTrimSpacesLeavesExplicitTrimSpacesUntouched() {
        String withJson = "{\"trim_spaces\": false}";
        assertEquals(withJson, AbstractExternalSourceSpecTestCase.injectTrimSpaces(withJson));
    }

    public void testInjectTrimSpacesDoesNotFalseMatchAValue() {
        // "trim_spaces" appears only as a value here, so the injection must still fire.
        assertEquals(
            "{\"null_value\": \"trim_spaces\", \"trim_spaces\": true}",
            AbstractExternalSourceSpecTestCase.injectTrimSpaces("{\"null_value\": \"trim_spaces\"}")
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
            AbstractExternalSourceSpecTestCase.injectTrimSpaces("{\"mappings\": {\"properties\": {\"a\": {\"type\": \"keyword\"}}}}")
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
                "{\"mappings\": {\"properties\": {\"trim_spaces\": {\"type\": \"keyword\"}}}}"
            )
        );
    }

    /** An explicitly-set trim_spaces SETTING is still left untouched, alongside a declared schema. */
    public void testInjectTrimSpacesLeavesAnExplicitSettingUntouchedBesideADeclaration() {
        String withJson = "{\"trim_spaces\": false, \"mappings\": {\"dynamic\": \"false\"}}";
        assertEquals(withJson, AbstractExternalSourceSpecTestCase.injectTrimSpaces(withJson));
    }
}
