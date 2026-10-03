/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

import org.elasticsearch.test.ESTestCase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;

/**
 * Exercises every validator rule: the fixture catalog + workload must be clean, and each targeted
 * mutation must trip exactly the rule it violates. Mutations are applied to the parsed records
 * (variant rebuild) or to single unique workload lines, never to multi-line YAML blocks.
 */
public class CatalogValidatorTests extends ESTestCase {

    public void testFixtureCatalogIsValid() throws IOException {
        assertThat(validate(fixtureCatalog(), fixtureWorkloadLines()), empty());
    }

    public void testFileSchemeIsRejected() throws IOException {
        PublicDataCatalog catalog = mutateVariant(
            fixtureCatalog(),
            "fixture-s3-parquet-snappy-single",
            v -> withResource(v, "file:///tmp/fixture.parquet")
        );
        List<String> errors = validate(catalog, fixtureWorkloadLines());
        assertThat(errors, hasItem(containsString("file://")));
        assertThat(errors, hasItem(containsString("does not match provider")));
    }

    public void testNonAnonymousS3IsRejected() throws IOException {
        PublicDataCatalog catalog = mutateVariant(
            fixtureCatalog(),
            "fixture-s3-parquet-snappy-single",
            v -> withDataSourceSettings(v, Map.of("auth", "auto", "region", "eu-central-1"))
        );
        assertThat(validate(catalog, fixtureWorkloadLines()), hasItem(containsString("not [anonymous]")));
    }

    public void testGlobOnHttpsIsRejected() throws IOException {
        PublicDataCatalog catalog = mutateVariant(
            fixtureCatalog(),
            "fixture-https-parquet-snappy-single",
            v -> withResource(v, "https://mirror.example.org/data/*.parquet")
        );
        assertThat(validate(catalog, fixtureWorkloadLines()), hasItem(containsString("cannot list objects")));
    }

    public void testMissingPinIsRejected() throws IOException {
        PublicDataCatalog catalog = mutateVariant(fixtureCatalog(), "fixture-s3-parquet-snappy-single", v -> withPin(v, null));
        assertThat(validate(catalog, fixtureWorkloadLines()), hasItem(containsString("no pin: block")));
    }

    public void testDegeneratePinIsRejected() throws IOException {
        PublicDataCatalog catalog = mutateVariant(
            fixtureCatalog(),
            "fixture-s3-parquet-snappy-single",
            v -> withPin(v, new PinSpec("HEAD", v.pin().verifiedAt(), 0, 0, List.of(), false, PinSpec.DEFAULT_SIZE_TOLERANCE_PERCENT))
        );
        assertThat(validate(catalog, fixtureWorkloadLines()), hasItem(containsString("degenerate pin")));
    }

    public void testMislabeledCodecExtensionIsRejected() throws IOException {
        // a TEXT-format variant claiming zstd over a .gz-suffixed NON-GLOB object (extension
        // checks skip globs); parquet variants are exempt (container formats compress internally)
        PublicDataCatalog catalog = mutateVariant(
            fixtureCatalog(),
            "fixture-s3-csv-gzip-shards",
            v -> withCodec(withResource(v, "s3://example-bucket/shards/part_0.csv.gz"), Codec.ZSTD)
        );
        assertThat(validate(catalog, fixtureWorkloadLines()), hasItem(containsString("lacks its suffix")));
    }

    public void testParquetCodecNeedsNoSuffix() throws IOException {
        PublicDataCatalog catalog = mutateVariant(fixtureCatalog(), "fixture-s3-parquet-snappy-single", v -> withCodec(v, Codec.ZSTD));
        // label change breaks the workload's reference-variant resolution, but no codec-suffix error
        List<String> errors = validate(catalog, fixtureWorkloadLines());
        assertThat(errors, org.hamcrest.Matchers.not(hasItem(containsString("lacks its suffix"))));
    }

    public void testFailureOnlyCorpusNeedsFailureVariant() throws IOException {
        PublicDataCatalog catalog = mutateVariant(fixtureCatalog(), "fixture-dirty-s3-csv-uncompressed-single", v -> withFailure(v, null));
        assertThat(validate(catalog, fixtureWorkloadLines()), hasItem(containsString("no expect_failure variant")));
    }

    public void testVolatilePinNeedsInvariantCorpusAndNotes() throws IOException {
        // the fixture corpus is assertion_mode exact and the reference variant has no notes,
        // so a volatile pin there must trip both guards at once
        // tags: [reference] is unique to the fixture's reference variant, so this targets its pin only
        String yaml = fixtureYaml().replace(
            "        tags: [reference]\n        pin:\n          method: HEAD",
            "        tags: [reference]\n        pin:\n          method: HEAD\n          volatile: true"
        );
        List<String> errors = validate(parseCatalog(yaml), fixtureWorkloadLines());
        assertThat(errors, hasItem(containsString("no notes: justifying why the bytes move")));
        assertThat(errors, hasItem(containsString("cannot carry frozen expected tables")));
    }

    public void testInvariantCorpusNeedsPerTestProvenance() throws IOException {
        // flipping the fixture corpus to invariant mode must demand the markers on every test,
        // so an invariant expectation can never look like a frozen one
        String yaml = fixtureYaml().replace(
            "    workload: fixture-workload.csv-spec",
            "    workload: fixture-workload.csv-spec\n    assertion_mode: invariant"
        );
        List<String> errors = validate(parseCatalog(yaml), fixtureWorkloadLines());
        assertThat(errors, hasItem(containsString("must carry // assertion-mode: invariant")));
        assertThat(errors, hasItem(containsString("// oracle-observed:")));
    }

    public void testAssertionModeMarkerRejectedOnExactCorpus() throws IOException {
        List<String> workload = replaceLine(
            fixtureWorkloadLines(),
            "// read-shape: scan",
            "// read-shape: scan",
            "// assertion-mode: invariant"
        );
        assertThat(validate(fixtureCatalog(), workload), hasItem(containsString("is assertion_mode exact")));
    }

    public void testUndeclaredSubResourceIsRejected() throws IOException {
        List<String> workload = replaceLine(
            fixtureWorkloadLines(),
            "dataset: fixture_right: \"{{corpus:right}}\"",
            "dataset: fixture_right: \"{{corpus:nowhere}}\""
        );
        assertThat(validate(fixtureCatalog(), workload), hasItem(containsString("declares no such sub_resource")));
    }

    public void testSubResourceMissingFromOneVariantIsRejected() throws IOException {
        // the gz shards leg drops [right]; the parquet reference still has it, so only one leg
        // would break at runtime -- exactly the asymmetry this rule exists to catch offline
        String yaml = fixtureYaml().replace("          right: \"s3://example-bucket/shards/part_[12].csv.gz\"\n", "");
        assertThat(validate(parseCatalog(yaml), fixtureWorkloadLines()), hasItem(containsString("declares no such sub_resource")));
    }

    public void testRepeatedSubResourceBindingIsRejected() throws IOException {
        List<String> workload = replaceLine(
            fixtureWorkloadLines(),
            "dataset: fixture_right: \"{{corpus:right}}\"",
            "dataset: fixture_right: \"{{corpus:left}}\""
        );
        assertThat(validate(fixtureCatalog(), workload), hasItem(containsString("more than once")));
    }

    public void testMultiSourceTestMayNotBindTheWholeCorpus() throws IOException {
        List<String> workload = replaceLine(
            fixtureWorkloadLines(),
            "dataset: fixture_right: \"{{corpus:right}}\"",
            "dataset: fixture_right: \"{{corpus}}\""
        );
        assertThat(validate(fixtureCatalog(), workload), hasItem(containsString("use {{corpus:<name>}} per dataset")));
    }

    public void testLiteralResourceInDatasetDirectiveIsRejected() throws IOException {
        List<String> workload = replaceLine(
            fixtureWorkloadLines(),
            "dataset: fixture: \"{{corpus}}\"",
            "dataset: fixture: \"s3://example-bucket/data/fixture.parquet\""
        );
        assertThat(validate(fixtureCatalog(), workload), hasItem(containsString("not a literal resource")));
    }

    public void testSubResourceMustMatchProviderScheme() throws IOException {
        String yaml = fixtureYaml().replace(
            "          left: \"s3://example-bucket/data/fixture-left.parquet\"",
            "          left: \"file:///tmp/left.parquet\""
        );
        List<String> errors = validate(parseCatalog(yaml), fixtureWorkloadLines());
        assertThat(errors, hasItem(containsString("file://")));
        assertThat(errors, hasItem(containsString("sub_resource [left]")));
    }

    public void testUnknownQuerySubsetEntryIsRejected() throws IOException {
        String yaml = fixtureYaml().replace(
            "query_subset: [q1_scan, q2_agg, q3_topn, q4_limit]",
            "query_subset: [q1_scan, q2_agg, q3_topn, q4_limit, q9_nope]"
        );
        assertThat(validate(parseCatalog(yaml), fixtureWorkloadLines()), hasItem(containsString("q9_nope")));
    }

    public void testQuerySubsetMustCoverAllShapes() throws IOException {
        String yaml = fixtureYaml().replace("query_subset: [q1_scan, q2_agg, q3_topn, q4_limit]", "query_subset: [q1_scan, q2_agg]");
        List<String> errors = validate(parseCatalog(yaml), fixtureWorkloadLines());
        assertThat(errors, hasItem(containsString("does not cover read shape [topn]")));
        assertThat(errors, hasItem(containsString("does not cover read shape [limit]")));
    }

    public void testUncoveredDimensionValueNeedsGap() throws IOException {
        String yaml = fixtureYaml().replace("cells: [format=tsv, format=ndjson]", "cells: [format=tsv]");
        assertThat(validate(parseCatalog(yaml), fixtureWorkloadLines()), hasItem(containsString("format=ndjson")));
    }

    public void testIgnoredTestWithoutDefectBlockIsRejected() throws IOException {
        List<String> workload = fixtureWorkloadLines().stream()
            .filter(line -> line.startsWith("// defect") == false)
            .collect(Collectors.toList());
        assertThat(validate(fixtureCatalog(), workload), hasItem(containsString("-Ignore'd without")));
    }

    public void testMultiRowExpectationNeedsTieBreaker() throws IOException {
        List<String> workload = replaceLine(
            fixtureWorkloadLines(),
            "FROM fixture | KEEP a, b | SORT a ASC, b ASC | LIMIT 10;",
            "FROM fixture | KEEP a, b | SORT a ASC | LIMIT 10;"
        );
        assertThat(validate(fixtureCatalog(), workload), hasItem(containsString("no SORT with a tie-breaker")));
    }

    public void testSortReviewedMarkerAllowsSingleKeySort() throws IOException {
        List<String> workload = replaceLine(
            fixtureWorkloadLines(),
            "FROM fixture | KEEP a, b | SORT a ASC, b ASC | LIMIT 10;",
            "FROM fixture | KEEP a, b | SORT a ASC | LIMIT 10;"
        );
        workload = replaceLine(workload, "// read-shape: scan", "// read-shape: scan", "// sort-reviewed: column a is unique here");
        assertThat(validate(fixtureCatalog(), workload), empty());
    }

    public void testMaxRowsCapsAreEnforced() throws IOException {
        List<String> workload = replaceLine(fixtureWorkloadLines(), "// max-rows: 10", "// max-rows: 2000");
        assertThat(validate(fixtureCatalog(), workload), hasItem(containsString("absolute cap")));

        workload = replaceLine(fixtureWorkloadLines(), "// max-rows: 10", "// max-rows: 400");
        assertThat(validate(fixtureCatalog(), workload), hasItem(containsString("without a reason")));
    }

    public void testExpectedRowsAboveMaxRowsIsRejected() throws IOException {
        List<String> workload = replaceLine(fixtureWorkloadLines(), "// max-rows: 10", "// max-rows: 1");
        assertThat(validate(fixtureCatalog(), workload), hasItem(containsString("above its max-rows")));
    }

    public void testMissingCapabilityIsRejected() throws IOException {
        List<String> workload = fixtureWorkloadLines().stream()
            .filter(line -> line.equals("required_capability: dataset_in_from_command") == false)
            .collect(Collectors.toList());
        assertThat(validate(fixtureCatalog(), workload), hasItem(containsString("required_capability")));
    }

    public void testLiteralDatasetResourceIsRejected() throws IOException {
        List<String> workload = fixtureWorkloadLines().stream()
            .map(line -> line.startsWith("dataset: fixture:") ? "dataset: fixture: \"s3://bucket/literal.parquet\"" : line)
            .collect(Collectors.toList());
        assertThat(validate(fixtureCatalog(), workload), hasItem(containsString("{{corpus}}")));
    }

    public void testMissingReadShapeCoverageIsRejected() throws IOException {
        List<String> workload = replaceLine(fixtureWorkloadLines(), "// read-shape: topn", "// read-shape: aggregate");
        // the csv shards leg's query_subset also loses its topn coverage, so expect both complaints
        List<String> errors = validate(fixtureCatalog(), workload);
        assertThat(errors, hasItem(containsString("does not cover read shape [topn]")));
    }

    public void testOrphanSpecFileIsRejected() throws IOException {
        WorkloadSpec workload = WorkloadSpec.parse("fixture-workload.csv-spec", fixtureWorkloadLines());
        WorkloadSpec orphan = WorkloadSpec.parse("public-orphan.csv-spec", fixtureWorkloadLines());
        List<String> errors = CatalogValidator.validate(
            fixtureCatalog(),
            Map.of("fixture-workload.csv-spec", workload, "public-orphan.csv-spec", orphan)
        );
        assertThat(errors, hasItem(containsString("not claimed by any corpus")));
    }

    // -- helpers --

    private static List<String> validate(PublicDataCatalog catalog, List<String> workloadLines) {
        WorkloadSpec workload = WorkloadSpec.parse("fixture-workload.csv-spec", workloadLines);
        return CatalogValidator.validate(catalog, Map.of("fixture-workload.csv-spec", workload));
    }

    private static List<String> replaceLine(List<String> lines, String from, String... to) {
        List<String> result = new java.util.ArrayList<>();
        boolean replaced = false;
        for (String line : lines) {
            if (line.equals(from)) {
                result.addAll(List.of(to));
                replaced = true;
            } else {
                result.add(line);
            }
        }
        assertTrue("fixture drifted: line [" + from + "] not found", replaced);
        return result;
    }

    private static PublicDataCatalog mutateVariant(PublicDataCatalog catalog, String label, UnaryOperator<VariantSpec> mutation) {
        boolean[] applied = new boolean[1];
        List<CorpusSpec> corpora = catalog.corpora().stream().map(corpus -> {
            List<VariantSpec> variants = corpus.variants().stream().map(variant -> {
                if (variant.label().equals(label)) {
                    applied[0] = true;
                    return mutation.apply(variant);
                }
                return variant;
            }).toList();
            return new CorpusSpec(
                corpus.id(),
                corpus.title(),
                corpus.registryUrl(),
                corpus.license(),
                corpus.description(),
                corpus.kind(),
                corpus.scale(),
                corpus.quality(),
                corpus.workload(),
                corpus.assertionMode(),
                variants
            );
        }).toList();
        assertTrue("fixture drifted: variant [" + label + "] not found", applied[0]);
        return new PublicDataCatalog(catalog.version(), corpora, catalog.gaps());
    }

    private static VariantSpec withResource(VariantSpec v, String resource) {
        return rebuild(v, resource, v.codec(), v.dataSourceSettings(), v.pin(), v.expectFailure());
    }

    private static VariantSpec withCodec(VariantSpec v, Codec codec) {
        return rebuild(v, v.resource(), codec, v.dataSourceSettings(), v.pin(), v.expectFailure());
    }

    private static VariantSpec withDataSourceSettings(VariantSpec v, Map<String, Object> settings) {
        return rebuild(v, v.resource(), v.codec(), settings, v.pin(), v.expectFailure());
    }

    private static VariantSpec withPin(VariantSpec v, PinSpec pin) {
        return rebuild(v, v.resource(), v.codec(), v.dataSourceSettings(), pin, v.expectFailure());
    }

    private static VariantSpec withFailure(VariantSpec v, FailureSpec failure) {
        return rebuild(v, v.resource(), v.codec(), v.dataSourceSettings(), v.pin(), failure);
    }

    private static VariantSpec rebuild(
        VariantSpec v,
        String resource,
        Codec codec,
        Map<String, Object> dataSourceSettings,
        PinSpec pin,
        FailureSpec failure
    ) {
        return new VariantSpec(
            v.corpusId(),
            v.provider(),
            v.format(),
            codec,
            v.layout(),
            v.partitioning(),
            v.region(),
            resource,
            v.subResources(),
            dataSourceSettings,
            v.datasetSettings(),
            v.datasetMappings(),
            pin,
            v.tags(),
            v.querySubset(),
            v.notes(),
            failure,
            v.caseId(),
            v.disabledReason()
        );
    }

    private static PublicDataCatalog fixtureCatalog() throws IOException {
        return parseCatalog(fixtureYaml());
    }

    private static PublicDataCatalog parseCatalog(String yaml) throws IOException {
        try (
            var parser = org.elasticsearch.xcontent.yaml.YamlXContent.yamlXContent.createParser(
                org.elasticsearch.xcontent.XContentParserConfiguration.EMPTY,
                yaml
            )
        ) {
            return PublicDataCatalog.fromMap(parser.mapOrdered());
        }
    }

    private static String fixtureYaml() throws IOException {
        return readResource("/fixture-catalog.yml");
    }

    private static List<String> fixtureWorkloadLines() throws IOException {
        return readResource("/fixture-workload.csv-spec").lines().collect(Collectors.toList());
    }

    private static String readResource(String resource) throws IOException {
        try (InputStream in = CatalogValidatorTests.class.getResourceAsStream(resource)) {
            assertNotNull("missing test resource " + resource, in);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                return reader.lines().collect(Collectors.joining("\n"));
            }
        }
    }
}
