/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.fixtures;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * The declaration is read by two consumers -- this class at run time and fixture-matrix.gradle at
 * configuration time -- so its rules are worth pinning on the side that can be unit-tested.
 */
public class FixtureMatrixTests extends ESTestCase {

    public void testEveryDeclaredFormatIsMaterialised() {
        FixtureMatrix matrix = FixtureMatrix.get();
        assertThat(matrix.formats(), contains("csv", "tsv", "ndjson", "orc", "parquet")); // dimension-copy-ok: a test that pins per-format
                                                                                          // expectations must name the formats it pins
        for (String format : matrix.formats()) {
            assertThat("every format carries the baseline", matrix.datasetsFor(format), not(empty()));
        }
    }

    public void testBaselineIsCarriedByEveryFormat() {
        FixtureMatrix matrix = FixtureMatrix.get();
        List<String> baseline = matrix.baseline();
        assertThat(baseline, not(empty()));
        for (String dataset : baseline) {
            assertThat("baseline dataset has no restriction reason", matrix.restrictionReason(dataset), nullValue());
            for (String format : matrix.formats()) {
                assertTrue(dataset + " must exist for " + format, matrix.declares(format, dataset));
            }
        }
    }

    public void testRestrictedDatasetDeclaresATypedReason() {
        FixtureMatrix matrix = FixtureMatrix.get();
        for (String format : matrix.formats()) {
            for (String dataset : matrix.datasetsFor(format)) {
                String reason = matrix.restrictionReason(dataset);
                if (reason != null) {
                    assertTrue(
                        "a restriction reason says which kind it is: " + dataset + " -> " + reason,
                        reason.startsWith("rule:") || reason.startsWith("gap:")
                    );
                }
            }
        }
    }

    public void testUnknownFormatIsRejectedRatherThanEmpty() {
        FixtureMatrix matrix = FixtureMatrix.get();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> matrix.datasetsFor("parqet"));
        assertThat(e.getMessage(), containsString("unknown fixture format [parqet]"));
    }

    public void testLongestLayoutSuffixWins() {
        FixtureMatrix matrix = FixtureMatrix.get();
        // _multifile_split must not be read as _multifile, and _multifile_type_drift not as either.
        assertThat(matrix.layoutFor("employees_multifile_split").name(), equalTo("multifile_split"));
        assertThat(matrix.layoutFor("employees_multifile_type_drift").name(), equalTo("multifile_type_drift"));
        assertThat(matrix.layoutFor("employees_multifile").name(), equalTo("multifile"));
        assertThat(matrix.layoutFor("x_multifile_perm").name(), equalTo("multifile_perm"));
    }

    public void testTemplateWithNoLayoutSuffixIsStandalone() {
        FixtureMatrix matrix = FixtureMatrix.get();
        FixtureMatrix.Layout layout = matrix.layoutFor("employees");
        assertTrue(layout.isStandalone());
        assertThat(layout.glob(), nullValue());
        assertThat(layout.dir(), equalTo("standalone"));
    }

    public void testHiveDirectoryDiffersFromItsSuffixAndGlobsRecursively() {
        FixtureMatrix.Layout hive = FixtureMatrix.get().layout("hive");
        assertThat(hive.suffix(), equalTo("_hive"));
        assertThat("the one layout whose directory is not its name", hive.dir(), equalTo("hive-partitioned"));
        assertThat("must recurse into the partition directories", hive.glob(), equalTo("**/*"));
    }

    public void testUndeclaredLayoutIsRejected() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> FixtureMatrix.get().layout("nope"));
        assertThat(e.getMessage(), containsString("undeclared fixture layout [nope]"));
    }

    public void testBaseFormatStripsTheCodecButNothingElse() {
        // The suites run with a file EXTENSION: compression is a separate dimension layered on top.
        assertThat(FixtureMatrix.baseFormat("csv.gz"), equalTo("csv"));
        assertThat(FixtureMatrix.baseFormat("ndjson.zstd"), equalTo("ndjson"));
        assertThat(FixtureMatrix.baseFormat("tsv.bz2"), equalTo("tsv"));
        assertThat(FixtureMatrix.baseFormat("parquet"), equalTo("parquet"));
        for (String format : FixtureMatrix.get().formats()) {
            assertThat(FixtureMatrix.baseFormat(format), equalTo(format));
        }
    }

    public void testEveryCompressedExtensionStripsToADeclaredFormat() {
        FixtureMatrix matrix = FixtureMatrix.get();
        for (String codec : List.of("gz", "zst", "zstd", "bz2", "bz")) {
            for (String text : List.of("csv", "tsv", "ndjson")) { // dimension-copy-ok: a test that pins per-format expectations must name
                                                                  // the formats it pins
                assertThat(matrix.formats(), hasItem(FixtureMatrix.baseFormat(text + "." + codec)));
            }
        }
    }

    public void testSplitPartsIsPositive() {
        assertTrue("a split layout must produce at least two files", FixtureMatrix.get().splitParts() > 1);
    }

    /**
     * The codec list a text suite crosses, and the one value that is not the same in both builds. bz2 and
     * bz are snapshot-only, so a release build that offered them would generate fixtures its own
     * distribution cannot read.
     */
    public void testSnapshotOnlyTextCodecsAreWithheldFromAReleaseBuild() {
        FixtureMatrix matrix = FixtureMatrix.get();
        assertThat(matrix.textCodecs(true), equalTo(List.of("gz", "zst", "zstd", "bz2", "bz")));
        assertThat(matrix.textCodecs(false), equalTo(List.of("gz", "zst", "zstd")));
        assertThat(matrix.textCodecFormats("csv", false), equalTo(List.of("csv.gz", "csv.zst", "csv.zstd")));
    }

    /** A suite may narrow the parquet codec set; one that does not gets the whole declared list. */
    public void testAParquetSuiteOverrideNarrowsTheCodecSet() {
        FixtureMatrix matrix = FixtureMatrix.get();
        assertThat(matrix.parquetCodecs("parquet-compressed-multifile"), equalTo(List.of("gzip", "zstd")));
        assertThat(matrix.parquetCodecs("parquet"), equalTo(List.of("snappy", "gzip", "zstd", "lz4_raw")));
    }

    /**
     * Padding is per format, not per dataset: only csv reads the authored bytes, so the same template is
     * padded on csv and not on the formats re-rendered from it.
     */
    public void testPaddingIsDeclaredPerDatasetAndOnlyAppliesToCsv() {
        FixtureMatrix matrix = FixtureMatrix.get();
        assertThat(matrix.paddedForTemplate("employees", "csv"), equalTo(true));
        assertThat(matrix.paddedForTemplate("employees", "tsv"), equalTo(false));
        assertThat(matrix.paddedForTemplate("web_logs", "csv"), equalTo(false));
    }

    /** A standalone template is its own dataset, and carries that dataset's declared write dialect. */
    public void testAStandaloneTemplateResolvesToItselfAndItsDialect() {
        FixtureMatrix matrix = FixtureMatrix.get();
        assertThat(matrix.datasetForTemplate("employees"), equalTo("employees"));
        assertThat(matrix.writeDialectForTemplate("employees"), equalTo("brackets"));
        assertThat(matrix.writeDialectForTemplate("web_logs"), equalTo("none"));
    }

    /** An unrecognised key is silently mis-parsed by one of the file's two readers, so it is refused. */
    public void testAnUnrecognisedKeyIsRejected() {
        Properties declaration = minimalDeclaration();
        declaration.setProperty("dataset.employees.colour", "blue");
        Exception e = expectThrows(IllegalStateException.class, () -> FixtureMatrix.parse(declaration));
        assertThat(e.getMessage(), containsString("dataset.employees.colour"));
        assertThat(e.getMessage(), containsString("fixture-matrix.gradle"));
    }

    /**
     * A codec list the declaration does not carry is a missing decision rather than an empty set: an
     * empty list would silently cross nothing and report green.
     */
    public void testAMissingCodecListIsRefusedRatherThanReadAsEmpty() {
        FixtureMatrix matrix = FixtureMatrix.parse(minimalDeclaration());
        assertThat(expectThrows(IllegalStateException.class, () -> matrix.textCodecs(true)).getMessage(), containsString("codec.text"));
        assertThat(
            expectThrows(IllegalStateException.class, () -> matrix.parquetCodecs("parquet")).getMessage(),
            containsString("codec.parquet")
        );
    }

    /**
     * Padding and write dialect decide how a fixture is written and read. Guessing either wrong produces
     * bytes that parse cleanly and mean something else, so an undeclared dataset is refused.
     */
    public void testAnUndeclaredDatasetRefusesToGuessItsPaddingOrDialect() {
        Properties declaration = minimalDeclaration();
        declaration.setProperty("dataset.employees", "csv");
        declaration.setProperty("dataset.employees.reason", "rule: the other formats cannot represent it");
        FixtureMatrix matrix = FixtureMatrix.parse(declaration);
        assertThat(
            expectThrows(IllegalStateException.class, () -> matrix.paddedForTemplate("employees", "csv")).getMessage(),
            containsString("employees")
        );
        assertThat(
            expectThrows(IllegalStateException.class, () -> matrix.writeDialectForTemplate("employees")).getMessage(),
            containsString("write_dialect")
        );
    }

    /** A suite that declares no spec exclusions excludes nothing, rather than failing to look. */
    public void testASuiteWithNoDeclaredExclusionsExcludesNothing() {
        assertThat(FixtureMatrix.parse(minimalDeclaration()).excludedSpecs("csv"), equalTo(Set.of()));
    }

    /**
     * The least a declaration can say and still be one. Every key here is required by the constructor, so
     * a test that omits one fails on that rather than on what it meant to check.
     */
    private static Properties minimalDeclaration() {
        Properties declaration = new Properties();
        declaration.setProperty("formats", "csv");
        declaration.setProperty("layout.split.parts", "2");
        declaration.setProperty("layout.standalone.dir", "standalone");
        return declaration;
    }

    /**
     * A derived layout's template is not its own dataset: it is assembled from another one, and the
     * declaration says which. Without derived_from there is no dataset to ask about padding or dialect.
     */
    public void testADerivedLayoutResolvesToTheDatasetItIsBuiltFrom() {
        Properties declaration = minimalDeclaration();
        declaration.setProperty("layout.multifile.dir", "multifile");
        declaration.setProperty("layout.multifile.derived_from", "employees");
        FixtureMatrix matrix = FixtureMatrix.parse(declaration);
        assertThat(matrix.layoutFor("employees_multifile").name(), equalTo("multifile"));
        assertThat(matrix.datasetForTemplate("employees_multifile"), equalTo("employees"));
    }

    /**
     * A layout built from no single dataset has nothing to inherit, so padding and dialect answer from
     * what the generators actually write rather than throwing on a dataset that does not exist.
     */
    public void testALayoutWithNoSourceDatasetFallsBackRatherThanThrowing() {
        Properties declaration = minimalDeclaration();
        declaration.setProperty("layout.assembled.dir", "assembled");
        FixtureMatrix matrix = FixtureMatrix.parse(declaration);
        assertThat(matrix.datasetForTemplate("mixed_assembled"), nullValue());
        assertThat(matrix.paddedForTemplate("mixed_assembled", "csv"), equalTo(false));
        assertThat(matrix.writeDialectForTemplate("mixed_assembled"), equalTo("none"));
    }

    /**
     * Excluding whole spec files removes coverage wholesale, so it costs a reason. Declaring the list and
     * omitting the reason is the shape that reads as deliberate and is not.
     */
    public void testAWholeFileExclusionMustDeclareItsReason() {
        Properties unexplained = minimalDeclaration();
        unexplained.setProperty("suite.csv.specs.exclude", "some-spec");
        assertThat(
            expectThrows(IllegalStateException.class, () -> FixtureMatrix.parse(unexplained).excludedSpecs("csv")).getMessage(),
            containsString("declares no reason")
        );

        Properties explained = minimalDeclaration();
        explained.setProperty("suite.csv.specs.exclude", "some-spec, other-spec");
        explained.setProperty("suite.csv.specs.exclude.reason", "rule: the reader cannot express these at all");
        assertThat(FixtureMatrix.parse(explained).excludedSpecs("csv"), equalTo(Set.of("some-spec", "other-spec")));
    }

    /**
     * A dialect a dataset cannot be written in is a skip, and a skip with no reason is indistinguishable
     * from an oversight. The delimiter-qualified form stacks on the unqualified one rather than replacing
     * it -- a dataset unrepresentable in a dialect is unrepresentable whatever the delimiter.
     */
    public void testUnrepresentableDialectsStackAndMustBeExplained() {
        Properties unexplained = minimalDeclaration();
        unexplained.setProperty("dataset.employees.unrepresentable_dialects", "plain");
        assertThat(
            expectThrows(IllegalStateException.class, () -> FixtureMatrix.parse(unexplained).unrepresentableDialects("employees"))
                .getMessage(),
            containsString("with no reason")
        );

        Properties explained = minimalDeclaration();
        explained.setProperty("dataset.employees.unrepresentable_dialects", "plain");
        explained.setProperty("dataset.employees.unrepresentable_dialects.reason", "rule: its values carry the delimiter");
        explained.setProperty("dataset.employees.unrepresentable_dialects.semicolon", "escaped");
        explained.setProperty("dataset.employees.unrepresentable_dialects.semicolon.reason", "rule: a semicolon appears in data");
        FixtureMatrix matrix = FixtureMatrix.parse(explained);
        assertThat(matrix.unrepresentableDialects("employees"), equalTo(Set.of("plain")));
        assertThat(matrix.unrepresentableDialects("employees", "semicolon"), equalTo(Set.of("plain", "escaped")));
    }

    /**
     * A dataset restricted to some formats is a gap in the crossing, so it carries a reason, and it cannot
     * name a format the declaration does not have -- that entry would restrict nothing.
     */
    public void testARestrictedDatasetNeedsAReasonAndRealFormats() {
        Properties noReason = minimalDeclaration();
        noReason.setProperty("dataset.employees", "csv");
        assertThat(
            expectThrows(IllegalStateException.class, () -> FixtureMatrix.parse(noReason)).getMessage(),
            containsString("declares no [dataset.employees.reason]")
        );

        Properties unknownFormat = minimalDeclaration();
        unknownFormat.setProperty("dataset.employees", "avro");
        unknownFormat.setProperty("dataset.employees.reason", "rule: only csv can carry it");
        assertThat(
            expectThrows(IllegalStateException.class, () -> FixtureMatrix.parse(unknownFormat)).getMessage(),
            containsString("names unknown format")
        );
    }

    /** A dataset that declares no unrepresentable dialects has none, rather than failing to look. */
    public void testADatasetWithNoDeclaredUnrepresentableDialectsHasNone() {
        assertThat(FixtureMatrix.parse(minimalDeclaration()).unrepresentableDialects("employees"), equalTo(Set.of()));
    }

    /**
     * A blank list is the same missing decision as an absent one, and reads more like a deliberate empty
     * set -- which is exactly the silent pass the declaration exists to prevent.
     */
    public void testABlankCodecListIsRefusedLikeAnAbsentOne() {
        Properties declaration = minimalDeclaration();
        declaration.setProperty("codec.text", "   ");
        declaration.setProperty("codec.parquet", "");
        FixtureMatrix matrix = FixtureMatrix.parse(declaration);
        assertThat(expectThrows(IllegalStateException.class, () -> matrix.textCodecs(true)).getMessage(), containsString("codec.text"));
        assertThat(
            expectThrows(IllegalStateException.class, () -> matrix.parquetCodecs("parquet")).getMessage(),
            containsString("codec.parquet")
        );
    }

    /** A blank exclusion list excludes nothing, and needs no reason because it removes no coverage. */
    public void testABlankSpecExclusionListExcludesNothing() {
        Properties declaration = minimalDeclaration();
        declaration.setProperty("suite.csv.specs.exclude", "  ");
        assertThat(FixtureMatrix.parse(declaration).excludedSpecs("csv"), equalTo(Set.of()));
    }
}
