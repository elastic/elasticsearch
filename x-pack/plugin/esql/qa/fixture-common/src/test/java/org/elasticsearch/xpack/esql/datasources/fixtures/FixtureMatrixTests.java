/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.fixtures;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

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
        assertThat(matrix.formats(), contains("csv", "tsv", "ndjson", "orc", "parquet"));
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
            for (String text : List.of("csv", "tsv", "ndjson")) {
                assertThat(matrix.formats(), hasItem(FixtureMatrix.baseFormat(text + "." + codec)));
            }
        }
    }

    public void testSplitPartsIsPositive() {
        assertTrue("a split layout must produce at least two files", FixtureMatrix.get().splitParts() > 1);
    }
}
