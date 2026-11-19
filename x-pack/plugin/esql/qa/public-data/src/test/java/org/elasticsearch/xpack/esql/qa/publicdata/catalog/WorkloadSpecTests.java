/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

/** The offline spec parser must see exactly what the runtime CsvSpecReader path sees, plus provenance. */
public class WorkloadSpecTests extends ESTestCase {

    public void testParseFixtureWorkload() {
        WorkloadSpec workload = WorkloadSpec.loadFromClasspath("/fixture-workload.csv-spec");
        assertEquals("fixture-workload.csv-spec", workload.fileName());
        assertEquals(6, workload.tests().size());

        WorkloadSpec.TestSpec scan = workload.tests().get(0);
        assertEquals("q1_scan", scan.name());
        assertEquals("q1_scan", scan.baseName());
        assertFalse(scan.disabled());
        assertEquals("scan", scan.readShape());
        assertEquals("10", scan.maxRows());
        assertEquals(2, scan.expectedRowCount());
        assertEquals(List.of("required_capability: dataset_in_from_command").size(), scan.requiredCapabilities().size());
        assertEquals("dataset_in_from_command", scan.requiredCapabilities().get(0));
        assertEquals(1, scan.datasetDirectives().size());
        assertTrue(scan.datasetDirectives().get(0).contains("{{corpus}}"));
        assertTrue(scan.query().startsWith("FROM fixture"));
        assertEquals("fixture", scan.provenance().get("corpus"));
        assertEquals("fixture-s3-parquet-snappy-single", scan.provenance().get("reference-variant"));
        assertTrue(scan.provenance().get("oracle-sql").startsWith("SELECT"));

        WorkloadSpec.TestSpec agg = workload.tests().get(1);
        assertEquals("aggregate", agg.readShape());
        assertEquals(1, agg.expectedRowCount());

        WorkloadSpec.TestSpec multi = workload.tests().get(2);
        assertEquals("q2_aggMulti", multi.name());
        assertEquals(
            List.of("dataset: fixture_left: \"{{corpus:left}}\"", "dataset: fixture_right: \"{{corpus:right}}\""),
            multi.datasetDirectives()
        );
        assertEquals(workload.tests().get(1).expectedTable(), multi.expectedTable());

        WorkloadSpec.TestSpec defect = workload.tests().get(5);
        assertEquals("q5_defect-Ignore", defect.name());
        assertTrue(defect.disabled());
        assertEquals("q5_defect", defect.baseName());
        assertTrue(defect.provenance().containsKey("defect"));
        assertEquals("fixture-s3-csv-gzip-shards", defect.provenance().get("defect-variant"));
    }

    public void testTrailingTestWithoutBodyFailsLoudly() {
        List<String> lines = List.of("q_orphan", "required_capability: dataset_in_from_command");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> WorkloadSpec.parse("broken.csv-spec", lines));
        assertTrue(e.getMessage().contains("q_orphan"));
    }

    public void testWarningLinesAreNotCountedAsRows() {
        List<String> lines = List.of(
            "// corpus: fixture",
            "q_warned",
            "required_capability: dataset_in_from_command",
            "dataset: fixture: \"{{corpus}}\"",
            "FROM fixture | KEEP a | SORT a ASC | LIMIT 1;",
            "warning: something happened",
            "a:integer",
            "1",
            ";"
        );
        WorkloadSpec workload = WorkloadSpec.parse("warned.csv-spec", lines);
        assertEquals(1, workload.tests().get(0).expectedRowCount());
    }

    public void testProseCommentsAreNotProvenance() {
        List<String> lines = List.of(
            "// This preamble: is prose, not a provenance key",
            "// corpus: fixture",
            "q_prose",
            "required_capability: dataset_in_from_command",
            "dataset: fixture: \"{{corpus}}\"",
            "FROM fixture | STATS c = COUNT(*);",
            "c:long",
            "1",
            ";"
        );
        WorkloadSpec.TestSpec test = WorkloadSpec.parse("prose.csv-spec", lines).tests().get(0);
        assertEquals("fixture", test.provenance().get("corpus"));
        assertFalse(test.provenance().containsKey("this preamble"));
    }
}
