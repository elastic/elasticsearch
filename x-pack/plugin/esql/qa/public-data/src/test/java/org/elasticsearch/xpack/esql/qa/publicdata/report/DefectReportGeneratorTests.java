/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.report;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataCatalog;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.WorkloadSpec;

import java.util.Map;

/** The fixture carries one -Ignore'd defect test; the report must surface it with its provenance. */
public class DefectReportGeneratorTests extends ESTestCase {

    public void testFixtureDefectIsReported() {
        PublicDataCatalog catalog = PublicDataCatalog.loadFromClasspath("/fixture-catalog.yml");
        Map<String, WorkloadSpec> workloads = Map.of(
            "fixture-workload.csv-spec",
            WorkloadSpec.loadFromClasspath("/fixture-workload.csv-spec")
        );
        String report = DefectReportGenerator.generate(catalog, workloads);
        assertTrue(report.contains("q5_defect (fixture)"));
        assertTrue(report.contains("fixture defect entry so unit tests cover the -Ignore + defect block path"));
        assertTrue(report.contains("**affected variant:** fixture-s3-csv-gzip-shards"));
        assertTrue(report.contains("Total: 1 defect(s)."));
    }
}
