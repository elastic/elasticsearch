/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.publicdata.validation;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.CoverageReportGenerator;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataCatalog;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataCatalogValidator;

import java.util.List;

/**
 * The actual gate the {@code catalogValidation} Gradle task exists to run: parses the real, checked-in
 * {@code public-data-catalog.yml} and every source's real csv-spec file, and fails if
 * {@link PublicDataCatalogValidator} reports any problem. Runs with no network access and no cluster.
 * <p>
 * Also asserts the coverage report generator runs cleanly end to end against the real catalog, since a
 * broken report generator would otherwise go unnoticed until someone runs the {@code
 * publicDataCoverageReport} task by hand.
 */
public class PublicDataCatalogOfflineValidationTests extends ESTestCase {

    public void testRealCatalogAndSpecsHaveNoProblems() {
        PublicDataCatalog catalog = PublicDataCatalog.loadFromClasspath();
        List<String> problems = PublicDataCatalogValidator.validate(catalog);
        assertEquals(String.join("\n", problems), List.of(), problems);
    }

    public void testCoverageReportGeneratesEndToEnd() throws Exception {
        PublicDataCatalog catalog = PublicDataCatalog.loadFromClasspath();
        String report = CoverageReportGenerator.generate(catalog);
        assertTrue(report.contains("# Public-data ES|QL suite: dimension coverage"));
        for (var source : catalog.sources()) {
            assertTrue(report.contains(source.displayName()));
        }
    }
}
