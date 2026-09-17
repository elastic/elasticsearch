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

import java.util.List;
import java.util.Map;

/** The inventory must disclose trims and derive blocked/gap statuses; no cell may read better than it is. */
public class CoverageInventoryTests extends ESTestCase {

    private static CoverageInventory inventory() {
        PublicDataCatalog catalog = PublicDataCatalog.loadFromClasspath("/fixture-catalog.yml");
        WorkloadSpec workload = WorkloadSpec.loadFromClasspath("/fixture-workload.csv-spec");
        return new CoverageInventory(catalog, Map.of("fixture-workload.csv-spec", workload));
    }

    public void testCells() {
        List<CoverageInventory.Cell> cells = inventory().cells();

        CoverageInventory.Cell reference = cell(cells, "fixture-s3-parquet-snappy-single");
        assertEquals("covered", reference.status());
        assertEquals(6, reference.tests());
        assertTrue(reference.crossValidated());
        assertTrue(reference.detail().contains("defect-disabled"));

        CoverageInventory.Cell shards = cell(cells, "fixture-s3-csv-gzip-shards");
        assertEquals("covered", shards.status());
        assertEquals(4, shards.tests());
        assertFalse(shards.crossValidated());
        assertTrue(shards.detail(), shards.detail().contains("subset: 4/6"));

        CoverageInventory.Cell backup = cell(cells, "fixture-https-parquet-snappy-single");
        assertEquals("gap", backup.status());

        CoverageInventory.Cell failure = cell(cells, "fixture-dirty-s3-csv-uncompressed-single");
        assertEquals("covered", failure.status());
        assertTrue(failure.detail().contains("expected-failure"));

        CoverageInventory.Cell declaredGap = cell(cells, "providers-pending");
        assertEquals("gap", declaredGap.status());
        assertTrue(declaredGap.detail().contains("provider=gcs"));
    }

    public void testRenderings() {
        String markdown = inventory().toMarkdown();
        assertTrue(markdown.contains("| fixture | fixture-s3-csv-gzip-shards | covered | 4 | no | subset: 4/6 |"));
        String json = inventory().toJson();
        assertTrue(json.contains("\"cell\": \"fixture-s3-csv-gzip-shards\""));
        assertTrue(json.contains("\"status\": \"covered\""));
    }

    private static CoverageInventory.Cell cell(List<CoverageInventory.Cell> cells, String label) {
        return cells.stream().filter(c -> c.label().equals(label)).findFirst().orElseThrow(() -> new AssertionError("no cell " + label));
    }
}
