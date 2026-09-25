/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.CorpusSpec;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataCatalog;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.VariantSpec;

import java.util.List;

/**
 * The filters are pure over the parsed catalog (system-property reading is a thin shim), so they
 * are tested by constructing filter records directly — no global state mutation.
 */
public class PublicDataFiltersTests extends ESTestCase {

    private static final PublicDataFilters NO_FILTERS = filters(null, null, null, null, null, 0);

    private static PublicDataFilters filters(String source, String variant, String provider, String format, String codec, int maxPerSpec) {
        return new PublicDataFilters(
            source,
            null,
            variant,
            provider,
            format,
            codec,
            null,
            null,
            null,
            null,
            false,
            maxPerSpec,
            3,
            "build/public-data-results",
            "8g"
        );
    }

    private static PublicDataCatalog catalog() {
        return PublicDataCatalog.loadFromClasspath("/fixture-catalog.yml");
    }

    public void testNoFiltersEnumerateAllActiveWorkloadVariants() {
        CorpusSpec fixture = catalog().corpus("fixture");
        List<VariantSpec> variants = NO_FILTERS.variants(fixture);
        // 3 catalogued variants: the HTTPS one is a backup entry and never enumerated
        assertEquals(2, variants.size());
        assertTrue(variants.stream().noneMatch(VariantSpec::isBackup));
    }

    public void testFailureVariantsAreNeverWorkloadParameters() {
        CorpusSpec dirty = catalog().corpus("fixture-dirty");
        assertEquals(0, NO_FILTERS.variants(dirty).size());
    }

    public void testSourceFilter() {
        assertTrue(filters("fixture", null, null, null, null, 0).matches(catalog().corpus("fixture")));
        assertFalse(filters("other", null, null, null, null, 0).matches(catalog().corpus("fixture")));
    }

    public void testVariantGlobFilter() {
        CorpusSpec fixture = catalog().corpus("fixture");
        List<VariantSpec> parquetOnly = filters(null, "*-s3-parquet-*", null, null, null, 0).variants(fixture);
        assertEquals(1, parquetOnly.size());
        assertEquals("fixture-s3-parquet-snappy-single", parquetOnly.get(0).label());
    }

    public void testDimensionFilters() {
        CorpusSpec fixture = catalog().corpus("fixture");
        assertEquals(1, filters(null, null, null, "csv", null, 0).variants(fixture).size());
        assertEquals(1, filters(null, null, null, null, "gzip", 0).variants(fixture).size());
        assertEquals(2, filters(null, null, "s3", null, null, 0).variants(fixture).size());
        assertEquals(0, filters(null, null, "gcs", null, null, 0).variants(fixture).size());
    }

    public void testMaxVariantsPerSpecCap() {
        CorpusSpec fixture = catalog().corpus("fixture");
        assertEquals(1, filters(null, null, null, null, null, 1).variants(fixture).size());
    }

    public void testFailIfEmptyListsAvailableLabels() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> filters("nonexistent", null, null, null, null, 0).failIfEmpty(List.of(), catalog())
        );
        assertTrue(e.getMessage().contains("matched no tests"));
        assertTrue(e.getMessage().contains("source=nonexistent"));
        assertTrue(e.getMessage().contains("fixture-s3-parquet-snappy-single"));
    }

    public void testFailIfEmptyPassesWithParameters() {
        // note: List.of(new Object[0]) would expand as empty varargs; build the list explicitly
        List<Object[]> parameters = new java.util.ArrayList<>();
        parameters.add(new Object[0]);
        NO_FILTERS.failIfEmpty(parameters, catalog());
    }

    public void testGlobToPattern() {
        assertTrue(PublicDataFilters.globToPattern("*-s3-*").matcher("clickbench-s3-parquet-snappy-single").matches());
        assertFalse(PublicDataFilters.globToPattern("*-gcs-*").matcher("clickbench-s3-parquet-snappy-single").matches());
        assertTrue(PublicDataFilters.globToPattern("clickbench-s?-*").matcher("clickbench-s3-parquet-snappy-single").matches());
    }
}
