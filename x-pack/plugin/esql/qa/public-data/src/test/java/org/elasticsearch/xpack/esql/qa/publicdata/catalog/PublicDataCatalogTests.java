/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.util.List;

/** Round-trips the fixture catalog through the YAML loader and checks every model feature. */
public class PublicDataCatalogTests extends ESTestCase {

    public void testLoadFixtureCatalog() {
        PublicDataCatalog catalog = PublicDataCatalog.loadFromClasspath("/fixture-catalog.yml");
        assertEquals(1, catalog.version());
        assertEquals(2, catalog.corpora().size());
        assertEquals(4, catalog.gaps().size());

        CorpusSpec fixture = catalog.corpus("fixture");
        assertEquals(CorpusSpec.Kind.WORKLOAD, fixture.kind());
        assertEquals(Scale.SMALL, fixture.scale());
        assertEquals(DataQuality.CLEAN, fixture.quality());
        assertEquals("fixture-workload.csv-spec", fixture.workload());
        assertEquals(3, fixture.variants().size());
        assertEquals(2, fixture.activeVariants().size());

        VariantSpec reference = fixture.variants().get(0);
        assertEquals("fixture-s3-parquet-snappy-single", reference.label());
        assertEquals("pd_ds_s3_eu_central_1", reference.datasetSourceName());
        assertTrue(reference.isReference());
        assertTrue(reference.seekable());
        assertTrue(reference.supportsGlob());
        assertEquals("anonymous", reference.dataSourceSettings().get("auth"));
        assertEquals("HEAD", reference.pin().method());
        assertEquals(Instant.parse("2026-08-11T09:14:00Z"), reference.pin().verifiedAt());
        assertFalse(reference.pin().degenerate());
        assertEquals(1, reference.pin().samples().size());
        assertEquals("abc123-4", reference.pin().samples().get(0).etag());

        VariantSpec shards = fixture.variants().get(1);
        assertEquals("fixture-s3-csv-gzip-shards", shards.label());
        assertEquals(Layout.UNIFORM_SHARDS, shards.layout());
        assertTrue(shards.layout().multiFile());
        assertFalse(shards.seekable());
        assertEquals(Boolean.FALSE, shards.datasetSettings().get("header_row"));
        assertEquals("false", shards.datasetMappings().get("dynamic"));
        assertEquals(List.of("q1_scan", "q2_agg", "q3_topn", "q4_limit").size(), shards.querySubset().size());
        assertEquals("LIST", shards.pin().method());
        assertEquals(3, shards.pin().objectCount());

        VariantSpec backup = fixture.variants().get(2);
        assertEquals(Provider.HTTPS, backup.provider());
        assertTrue(backup.isBackup());
        assertFalse(backup.active());
        assertFalse(backup.supportsGlob());

        CorpusSpec dirty = catalog.corpus("fixture-dirty");
        assertEquals(CorpusSpec.Kind.FAILURE_ONLY, dirty.kind());
        assertNull(dirty.workload());
        VariantSpec mislabeled = dirty.variants().get(0);
        assertNotNull(mislabeled.expectFailure());
        assertTrue(mislabeled.expectFailure().statusMatches(400));
        assertTrue(mislabeled.expectFailure().statusMatches(422));
        assertFalse(mislabeled.expectFailure().statusMatches(500));
    }

    public void testUnknownCorpusFailsLoudly() {
        PublicDataCatalog catalog = PublicDataCatalog.loadFromClasspath("/fixture-catalog.yml");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> catalog.corpus("nope"));
        assertTrue(e.getMessage().contains("nope"));
    }

    public void testMissingResourceFailsLoudly() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PublicDataCatalog.loadFromClasspath("/no-such-catalog.yml")
        );
        assertTrue(e.getMessage().contains("no-such-catalog.yml"));
    }

    public void testVariantsForWorkload() {
        PublicDataCatalog catalog = PublicDataCatalog.loadFromClasspath("/fixture-catalog.yml");
        assertEquals(3, catalog.variantsForWorkload("fixture-workload.csv-spec").size());
        assertEquals(0, catalog.variantsForWorkload("unclaimed.csv-spec").size());
    }
}
