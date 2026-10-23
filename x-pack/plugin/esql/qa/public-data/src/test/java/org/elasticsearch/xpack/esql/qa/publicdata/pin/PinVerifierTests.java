/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.pin;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PinSpec;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataCatalog;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.VariantSpec;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Pin verification with a stub probe. The {@link PinProbe} interface has no body-fetching method
 * at all — metadata-only is structural — and the stub additionally records exactly which calls
 * were made, proving verification stays within HEAD/LIST.
 */
public class PinVerifierTests extends ESTestCase {

    /** Stub probe: canned metadata per key/prefix; records calls; throws on anything unexpected. */
    private static class StubProbe implements PinProbe {
        final Map<String, ObjectMetadata> headByUri;
        final Map<String, List<ObjectMetadata>> listByPrefix;
        final List<String> calls = new java.util.ArrayList<>();

        StubProbe(Map<String, ObjectMetadata> headByUri, Map<String, List<ObjectMetadata>> listByPrefix) {
            this.headByUri = headByUri;
            this.listByPrefix = listByPrefix;
        }

        @Override
        public ObjectMetadata head(String uri) throws IOException {
            calls.add("HEAD " + uri);
            ObjectMetadata metadata = headByUri.get(uri);
            if (metadata == null) {
                throw new IOException("unexpected HEAD " + uri);
            }
            return metadata;
        }

        @Override
        public List<ObjectMetadata> list(String uri, int maxKeys) throws IOException {
            calls.add("LIST " + uri);
            List<ObjectMetadata> listing = listByPrefix.get(uri);
            if (listing == null) {
                throw new IOException("unexpected LIST " + uri);
            }
            return listing;
        }
    }

    private static VariantSpec referenceVariant() {
        return PublicDataCatalog.loadFromClasspath("/fixture-catalog.yml").corpus("fixture").variants().get(0);
    }

    private static VariantSpec shardsVariant() {
        return PublicDataCatalog.loadFromClasspath("/fixture-catalog.yml").corpus("fixture").variants().get(1);
    }

    public void testHeadPinMatches() {
        StubProbe probe = new StubProbe(
            Map.of(
                "s3://example-bucket/data/fixture.parquet",
                new ObjectMetadata("data/fixture.parquet", "abc123-4", 12345678, "whenever")
            ),
            Map.of()
        );
        PinVerifier verifier = new PinVerifier(v -> probe);
        PinVerifier.VariantResult result = verifier.verifyVariant(referenceVariant());
        assertEquals(PinVerifier.Status.OK, result.status());
        assertEquals(List.of("HEAD s3://example-bucket/data/fixture.parquet"), probe.calls);
    }

    /**
     * A volatile pin exists for publishers that rewrite the same logical object nightly (NOAA's
     * by_year CSVs). ETag churn there is noise, and reporting it every run would teach the reader to
     * ignore PIN_DRIFT — so it is deliberately not compared, while a size move beyond the declared
     * tolerance still is.
     */
    public void testVolatilePinIgnoresEtagButNotLargeSizeMoves() {
        VariantSpec variant = volatileVariant();
        StubProbe rewritten = new StubProbe(
            Map.of(
                "s3://example-bucket/data/fixture.parquet",
                // same object, re-published: new ETag, a handful of bytes different
                new ObjectMetadata("data/fixture.parquet", "COMPLETELY-DIFFERENT", 12345678 + 7, "whenever")
            ),
            Map.of()
        );
        assertEquals(PinVerifier.Status.OK, new PinVerifier(v -> rewritten).verifyVariant(variant).status());

        StubProbe halved = new StubProbe(
            Map.of(
                "s3://example-bucket/data/fixture.parquet",
                new ObjectMetadata("data/fixture.parquet", "abc123-4", 12345678 / 2, "whenever")
            ),
            Map.of()
        );
        PinVerifier.VariantResult result = new PinVerifier(v -> halved).verifyVariant(variant);
        assertEquals(PinVerifier.Status.PIN_DRIFT, result.status());
        assertTrue(result.details().toString(), result.details().get(0).contains("tolerance"));
    }

    private static VariantSpec volatileVariant() {
        VariantSpec reference = referenceVariant();
        PinSpec pin = reference.pin();
        return new VariantSpec(
            reference.corpusId(),
            reference.provider(),
            reference.format(),
            reference.codec(),
            reference.layout(),
            reference.partitioning(),
            reference.region(),
            reference.resource(),
            reference.subResources(),
            reference.dataSourceSettings(),
            reference.datasetSettings(),
            reference.datasetMappings(),
            new PinSpec(pin.method(), pin.verifiedAt(), pin.objectCount(), pin.totalBytes(), pin.samples(), true, 10),
            reference.tags(),
            reference.querySubset(),
            "upstream re-publishes this object nightly",
            reference.expectFailure(),
            reference.caseId(),
            reference.disabledReason()
        );
    }

    public void testHeadPinDriftOnEtagAndSize() {
        StubProbe probe = new StubProbe(
            Map.of(
                "s3://example-bucket/data/fixture.parquet",
                new ObjectMetadata("data/fixture.parquet", "recompacted-1", 999, "whenever")
            ),
            Map.of()
        );
        PinVerifier.VariantResult result = new PinVerifier(v -> probe).verifyVariant(referenceVariant());
        assertEquals(PinVerifier.Status.PIN_DRIFT, result.status());
        assertEquals(2, result.details().size());
    }

    public void testListPinDriftOnObjectCount() {
        StubProbe probe = new StubProbe(
            Map.of(),
            Map.of(
                "s3://example-bucket/shards/part_",
                List.of(
                    new ObjectMetadata("shards/part_0.csv.gz", "e0", 100, null),
                    new ObjectMetadata("shards/part_1.csv.gz", "e1", 100, null),
                    new ObjectMetadata("shards/part_2.csv.gz", "e2", 100, null),
                    new ObjectMetadata("shards/part_3.csv.gz", "e3", 100, null)
                )
            )
        );
        PinVerifier.VariantResult result = new PinVerifier(v -> probe).verifyVariant(shardsVariant());
        assertEquals(PinVerifier.Status.PIN_DRIFT, result.status());
        assertTrue(result.details().toString(), result.details().stream().anyMatch(d -> d.contains("object count changed")));
    }

    public void testListPinMatches() {
        StubProbe probe = new StubProbe(
            Map.of(),
            Map.of(
                "s3://example-bucket/shards/part_",
                List.of(
                    new ObjectMetadata("shards/part_0.csv.gz", "e0", 100, null),
                    new ObjectMetadata("shards/part_1.csv.gz", "e1", 100, null),
                    new ObjectMetadata("shards/part_2.csv.gz", "e2", 100, null)
                )
            )
        );
        PinVerifier.VariantResult result = new PinVerifier(v -> probe).verifyVariant(shardsVariant());
        assertEquals(result.details().toString(), PinVerifier.Status.OK, result.status());
        assertEquals(List.of("LIST s3://example-bucket/shards/part_"), probe.calls);
    }

    public void testUnreachableStoreIsAttributedAsInfra() {
        StubProbe probe = new StubProbe(Map.of(), Map.of());
        PinVerifier.VariantResult result = new PinVerifier(v -> probe).verifyVariant(referenceVariant());
        assertEquals(PinVerifier.Status.UNREACHABLE, result.status());
    }

    public void testBackupVariantsAreSkipped() {
        VariantSpec backup = PublicDataCatalog.loadFromClasspath("/fixture-catalog.yml").corpus("fixture").variants().get(2);
        PinVerifier.VariantResult result = new PinVerifier(v -> { throw new AssertionError("no probe for backups"); }).verifyVariant(
            backup
        );
        assertEquals(PinVerifier.Status.SKIPPED, result.status());
    }

    public void testListPrefixStripsGlobMetacharacters() {
        assertEquals("s3://b/shards/part_", PinVerifier.listPrefix("s3://b/shards/part_*.csv.gz"));
        assertEquals("s3://b/x/y_", PinVerifier.listPrefix("s3://b/x/y_?.parquet"));
        assertEquals("s3://b/plain.parquet", PinVerifier.listPrefix("s3://b/plain.parquet"));
        // braces containing commas cannot occur: the engine splits resources on commas first,
        // and the validator's per-entry scheme rule rejects the schemeless fragments
    }

    public void testListPrefixOfCommaListIsTheCommonPrefix() {
        assertEquals("s3://b/dir/file-", PinVerifier.listPrefix("s3://b/dir/file-a.csv,s3://b/dir/file-b.csv"));
        assertEquals("s3://b/dir/", PinVerifier.listPrefix("s3://b/dir/alpha.csv,s3://b/dir/beta.csv,s3://b/dir/x_*.csv"));
    }
}
