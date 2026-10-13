/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.DeclaredReadSpec;
import org.elasticsearch.xpack.esql.datasources.DefinitionVersion;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ExternalRelationSerializationTests extends AbstractLogicalPlanSerializationTests<ExternalRelation> {

    public static ExternalRelation randomExternalRelation() {
        String sourcePath = "s3://bucket/" + randomAlphaOfLength(8) + ".parquet";
        String sourceType = randomFrom("parquet", "csv", "file", "iceberg");
        List<Attribute> output = randomFieldAttributes(1, 5, false);
        Map<String, Object> config = randomBoolean() ? Map.of() : Map.of("endpoint", "https://s3.example.com");
        Map<String, Object> sourceMetadata = randomBoolean() ? Map.of() : randomSourceMetadataWithStats();
        SimpleSourceMetadata metadata = new SimpleSourceMetadata(output, sourceType, sourcePath, null, null, sourceMetadata, config);
        return new ExternalRelation(randomSource(), sourcePath, metadata, output, FileList.UNRESOLVED, Map.of());
    }

    private static Map<String, Object> randomSourceMetadataWithStats() {
        Map<String, Object> map = new HashMap<>();
        map.put("schema_version", 1);
        if (randomBoolean()) {
            map.put("_stats.row_count", randomLongBetween(0, 100_000));
            map.put("_stats.size_bytes", randomLongBetween(1000, 10_000_000));
            String intCol = randomAlphaOfLength(5);
            map.put("_stats.columns." + intCol + ".null_count", randomLongBetween(0, 1000));
            map.put("_stats.columns." + intCol + ".min", randomIntBetween(0, 100));
            map.put("_stats.columns." + intCol + ".max", randomIntBetween(100, 1000));
            String strCol = randomAlphaOfLength(5);
            map.put("_stats.columns." + strCol + ".null_count", randomLongBetween(0, 100));
            map.put("_stats.columns." + strCol + ".min", randomAlphaOfLength(5));
            map.put("_stats.columns." + strCol + ".max", randomAlphaOfLength(5));
        }
        return map;
    }

    @Override
    protected ExternalRelation createTestInstance() {
        return randomExternalRelation();
    }

    @Override
    protected ExternalRelation mutateInstance(ExternalRelation instance) throws IOException {
        String sourcePath = instance.sourcePath();
        List<Attribute> output = instance.output();
        String sourceType = instance.metadata().sourceType();
        Map<String, Object> config = instance.metadata().config();
        Map<String, Object> sourceMetadata = instance.metadata().sourceMetadata();

        switch (between(0, 2)) {
            case 0 -> sourcePath = randomValueOtherThan(sourcePath, () -> "s3://bucket/" + randomAlphaOfLength(8) + ".parquet");
            case 1 -> output = randomValueOtherThan(output, () -> randomFieldAttributes(1, 5, false));
            case 2 -> sourceType = randomValueOtherThan(sourceType, () -> randomFrom("parquet", "csv", "file", "iceberg"));
            default -> throw new IllegalStateException();
        }
        SimpleSourceMetadata metadata = new SimpleSourceMetadata(output, sourceType, sourcePath, null, null, sourceMetadata, config);
        return new ExternalRelation(instance.source(), sourcePath, metadata, output, FileList.UNRESOLVED, Map.of());
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }

    /**
     * Exercises the wire-encoding branch where the projected {@code output} is narrower than the
     * source's full positional column layout — the case that triggers post-optimizer-narrowing
     * (e.g. a STATS aggregation projects external columns down to a single aggregated column).
     * The serialized form must carry the full {@code metadata.schema()} so that the data-node
     * rebuild does not mistake the projection for the source schema, which would lose the
     * coordinator's reconciled per-source layout.
     */
    public void testSchemaWiderThanOutputRoundTrips() throws IOException {
        List<Attribute> fullSchema = randomFieldAttributes(3, 6, false);
        // Pick a strict subset for the narrowed output.
        List<Attribute> narrowedOutput = List.of(fullSchema.get(0));
        SimpleSourceMetadata metadata = new SimpleSourceMetadata(
            fullSchema,
            "csv",
            "s3://bucket/" + randomAlphaOfLength(8) + ".csv",
            null,
            null,
            Map.of(),
            Map.of()
        );
        ExternalRelation original = new ExternalRelation(
            randomSource(),
            metadata.location(),
            metadata,
            narrowedOutput,
            FileList.UNRESOLVED,
            Map.of()
        );

        ExternalRelation roundTripped = copyInstance(original);

        assertEquals(narrowedOutput, roundTripped.output());
        assertEquals(
            "metadata.schema() must preserve the source's full column layout across the wire, " + "not the (potentially narrower) output",
            fullSchema,
            roundTripped.metadata().schema()
        );
    }

    /**
     * The dataset-tier definition version is coordinator-only and must not reach a data node. THIS is the route it
     * would take: the Mapper wraps this relation into a {@code FragmentExec} and the data node expands it locally,
     * so {@code ExternalSourceExec.writeTo} - which strips the same key - never runs on that path.
     * <p>
     * Why it matters rather than being tidiness: a data node builds its storage provider from this config map and
     * {@code StorageProviderCache} keys on the whole map, which is why {@code FRAMEWORK_KEYS} strips the file-tier
     * version. An older node's {@code FRAMEWORK_KEYS} does not know this key, so it would reach that cache key and
     * fragment the client pool per dataset definition - a pool that throws at its ceiling rather than degrading.
     */
    public void testDatasetVersionDoesNotRideTheRelationOntoTheWire() throws IOException {
        List<Attribute> output = randomFieldAttributes(1, 3, false);
        Map<String, Object> config = new HashMap<>();
        config.put("format", "csv");
        config.put(DefinitionVersion.DATASET_CONFIG_KEY, "0123456789abcdef0123456789abcdef");
        SimpleSourceMetadata metadata = new SimpleSourceMetadata(output, "csv", "s3://bucket/x.csv", null, null, Map.of(), config);
        ExternalRelation original = new ExternalRelation(
            randomSource(),
            metadata.location(),
            metadata,
            output,
            FileList.UNRESOLVED,
            Map.of()
        );

        ExternalRelation roundTripped = copyInstance(original);

        assertFalse(
            "the coordinator-only dataset version must not reach a data node",
            roundTripped.metadata().config().containsKey(DefinitionVersion.DATASET_CONFIG_KEY)
        );
        assertEquals("and the rest of the config still travels", "csv", roundTripped.metadata().config().get("format"));
    }

    /** The declared read-instructions ride the wire on a node supporting {@code dataset_declared_schema}. */
    public void testDeclaredReadSpecSurvivesRoundTripWhenSupported() throws IOException {
        DeclaredReadSpec spec = DeclaredReadSpec.of(Map.of("id", "emp_no"));
        List<Attribute> output = randomFieldAttributes(1, 3, false);
        SimpleSourceMetadata metadata = new SimpleSourceMetadata(output, "csv", "s3://bucket/x.csv", null, null, Map.of(), Map.of());
        ExternalRelation original = new ExternalRelation(
            randomSource(),
            metadata.location(),
            metadata,
            output,
            FileList.UNRESOLVED,
            Map.of(),
            null,
            List.of(),
            spec
        );
        ExternalRelation roundTripped = copyInstance(original, TransportVersion.current());
        assertThat(roundTripped.declaredReadSpec(), equalTo(spec));
    }

    /**
     * Serializing a NON-empty spec toward a node predating {@code dataset_declared_schema} is rejected loudly rather
     * than silently dropped — dropping it would return wrong rows (physical names) on the old node.
     */
    public void testDeclaredReadSpecRejectedForOlderTransportVersion() throws IOException {
        DeclaredReadSpec spec = DeclaredReadSpec.of(Map.of("id", "emp_no"));
        TransportVersion before = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("dataset_declared_schema"));
        List<Attribute> output = randomFieldAttributes(1, 3, false, before);
        SimpleSourceMetadata metadata = new SimpleSourceMetadata(output, "csv", "s3://bucket/x.csv", null, null, Map.of(), Map.of());
        ExternalRelation original = new ExternalRelation(
            randomSource(),
            metadata.location(),
            metadata,
            output,
            FileList.UNRESOLVED,
            Map.of(),
            null,
            List.of(),
            spec
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> copyInstance(original, before));
        assertThat(e.getMessage(), containsString("not supported on all nodes"));
    }

    /**
     * An EMPTY spec toward a node predating {@code dataset_declared_schema} must still serialize cleanly — only a
     * NON-empty spec is rejected. Guards the {@code else if (isEmpty() == false)} branch against an always-throw regression.
     */
    public void testEmptyDeclaredReadSpecSerializesToOlderTransportVersion() throws IOException {
        TransportVersion before = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("dataset_declared_schema"));
        List<Attribute> output = randomFieldAttributes(1, 3, false, before);
        SimpleSourceMetadata metadata = new SimpleSourceMetadata(output, "csv", "s3://bucket/x.csv", null, null, Map.of(), Map.of());
        // Six-arg constructor => default (NONE) spec.
        ExternalRelation original = new ExternalRelation(
            randomSource(),
            metadata.location(),
            metadata,
            output,
            FileList.UNRESOLVED,
            Map.of()
        );
        ExternalRelation roundTripped = copyInstance(original, before);
        assertThat(roundTripped.declaredReadSpec(), equalTo(DeclaredReadSpec.NONE));
    }
}
