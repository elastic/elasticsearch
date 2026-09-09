/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.cache.ReadConfigFingerprint;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The resolved read configuration is DERIVED on both sides rather than shipped: the coordinator computes it while resolving, and the
 * data node recomputes it from the split it was handed. That only works if the two derivations agree — and an identity
 * the two sides compute differently is worse than none, because it matches nothing and disables the warm path in
 * silence rather than failing.
 * <p>
 * The wire is where they can drift: a split's schema is written as name, type name and a nullability flag, with the
 * planner-internal UNKNOWN normalized to nullable, so an attribute does not survive the round trip byte-identical.
 * These tests pin that the FINGERPRINT survives it anyway.
 */
public class ReadConfigSymmetryTests extends ESTestCase {

    private final NamedWriteableRegistry registry = new NamedWriteableRegistry(List.of(FileSplit.ENTRY));

    public void testFingerprintSurvivesTheSplitRoundTrip() throws IOException {
        List<Attribute> coordinatorSchema = List.of(attr("user", DataType.KEYWORD), attr("count", DataType.LONG));
        DeclaredReadSpec spec = new DeclaredReadSpec(Map.of(), null, Map.of(), Set.of(), SchemaProvenance.INFERRED);

        String coordinatorSide = ReadConfigFingerprint.of(coordinatorSchema, spec);
        String dataNodeSide = ReadConfigFingerprint.of(roundTrip(coordinatorSchema).readSchema(), spec);

        assertEquals("the coordinator and the data node must derive the same resolved read configuration", coordinatorSide, dataNodeSide);
        assertNotEquals(ReadConfigFingerprint.UNKNOWN, coordinatorSide);
    }

    public void testFingerprintSurvivesTheRoundTripUnderADeclaration() throws IOException {
        // A declaration is where the two sides have the most room to disagree: renames, per-column patterns and the
        // binding mode all feed the read configuration, and only the schema half crosses the wire in the split.
        List<Attribute> coordinatorSchema = List.of(attr("user", DataType.KEYWORD), attr("ts", DataType.DATETIME));
        DeclaredReadSpec spec = new DeclaredReadSpec(
            Map.of("user", "user_name"),
            null,
            Map.of("ts", "yyyyMMdd"),
            Set.of("user", "ts"),
            SchemaProvenance.DECLARED
        );

        assertEquals(
            ReadConfigFingerprint.of(coordinatorSchema, spec),
            ReadConfigFingerprint.of(roundTrip(coordinatorSchema).readSchema(), spec)
        );
    }

    public void testNullabilityDifferencesDoNotMoveTheFingerprint() throws IOException {
        // The wire collapses the planner's UNKNOWN nullability to nullable, so if nullability ever entered the hash the
        // two sides would silently derive different values. Pin the exclusion rather than trusting it stays excluded.
        List<Attribute> unknownNullability = List.of(
            new ReferenceAttribute(Source.EMPTY, null, "a", DataType.KEYWORD, Nullability.UNKNOWN, null, false)
        );
        List<Attribute> explicitlyNullable = List.of(
            new ReferenceAttribute(Source.EMPTY, null, "a", DataType.KEYWORD, Nullability.TRUE, null, false)
        );

        assertEquals(
            ReadConfigFingerprint.of(unknownNullability, DeclaredReadSpec.NONE),
            ReadConfigFingerprint.of(explicitlyNullable, DeclaredReadSpec.NONE)
        );
        assertEquals(
            ReadConfigFingerprint.of(unknownNullability, DeclaredReadSpec.NONE),
            ReadConfigFingerprint.of(roundTrip(unknownNullability).readSchema(), DeclaredReadSpec.NONE)
        );
    }

    public void testAbsentReadSchemaDerivesUnknownOnBothSides() throws IOException {
        // Legitimate on the rails that carry no pin. Both sides must agree that the read configuration is unknown, so neither
        // stamps nor matches on it.
        assertEquals(ReadConfigFingerprint.UNKNOWN, ReadConfigFingerprint.of(null, DeclaredReadSpec.NONE));
        assertEquals(ReadConfigFingerprint.UNKNOWN, ReadConfigFingerprint.of(roundTrip(null).readSchema(), DeclaredReadSpec.NONE));
    }

    /** Ships a split carrying {@code readSchema} through the wire and returns what the data node would see. */
    private FileSplit roundTrip(List<Attribute> readSchema) throws IOException {
        FileSplit original = FileSplit.withReadSchema(
            "csv",
            StoragePath.of("s3://bucket/data/file.csv"),
            0,
            2048,
            ".csv",
            Map.of(),
            Map.of(),
            null,
            readSchema
        );
        BytesStreamOutput out = new BytesStreamOutput();
        out.writeNamedWriteable(original);
        try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
            return (FileSplit) in.readNamedWriteable(ExternalSplit.class);
        }
    }

    private static Attribute attr(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, name, type);
    }
}
