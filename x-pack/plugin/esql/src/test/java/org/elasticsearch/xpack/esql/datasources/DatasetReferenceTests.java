/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * The slot itself: that the name arm is written as the bare name an older node has always read, that the inline arm
 * is refused cluster-wide while the query is analyzed rather than at the serializer, and that a round trip through
 * the current version returns what went in.
 */
public class DatasetReferenceTests extends ESTestCase {

    private static final DatasetReference.Named NAMED = new DatasetReference.Named("logs");
    private static final DatasetReference.Inline INLINE = new DatasetReference.Inline(
        "s3_source",
        Map.of("resource", "s3://bucket/*.csv", "settings", Map.of("format", "csv"))
    );

    public void testBothArmsSurviveACurrentVersionRoundTrip() throws IOException {
        assertThat(roundTrip(NAMED, TransportVersion.current()), equalTo(NAMED));
        assertThat(roundTrip(INLINE, TransportVersion.current()), equalTo(INLINE));
        assertThat(roundTrip(null, TransportVersion.current()), nullValue());
    }

    /**
     * The name arm's bytes toward a node predating the reference are the bytes that node has always read, which is
     * what makes the union backward compatible rather than merely version-gated: every query that works today keeps
     * working, and such a node yields the name arm back.
     */
    public void testTheNameArmIsStillABareNameForAnOlderNode() throws IOException {
        TransportVersion before = TransportVersionUtils.getPreviousVersion(DatasetReference.DATASET_REFERENCE);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(before);
            DatasetReference.writeOptional(out, NAMED);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(before);
                assertThat(in.readOptionalString(), equalTo("logs"));
            }
        }
        assertThat(roundTrip(NAMED, before), equalTo(NAMED));
        assertThat(roundTrip(null, before), nullValue());
    }

    /** Reaching the serializer with an inline arm means a target older than the coordinator's minimum. */
    public void testTheInlineArmCannotBeWrittenForAnOlderNode() throws IOException {
        TransportVersion before = TransportVersionUtils.getPreviousVersion(DatasetReference.DATASET_REFERENCE);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(before);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DatasetReference.writeOptional(out, INLINE));
            assertThat(e.getMessage(), containsString("not supported on all nodes in the cluster"));
        }
    }

    /**
     * And toward a node so old it has no dataset slot at all. That arm is as unreadable there as one version up, so
     * the refusal comes before the slot's own version check rather than after it — where it would have returned
     * having written nothing.
     */
    public void testTheInlineArmCannotBeWrittenForANodeWithoutTheSlot() throws IOException {
        TransportVersion beforeTheSlot = TransportVersionUtils.getPreviousVersion(DatasetReference.DATASET_NAME);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(beforeTheSlot);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DatasetReference.writeOptional(out, INLINE));
            assertThat(e.getMessage(), containsString("not supported on all nodes in the cluster"));
        }
    }

    public void testAPlanCarryingAnInlineArmIsRefusedOnAnOlderCluster() {
        TransportVersion before = TransportVersionUtils.getPreviousVersion(DatasetReference.DATASET_REFERENCE);

        VerificationException e = expectThrows(
            VerificationException.class,
            () -> DatasetReference.ensureEveryReferenceCanTravel(relation(INLINE), before)
        );
        assertThat(e.getMessage(), containsString("not supported on all nodes in the cluster"));
    }

    public void testAPlanCarryingAnInlineArmPassesOnceEveryNodeCanReadOne() {
        DatasetReference.ensureEveryReferenceCanTravel(relation(INLINE), DatasetReference.DATASET_REFERENCE);
        DatasetReference.ensureEveryReferenceCanTravel(relation(INLINE), TransportVersion.current());
    }

    /** The refusal is about the arm, not about external reads: a name arm and an index pattern are unaffected. */
    public void testNothingElseIsRefused() {
        TransportVersion before = TransportVersionUtils.getPreviousVersion(DatasetReference.DATASET_REFERENCE);

        DatasetReference.ensureEveryReferenceCanTravel(relation(NAMED), before);
        DatasetReference.ensureEveryReferenceCanTravel(relation(null), before);
        DatasetReference.ensureEveryReferenceCanTravel(indexPlan(), before);
    }

    /**
     * The inline arm renders its field names and never their values. A record's generated toString would print the
     * whole definition, and this one holds whatever the user wrote inside the braces, so any log line or assertion
     * message that formats a reference would carry it.
     */
    public void testTheInlineArmDoesNotRenderItsDefinition() {
        String rendered = INLINE.toString();
        assertThat(rendered, containsString("s3_source"));
        assertThat(rendered, containsString("resource"));
        assertThat(rendered, containsString("settings"));
        assertThat(rendered, not(containsString("s3://bucket/*.csv")));
        assertThat(rendered, not(containsString("csv")));
    }

    /** A definition a stream carried that the record cannot hold is a malformed request, reported as one. */
    public void testAWireDefinitionThatIsNotAMapOfNamedValuesIsRefused() throws IOException {
        assertThat(expectThrows(IOException.class, () -> readInlineDefinition("not a map")).getMessage(), containsString("not a map"));
        Map<String, Object> withNull = new HashMap<>();
        withNull.put("description", null);
        assertThat(
            expectThrows(IOException.class, () -> readInlineDefinition(withNull)).getMessage(),
            containsString("sets [description] to null")
        );
    }

    /** A field named by something other than a string is the same malformed payload, reported the same way. */
    public void testAWireDefinitionWithAFieldNotNamedByAStringIsRefused() {
        Map<Object, Object> numberedKey = new HashMap<>();
        numberedKey.put(1, "s3://bucket/*.csv");

        assertThat(
            expectThrows(IOException.class, () -> readInlineDefinition(numberedKey)).getMessage(),
            containsString("has a field not named by a string")
        );
    }

    /** A tag this version does not know is a malformed payload too, not a silently dropped reference. */
    public void testAnUnknownWireTagIsRefused() {
        IOException e = expectThrows(IOException.class, () -> {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setTransportVersion(TransportVersion.current());
                out.writeBoolean(true);
                out.writeByte((byte) 7);
                try (StreamInput in = out.bytes().streamInput()) {
                    in.setTransportVersion(TransportVersion.current());
                    DatasetReference.readOptionalFrom(in);
                }
            }
        });
        assertThat(e.getMessage(), containsString("unknown dataset reference tag [7]"));
    }

    /**
     * Toward a node predating the slot entirely, a name arm writes nothing at all — the bytes have to be what that
     * node has always read, and it reads no dataset behind an external source. Asserted on the byte count, because a
     * reader that returns null tells you nothing about whether something was written and then skipped.
     */
    public void testThereIsNoSlotAtAllForANodeWithoutOne() throws IOException {
        TransportVersion beforeTheSlot = TransportVersionUtils.getPreviousVersion(DatasetReference.DATASET_NAME);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(beforeTheSlot);
            DatasetReference.writeOptional(out, NAMED);
            DatasetReference.writeOptional(out, null);
            assertThat(out.bytes().length(), equalTo(0));
        }
        assertThat(roundTrip(NAMED, beforeTheSlot), nullValue());
        assertThat(roundTrip(null, beforeTheSlot), nullValue());
    }

    /** The walk reads a resolved leaf as well as an unresolved one, or the refusal would depend on when it ran. */
    public void testAResolvedLeafCarryingAnInlineArmIsRefusedToo() {
        TransportVersion before = TransportVersionUtils.getPreviousVersion(DatasetReference.DATASET_REFERENCE);

        VerificationException e = expectThrows(
            VerificationException.class,
            () -> DatasetReference.ensureEveryReferenceCanTravel(externalRelation(INLINE), before)
        );
        assertThat(e.getMessage(), containsString("not supported on all nodes in the cluster"));
        DatasetReference.ensureEveryReferenceCanTravel(externalRelation(NAMED), before);
    }

    /** Neither arm holds a null, so a reference always names what it reads. */
    public void testNeitherArmAcceptsANull() {
        expectThrows(NullPointerException.class, () -> new DatasetReference.Named(null));
        expectThrows(NullPointerException.class, () -> new DatasetReference.Inline(null, Map.of()));
        expectThrows(NullPointerException.class, () -> new DatasetReference.Inline("s3_source", null));
    }

    private static LogicalPlan externalRelation(DatasetReference reference) {
        List<Attribute> output = List.of(
            new FieldAttribute(Source.EMPTY, "a", new EsField("a", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE))
        );
        String path = "s3://bucket/*.csv";
        return new ExternalRelation(
            Source.EMPTY,
            path,
            new SimpleSourceMetadata(output, "csv", path),
            output,
            FileList.UNRESOLVED,
            Map.of(),
            reference
        );
    }

    /** Writes the inline tag, a data source and {@code definition} as a generic value, then reads the slot back. */
    private static DatasetReference readInlineDefinition(Object definition) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersion.current());
            out.writeBoolean(true);
            out.writeByte((byte) 1);
            out.writeString("s3_source");
            out.writeGenericValue(definition);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(TransportVersion.current());
                return DatasetReference.readOptionalFrom(in);
            }
        }
    }

    private static DatasetReference roundTrip(DatasetReference reference, TransportVersion version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            DatasetReference.writeOptional(out, reference);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(version);
                return DatasetReference.readOptionalFrom(in);
            }
        }
    }

    private static LogicalPlan relation(DatasetReference reference) {
        return new UnresolvedExternalRelation(
            Source.EMPTY,
            Literal.keyword(Source.EMPTY, "s3://bucket/*.csv"),
            Map.of(),
            List.of(),
            reference,
            null
        );
    }

    private static LogicalPlan indexPlan() {
        return new UnresolvedRelation(Source.EMPTY, new IndexPattern(Source.EMPTY, "logs"), false, List.of(), IndexMode.STANDARD, null);
    }
}
