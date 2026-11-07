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
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
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

    /** What each arm is called in a diagnostic: the dataset's own name, or the source an inline definition reads. */
    public void testEachArmNamesItself() {
        assertThat(NAMED.describe(), equalTo("logs"));
        assertThat(INLINE.describe(), equalTo("s3_source"));
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
