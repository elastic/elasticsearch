/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.spill.SpillStatus;
import org.elasticsearch.compute.spill.SpillStatusTests;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class GroupedLimitOperatorStatusTests extends AbstractWireSerializingTestCase<GroupedLimitOperator.Status> {
    public void testToXContent() {
        assertThat(Strings.toString(new GroupedLimitOperator.Status(10, 5, 3, 111, 222, 4096)), equalTo("""
            {"limit_per_group":10,"group_count":5,"pages_processed":3,"rows_received":111,"rows_emitted":222,\
            "ram_bytes_used":4096,"ram_used":"4kb"}"""));
    }

    @Override
    protected Writeable.Reader<GroupedLimitOperator.Status> instanceReader() {
        return GroupedLimitOperator.Status::new;
    }

    public void testToXContentAfterASpill() {
        assertThat(
            Strings.toString(new GroupedLimitOperator.Status(10, 5, 3, 111, 222, 4096, 50, 2, new SpillStatus(1024, 512, 1, 7))),
            equalTo("""
                {"limit_per_group":10,"group_count":5,"pages_processed":3,"rows_received":111,"rows_emitted":222,\
                "ram_bytes_used":4096,"ram_used":"4kb","rows_spilled":50,"partitions_replayed":2,"spilled_bytes":1024,\
                "spilled":"1kb","read_back_bytes":512,"read_back":"512b","spill_runs":1,"spill_nanos":7}""")
        );
    }

    @Override
    protected GroupedLimitOperator.Status createTestInstance() {
        return new GroupedLimitOperator.Status(
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeInt(),
            randomBoolean() ? SpillStatus.NONE : SpillStatusTests.randomSpillStatus()
        );
    }

    @Override
    protected GroupedLimitOperator.Status mutateInstance(GroupedLimitOperator.Status instance) throws IOException {
        int limitPerGroup = instance.limitPerGroup();
        int groupCount = instance.groupCount();
        int pagesProcessed = instance.pagesProcessed();
        long rowsReceived = instance.rowsReceived();
        long rowsEmitted = instance.rowsEmitted();
        long ramBytesUsed = instance.ramBytesUsed();
        long rowsSpilled = instance.rowsSpilled();
        int partitionsReplayed = instance.partitionsReplayed();
        SpillStatus spill = instance.spill();
        switch (between(0, 8)) {
            case 0 -> limitPerGroup = randomValueOtherThan(limitPerGroup, ESTestCase::randomNonNegativeInt);
            case 1 -> groupCount = randomValueOtherThan(groupCount, ESTestCase::randomNonNegativeInt);
            case 2 -> pagesProcessed = randomValueOtherThan(pagesProcessed, ESTestCase::randomNonNegativeInt);
            case 3 -> rowsReceived = randomValueOtherThan(rowsReceived, ESTestCase::randomNonNegativeLong);
            case 4 -> rowsEmitted = randomValueOtherThan(rowsEmitted, ESTestCase::randomNonNegativeLong);
            case 5 -> ramBytesUsed = randomValueOtherThan(ramBytesUsed, ESTestCase::randomNonNegativeLong);
            case 6 -> rowsSpilled = randomValueOtherThan(rowsSpilled, ESTestCase::randomNonNegativeLong);
            case 7 -> partitionsReplayed = randomValueOtherThan(partitionsReplayed, ESTestCase::randomNonNegativeInt);
            case 8 -> spill = randomValueOtherThan(spill, SpillStatusTests::randomSpillStatus);
            default -> throw new IllegalArgumentException();
        }
        return new GroupedLimitOperator.Status(
            limitPerGroup,
            groupCount,
            pagesProcessed,
            rowsReceived,
            rowsEmitted,
            ramBytesUsed,
            rowsSpilled,
            partitionsReplayed,
            spill
        );
    }
}
