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

public class OutputOperatorStatusTests extends AbstractWireSerializingTestCase<OutputOperator.Status> {
    public void testToXContent() {
        assertThat(Strings.toString(new OutputOperator.Status(3, 111, 0, SpillStatus.NONE)), equalTo("""
            {"pages_received":3,"rows_received":111}"""));
    }

    public void testToXContentAfterASpill() {
        assertThat(Strings.toString(new OutputOperator.Status(3, 111, 100, new SpillStatus(1024, 0, 1, 7))), equalTo("""
            {"pages_received":3,"rows_received":111,"rows_spilled":100,"spilled_bytes":1024,"spilled":"1kb",\
            "read_back_bytes":0,"read_back":"0b","spill_runs":1,"spill_nanos":7}"""));
    }

    @Override
    protected Writeable.Reader<OutputOperator.Status> instanceReader() {
        return OutputOperator.Status::new;
    }

    @Override
    protected OutputOperator.Status createTestInstance() {
        return new OutputOperator.Status(
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomBoolean() ? SpillStatus.NONE : SpillStatusTests.randomSpillStatus()
        );
    }

    @Override
    protected OutputOperator.Status mutateInstance(OutputOperator.Status instance) throws IOException {
        int pagesReceived = instance.pagesReceived();
        long rowsReceived = instance.rowsReceived();
        long rowsSpilled = instance.rowsSpilled();
        SpillStatus spill = instance.spill();
        switch (between(0, 3)) {
            case 0 -> pagesReceived = randomValueOtherThan(pagesReceived, ESTestCase::randomNonNegativeInt);
            case 1 -> rowsReceived = randomValueOtherThan(rowsReceived, ESTestCase::randomNonNegativeLong);
            case 2 -> rowsSpilled = randomValueOtherThan(rowsSpilled, ESTestCase::randomNonNegativeLong);
            case 3 -> spill = randomValueOtherThan(spill, SpillStatusTests::randomSpillStatus);
            default -> throw new UnsupportedOperationException();
        }
        return new OutputOperator.Status(pagesReceived, rowsReceived, rowsSpilled, spill);
    }
}
