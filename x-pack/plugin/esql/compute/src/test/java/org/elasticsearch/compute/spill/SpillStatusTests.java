/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class SpillStatusTests extends AbstractWireSerializingTestCase<SpillStatus> {

    public static SpillStatus randomSpillStatus() {
        return new SpillStatus(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeInt(), randomNonNegativeLong());
    }

    @Override
    protected Writeable.Reader<SpillStatus> instanceReader() {
        return SpillStatus::readFrom;
    }

    @Override
    protected SpillStatus createTestInstance() {
        return randomBoolean() ? SpillStatus.NONE : randomSpillStatus();
    }

    @Override
    protected SpillStatus mutateInstance(SpillStatus instance) throws IOException {
        long spilledBytes = instance.spilledBytes();
        long readBackBytes = instance.readBackBytes();
        int spillRuns = instance.spillRuns();
        long spillNanos = instance.spillNanos();
        switch (between(0, 3)) {
            case 0 -> spilledBytes = randomValueOtherThan(spilledBytes, ESTestCase::randomNonNegativeLong);
            case 1 -> readBackBytes = randomValueOtherThan(readBackBytes, ESTestCase::randomNonNegativeLong);
            case 2 -> spillRuns = randomValueOtherThan(spillRuns, ESTestCase::randomNonNegativeInt);
            case 3 -> spillNanos = randomValueOtherThan(spillNanos, ESTestCase::randomNonNegativeLong);
            default -> throw new IllegalArgumentException();
        }
        return new SpillStatus(spilledBytes, readBackBytes, spillRuns, spillNanos);
    }

    public void testNoneWritesNothing() throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            SpillStatus.NONE.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            assertThat(Strings.toString(builder), equalTo("{}"));
        }
    }

    public void testToXContent() throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            new SpillStatus(4096, 2048, 2, 1_500_000).toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            assertThat(Strings.toString(builder), equalTo("""
                {"spilled_bytes":4096,"spilled":"4kb","read_back_bytes":2048,"read_back":"2kb","spill_runs":2,"spill_nanos":1500000}"""));
        }
    }

    public void testPlus() {
        assertThat(new SpillStatus(1, 2, 3, 4).plus(new SpillStatus(10, 20, 30, 40)), equalTo(new SpillStatus(11, 22, 33, 44)));
        assertTrue(SpillStatus.NONE.plus(SpillStatus.NONE).isNone());
    }
}
