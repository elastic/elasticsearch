/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.slm;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class SnapshotInvocationRecordTests extends AbstractXContentSerializingTestCase<SnapshotInvocationRecord> {

    @Override
    protected SnapshotInvocationRecord doParseInstance(XContentParser parser) throws IOException {
        return SnapshotInvocationRecord.parse(parser, null);
    }

    @Override
    protected SnapshotInvocationRecord createTestInstance() {
        return randomSnapshotInvocationRecord();
    }

    @Override
    protected Writeable.Reader<SnapshotInvocationRecord> instanceReader() {
        return SnapshotInvocationRecord::new;
    }

    @Override
    protected SnapshotInvocationRecord mutateInstance(SnapshotInvocationRecord instance) {
        switch (between(0, 2)) {
            case 0:
                return new SnapshotInvocationRecord(
                    randomValueOtherThan(instance.getSnapshotName(), () -> randomAlphaOfLengthBetween(2, 10)),
                    instance.getSnapshotFinishTimestamp() - 100,
                    instance.getSnapshotFinishTimestamp(),
                    instance.getDetails()
                );
            case 1:
                long timestamp = randomValueOtherThan(instance.getSnapshotFinishTimestamp(), ESTestCase::randomNonNegativeLong);
                return new SnapshotInvocationRecord(instance.getSnapshotName(), timestamp - 100, timestamp, instance.getDetails());
            case 2:
                return new SnapshotInvocationRecord(
                    instance.getSnapshotName(),
                    instance.getSnapshotFinishTimestamp() - 100,
                    instance.getSnapshotFinishTimestamp(),
                    randomValueOtherThan(instance.getDetails(), () -> randomAlphaOfLengthBetween(2, 10))
                );
            default:
                throw new AssertionError("failure, got illegal switch case");
        }
    }

    public void testDetailsFieldIsTruncated() {
        {
            // value larger than the max allowed
            SnapshotInvocationRecord snapshotInvocationRecord = new SnapshotInvocationRecord(
                randomAlphaOfLengthBetween(5, 10),
                randomNonNegativeNullableLong(),
                randomNonNegativeLong(),
                randomAlphaOfLengthBetween(1300, 1500)
            );
            assertThat(snapshotInvocationRecord.getDetails().length(), is(SnapshotInvocationRecord.MAX_DETAILS_LENGTH));
        }

        {
            // value lower than the max allowed
            String details = randomAlphaOfLengthBetween(5, 500);
            SnapshotInvocationRecord snapshotInvocationRecord = new SnapshotInvocationRecord(
                randomAlphaOfLengthBetween(5, 10),
                randomNonNegativeNullableLong(),
                randomNonNegativeLong(),
                details
            );
            assertThat(snapshotInvocationRecord.getDetails().length(), is(details.length()));
        }

        {
            // null value, remains null
            SnapshotInvocationRecord snapshotInvocationRecord = new SnapshotInvocationRecord(
                randomAlphaOfLengthBetween(5, 10),
                randomNonNegativeNullableLong(),
                randomNonNegativeLong(),
                null
            );
            assertThat(snapshotInvocationRecord.getDetails(), is(nullValue()));
        }
    }

    public static SnapshotInvocationRecord randomSnapshotInvocationRecord() {
        return new SnapshotInvocationRecord(
            randomAlphaOfLengthBetween(5, 10),
            randomNonNegativeNullableLong(),
            randomNonNegativeLong(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10)
        );
    }

    private static Long randomNonNegativeNullableLong() {
        long value = randomLong();
        if (value < 0) {
            return null;
        } else {
            return value;
        }
    }

}
