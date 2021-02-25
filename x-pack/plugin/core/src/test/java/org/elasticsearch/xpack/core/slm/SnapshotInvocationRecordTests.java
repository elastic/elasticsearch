/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.slm;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class SnapshotInvocationRecordTests extends AbstractSerializingTestCase<SnapshotInvocationRecord> {

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
                    randomValueOtherThan(instance.getSnapshotName(), () -> randomAlphaOfLengthBetween(2,10)),
                    instance.getTimestamp(),
                    instance.getDetails());
            case 1:
                return new SnapshotInvocationRecord(instance.getSnapshotName(),
                    randomValueOtherThan(instance.getTimestamp(), ESTestCase::randomNonNegativeLong),
                    instance.getDetails());
            case 2:
                return new SnapshotInvocationRecord(instance.getSnapshotName(),
                    instance.getTimestamp(),
                    randomValueOtherThan(instance.getDetails(), () -> randomAlphaOfLengthBetween(2,10)));
            default:
                throw new AssertionError("failure, got illegal switch case");
        }
    }

    public static SnapshotInvocationRecord randomSnapshotInvocationRecord() {
        return new SnapshotInvocationRecord(
            randomAlphaOfLengthBetween(5,10),
            randomNonNegativeLong(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10));
    }

}
