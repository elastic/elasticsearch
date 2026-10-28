/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class MMROperatorStatusTests extends AbstractWireSerializingTestCase<MMROperator.Status> {
    public static MMROperator.Status simple() {
        return new MMROperator.Status(10, 15, 9, 111, 222);
    }

    public static String simpleToJson() {
        return """
            {"emit_nanos":10,"pages_received":15,"pages_processed":9,"rows_received":111,"rows_emitted":222}""";
    }

    @Override
    protected Writeable.Reader<MMROperator.Status> instanceReader() {
        return MMROperator.Status::new;
    }

    @Override
    protected MMROperator.Status createTestInstance() {
        return new MMROperator.Status(
            randomNonNegativeLong(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    @Override
    protected MMROperator.Status mutateInstance(MMROperator.Status instance) throws IOException {
        long emitNanos = instance.emitNanos();
        int pagesReceived = instance.pagesReceived();
        int pagesProcessed = instance.pagesProcessed();
        long rowsReceived = instance.rowsReceived();
        long rowsEmitted = instance.rowsEmitted();
        switch (between(0, 4)) {
            case 0 -> emitNanos = randomValueOtherThan(instance.emitNanos(), ESTestCase::randomNonNegativeLong);
            case 1 -> pagesReceived = randomValueOtherThan(instance.pagesReceived(), ESTestCase::randomNonNegativeInt);
            case 2 -> pagesProcessed = randomValueOtherThan(instance.pagesProcessed(), ESTestCase::randomNonNegativeInt);
            case 3 -> rowsReceived = randomValueOtherThan(instance.rowsReceived(), ESTestCase::randomNonNegativeLong);
            case 4 -> rowsEmitted = randomValueOtherThan(instance.rowsEmitted(), ESTestCase::randomNonNegativeLong);
            default -> throw new UnsupportedOperationException();
        }
        return new MMROperator.Status(emitNanos, pagesReceived, pagesProcessed, rowsReceived, rowsEmitted);
    }
}
