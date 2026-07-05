/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class TsInfoOperatorStatusTests extends AbstractWireSerializingTestCase<TsInfoOperator.Status> {
    public static TsInfoOperator.Status simple() {
        return new TsInfoOperator.Status(TsInfoOperator.Mode.FINAL, 3, 42, 10, 7);
    }

    public static String simpleToJson() {
        return """
            {
              "mode" : "FINAL",
              "pages_received" : 3,
              "rows_received" : 42,
              "rows_emitted" : 10,
              "entries_accumulated" : 7
            }""";
    }

    public void testToXContent() {
        assertThat(Strings.toString(simple(), true, true), equalTo(simpleToJson()));
    }

    @Override
    protected Writeable.Reader<TsInfoOperator.Status> instanceReader() {
        return TsInfoOperator.Status::new;
    }

    @Override
    public TsInfoOperator.Status createTestInstance() {
        return new TsInfoOperator.Status(
            randomFrom(TsInfoOperator.Mode.values()),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeInt()
        );
    }

    @Override
    protected TsInfoOperator.Status mutateInstance(TsInfoOperator.Status instance) {
        TsInfoOperator.Mode mode = instance.mode();
        int pagesReceived = instance.pagesReceived();
        long rowsReceived = instance.rowsReceived();
        long rowsEmitted = instance.rowsEmitted();
        int entriesAccumulated = instance.entriesAccumulated();
        switch (between(0, 4)) {
            case 0 -> mode = randomValueOtherThan(mode, () -> randomFrom(TsInfoOperator.Mode.values()));
            case 1 -> pagesReceived = randomValueOtherThan(pagesReceived, ESTestCase::randomNonNegativeInt);
            case 2 -> rowsReceived = randomValueOtherThan(rowsReceived, ESTestCase::randomNonNegativeLong);
            case 3 -> rowsEmitted = randomValueOtherThan(rowsEmitted, ESTestCase::randomNonNegativeLong);
            case 4 -> entriesAccumulated = randomValueOtherThan(entriesAccumulated, ESTestCase::randomNonNegativeInt);
            default -> throw new UnsupportedOperationException();
        }
        return new TsInfoOperator.Status(mode, pagesReceived, rowsReceived, rowsEmitted, entriesAccumulated);
    }
}
