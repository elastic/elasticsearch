/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class TopNOperatorStatusTests extends AbstractWireSerializingTestCase<TopNOperatorStatus> {
    public static TopNOperatorStatus simple() {
        return new TopNOperatorStatus(10, 2000, 123, 123, 111, 222);
    }

    public static String simpleToJson() {
        return """
            {
              "occupied_rows" : 10,
              "ram_bytes_used" : 2000,
              "ram_used" : "1.9kb",
              "pages_received" : 123,
              "pages_emitted" : 123,
              "rows_received" : 111,
              "rows_emitted" : 222
            }""";
    }

    public void testToXContent() {
        assertThat(Strings.toString(simple(), true, true), equalTo(simpleToJson()));
    }

    @Override
    protected Writeable.Reader<TopNOperatorStatus> instanceReader() {
        return TopNOperatorStatus::new;
    }

    @Override
    protected TopNOperatorStatus createTestInstance() {
        return new TopNOperatorStatus(
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    @Override
    protected TopNOperatorStatus mutateInstance(TopNOperatorStatus instance) {
        int occupiedRows = instance.occupiedRows();
        long ramBytesUsed = instance.ramBytesUsed();
        int pagesReceived = instance.pagesReceived();
        int pagesEmitted = instance.pagesEmitted();
        long rowsReceived = instance.rowsReceived();
        long rowsEmitted = instance.rowsEmitted();
        switch (between(0, 5)) {
            case 0:
                occupiedRows = randomValueOtherThan(occupiedRows, ESTestCase::randomNonNegativeInt);
                break;
            case 1:
                ramBytesUsed = randomValueOtherThan(ramBytesUsed, ESTestCase::randomNonNegativeLong);
                break;
            case 2:
                pagesReceived = randomValueOtherThan(pagesReceived, ESTestCase::randomNonNegativeInt);
                break;
            case 3:
                pagesEmitted = randomValueOtherThan(pagesEmitted, ESTestCase::randomNonNegativeInt);
                break;
            case 4:
                rowsReceived = randomValueOtherThan(rowsReceived, ESTestCase::randomNonNegativeLong);
                break;
            case 5:
                rowsEmitted = randomValueOtherThan(rowsEmitted, ESTestCase::randomNonNegativeLong);
                break;
            default:
                throw new IllegalArgumentException();
        }
        return new TopNOperatorStatus(occupiedRows, ramBytesUsed, pagesReceived, pagesEmitted, rowsReceived, rowsEmitted);
    }
}
