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

import static org.hamcrest.Matchers.equalTo;

public class TopNOperatorStatusTests extends AbstractWireSerializingTestCase<TopNOperatorStatus> {
    public void testToXContent() {
        assertThat(Strings.toString(new TopNOperatorStatus(10, 2000)), equalTo("""
            {"occupied_rows":10,"ram_bytes_used":2000,"ram_used":"1.9kb"}"""));
    }

    @Override
    protected Writeable.Reader<TopNOperatorStatus> instanceReader() {
        return TopNOperatorStatus::new;
    }

    @Override
    protected TopNOperatorStatus createTestInstance() {
        return new TopNOperatorStatus(randomNonNegativeInt(), randomNonNegativeLong());
    }

    @Override
    protected TopNOperatorStatus mutateInstance(TopNOperatorStatus instance) {
        int occupiedRows = instance.occupiedRows();
        long ramBytesUsed = instance.ramBytesUsed();
        switch (between(0, 1)) {
            case 0:
                occupiedRows = randomValueOtherThan(occupiedRows, () -> randomNonNegativeInt());
                break;
            case 1:
                ramBytesUsed = randomValueOtherThan(ramBytesUsed, () -> randomNonNegativeLong());
                break;
            default:
                throw new IllegalArgumentException();
        }
        return new TopNOperatorStatus(occupiedRows, ramBytesUsed);
    }
}
