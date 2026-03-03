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

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class GroupedLimitOperatorStatusTests extends AbstractWireSerializingTestCase<GroupedLimitOperator.Status> {
    public void testToXContent() {
        assertThat(Strings.toString(new GroupedLimitOperator.Status(10, 5, 3, 111, 222)), equalTo("""
            {"limit_per_group":10,"group_count":5,"pages_processed":3,"rows_received":111,"rows_emitted":222}"""));
    }

    @Override
    protected Writeable.Reader<GroupedLimitOperator.Status> instanceReader() {
        return GroupedLimitOperator.Status::new;
    }

    @Override
    protected GroupedLimitOperator.Status createTestInstance() {
        return new GroupedLimitOperator.Status(
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    @Override
    protected GroupedLimitOperator.Status mutateInstance(GroupedLimitOperator.Status instance) throws IOException {
        int limitPerGroup = instance.limitPerGroup();
        int groupCount = instance.groupCount();
        int pagesProcessed = instance.pagesProcessed();
        long rowsReceived = instance.rowsReceived();
        long rowsEmitted = instance.rowsEmitted();
        switch (between(0, 4)) {
            case 0 -> limitPerGroup = randomValueOtherThan(limitPerGroup, ESTestCase::randomNonNegativeInt);
            case 1 -> groupCount = randomValueOtherThan(groupCount, ESTestCase::randomNonNegativeInt);
            case 2 -> pagesProcessed = randomValueOtherThan(pagesProcessed, ESTestCase::randomNonNegativeInt);
            case 3 -> rowsReceived = randomValueOtherThan(rowsReceived, ESTestCase::randomNonNegativeLong);
            case 4 -> rowsEmitted = randomValueOtherThan(rowsEmitted, ESTestCase::randomNonNegativeLong);
            default -> throw new IllegalArgumentException();
        }
        return new GroupedLimitOperator.Status(limitPerGroup, groupCount, pagesProcessed, rowsReceived, rowsEmitted);
    }
}
