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

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class LimitStatusTests extends AbstractWireSerializingTestCase<LimitOperator.Status> {
    public void testToXContent() {
        assertThat(Strings.toString(new LimitOperator.Status(10, 1, 1)), equalTo("""
            {"limit":10,"limit_remaining":1,"pages_processed":1}"""));
    }

    @Override
    protected Writeable.Reader<LimitOperator.Status> instanceReader() {
        return LimitOperator.Status::new;
    }

    @Override
    protected LimitOperator.Status createTestInstance() {
        return new LimitOperator.Status(between(0, Integer.MAX_VALUE), between(0, Integer.MAX_VALUE), between(0, Integer.MAX_VALUE));
    }

    @Override
    protected LimitOperator.Status mutateInstance(LimitOperator.Status instance) throws IOException {
        int limit = instance.limit();
        int limitRemaining = instance.limitRemaining();
        int pagesProcessed = instance.pagesProcessed();
        switch (between(0, 2)) {
            case 0:
                limit = randomValueOtherThan(limit, () -> between(0, Integer.MAX_VALUE));
                break;
            case 1:
                limitRemaining = randomValueOtherThan(limitRemaining, () -> between(0, Integer.MAX_VALUE));
                break;
            case 2:
                pagesProcessed = randomValueOtherThan(pagesProcessed, () -> between(0, Integer.MAX_VALUE));
                break;
            default:
                throw new IllegalArgumentException();
        }
        return new LimitOperator.Status(limit, limitRemaining, pagesProcessed);
    }
}
