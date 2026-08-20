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

public class StreamingPageOperatorStatusTests extends AbstractWireSerializingTestCase<StreamingPageOperator.Status> {

    public void testToXContent() {
        assertThat(Strings.toString(new StreamingPageOperator.Status(3, 42L)), equalTo("""
            {"pages_emitted":3,"rows_emitted":42}"""));
    }

    @Override
    protected Writeable.Reader<StreamingPageOperator.Status> instanceReader() {
        return StreamingPageOperator.Status::new;
    }

    @Override
    protected StreamingPageOperator.Status createTestInstance() {
        return new StreamingPageOperator.Status(randomNonNegativeInt(), randomNonNegativeLong());
    }

    @Override
    protected StreamingPageOperator.Status mutateInstance(StreamingPageOperator.Status instance) throws IOException {
        int pagesEmitted = instance.pagesEmitted();
        long rowsEmitted = instance.rowsEmitted();
        switch (between(0, 1)) {
            case 0:
                pagesEmitted = randomValueOtherThan(pagesEmitted, ESTestCase::randomNonNegativeInt);
                break;
            case 1:
                rowsEmitted = randomValueOtherThan(rowsEmitted, ESTestCase::randomNonNegativeLong);
                break;
            default:
                throw new IllegalArgumentException();
        }
        return new StreamingPageOperator.Status(pagesEmitted, rowsEmitted);
    }
}
