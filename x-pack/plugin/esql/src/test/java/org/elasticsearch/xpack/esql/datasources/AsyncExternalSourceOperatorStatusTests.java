/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class AsyncExternalSourceOperatorStatusTests extends AbstractWireSerializingTestCase<AsyncExternalSourceOperator.Status> {

    @Override
    protected Writeable.Reader<AsyncExternalSourceOperator.Status> instanceReader() {
        return AsyncExternalSourceOperator.Status::new;
    }

    @Override
    protected AsyncExternalSourceOperator.Status createTestInstance() {
        return new AsyncExternalSourceOperator.Status(
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            null
        );
    }

    @Override
    protected AsyncExternalSourceOperator.Status mutateInstance(AsyncExternalSourceOperator.Status instance) throws IOException {
        int pagesWaiting = instance.pagesWaiting();
        int pagesEmitted = instance.pagesEmitted();
        long rowsEmitted = instance.rowsEmitted();
        long bytesBuffered = instance.bytesBuffered();
        switch (between(0, 3)) {
            case 0 -> pagesWaiting = randomValueOtherThan(pagesWaiting, ESTestCase::randomNonNegativeInt);
            case 1 -> pagesEmitted = randomValueOtherThan(pagesEmitted, ESTestCase::randomNonNegativeInt);
            case 2 -> rowsEmitted = randomValueOtherThan(rowsEmitted, ESTestCase::randomNonNegativeLong);
            case 3 -> bytesBuffered = randomValueOtherThan(bytesBuffered, ESTestCase::randomNonNegativeLong);
            default -> throw new UnsupportedOperationException();
        }
        return new AsyncExternalSourceOperator.Status(pagesWaiting, pagesEmitted, rowsEmitted, bytesBuffered, null);
    }

    public void testToXContent() {
        assertThat(
            Strings.toString(new AsyncExternalSourceOperator.Status(5, 10, 111, 2048, null)),
            equalTo("{\"pages_waiting\":5,\"pages_emitted\":10,\"rows_emitted\":111,\"bytes_buffered\":2048}")
        );
    }

    public void testToXContentWithFailure() {
        assertThat(
            Strings.toString(new AsyncExternalSourceOperator.Status(5, 10, 111, 2048, new RuntimeException("boom"))),
            equalTo("{\"pages_waiting\":5,\"pages_emitted\":10,\"rows_emitted\":111,\"bytes_buffered\":2048,\"failure\":\"boom\"}")
        );
    }
}
