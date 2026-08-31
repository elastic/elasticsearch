/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class RemoteFetchOperatorStatusTests extends AbstractWireSerializingTestCase<RemoteFetchOperator.Status> {
    @Override
    protected Writeable.Reader<RemoteFetchOperator.Status> instanceReader() {
        return RemoteFetchOperator.Status::new;
    }

    @Override
    protected RemoteFetchOperator.Status createTestInstance() {
        return new RemoteFetchOperator.Status(
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeInt()
        );
    }

    @Override
    protected RemoteFetchOperator.Status mutateInstance(RemoteFetchOperator.Status in) throws IOException {
        int pagesReceived = in.pagesReceived();
        int pagesEmitted = in.pagesEmitted();
        long rowsReceived = in.rowsReceived();
        long rowsEmitted = in.rowsEmitted();
        long batchesSent = in.batchesSent();
        int exchangesOpened = in.exchangesOpened();
        switch (randomIntBetween(0, 5)) {
            case 0 -> pagesReceived = randomValueOtherThan(pagesReceived, ESTestCase::randomNonNegativeInt);
            case 1 -> pagesEmitted = randomValueOtherThan(pagesEmitted, ESTestCase::randomNonNegativeInt);
            case 2 -> rowsReceived = randomValueOtherThan(rowsReceived, ESTestCase::randomNonNegativeLong);
            case 3 -> rowsEmitted = randomValueOtherThan(rowsEmitted, ESTestCase::randomNonNegativeLong);
            case 4 -> batchesSent = randomValueOtherThan(batchesSent, ESTestCase::randomNonNegativeLong);
            case 5 -> exchangesOpened = randomValueOtherThan(exchangesOpened, ESTestCase::randomNonNegativeInt);
            default -> throw new UnsupportedOperationException();
        }
        return new RemoteFetchOperator.Status(pagesReceived, pagesEmitted, rowsReceived, rowsEmitted, batchesSent, exchangesOpened);
    }

    public void testToXContent() {
        RemoteFetchOperator.Status status = new RemoteFetchOperator.Status(1, 2, 30, 20, 4, 3);
        assertThat(Strings.toString(status), equalTo("""
            {"pages_received":1,"pages_emitted":2,"rows_received":30,"rows_emitted":20,"batches_sent":4,"exchanges_opened":3}"""));
    }
}
