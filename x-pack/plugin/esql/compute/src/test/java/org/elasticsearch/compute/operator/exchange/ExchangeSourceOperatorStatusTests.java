/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class ExchangeSourceOperatorStatusTests extends AbstractWireSerializingTestCase<ExchangeSourceOperator.Status> {
    public void testToXContent() {
        assertThat(Strings.toString(new ExchangeSourceOperator.Status(0, 10)), equalTo("""
            {"pages_waiting":0,"pages_emitted":10}"""));
    }

    @Override
    protected Writeable.Reader<ExchangeSourceOperator.Status> instanceReader() {
        return ExchangeSourceOperator.Status::new;
    }

    @Override
    protected ExchangeSourceOperator.Status createTestInstance() {
        return new ExchangeSourceOperator.Status(between(0, Integer.MAX_VALUE), between(0, Integer.MAX_VALUE));
    }

    @Override
    protected ExchangeSourceOperator.Status mutateInstance(ExchangeSourceOperator.Status instance) throws IOException {
        switch (between(0, 1)) {
            case 0:
                return new ExchangeSourceOperator.Status(
                    randomValueOtherThan(instance.pagesWaiting(), () -> between(0, Integer.MAX_VALUE)),
                    instance.pagesEmitted()
                );
            case 1:
                return new ExchangeSourceOperator.Status(
                    instance.pagesWaiting(),
                    randomValueOtherThan(instance.pagesEmitted(), () -> between(0, Integer.MAX_VALUE))
                );
            default:
                throw new UnsupportedOperationException();
        }
    }
}
