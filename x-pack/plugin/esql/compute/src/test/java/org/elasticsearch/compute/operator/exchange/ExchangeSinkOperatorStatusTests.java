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

public class ExchangeSinkOperatorStatusTests extends AbstractWireSerializingTestCase<ExchangeSinkOperator.Status> {
    public void testToXContent() {
        assertThat(Strings.toString(new ExchangeSinkOperator.Status(10)), equalTo("""
            {"pages_accepted":10}"""));
    }

    @Override
    protected Writeable.Reader<ExchangeSinkOperator.Status> instanceReader() {
        return ExchangeSinkOperator.Status::new;
    }

    @Override
    protected ExchangeSinkOperator.Status createTestInstance() {
        return new ExchangeSinkOperator.Status(between(0, Integer.MAX_VALUE));
    }

    @Override
    protected ExchangeSinkOperator.Status mutateInstance(ExchangeSinkOperator.Status instance) throws IOException {
        return new ExchangeSinkOperator.Status(randomValueOtherThan(instance.pagesAccepted(), () -> between(0, Integer.MAX_VALUE)));
    }
}
