/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.correlation;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.stream.Stream;

public class CountCorrelationIndicatorTests extends AbstractXContentSerializingTestCase<CountCorrelationIndicator> {

    public static CountCorrelationIndicator randomInstance() {
        double[] expectations = Stream.generate(ESTestCase::randomDouble)
            .limit(randomIntBetween(5, 100))
            .mapToDouble(Double::doubleValue)
            .toArray();
        double[] fractions = Stream.generate(ESTestCase::randomDouble)
            .limit(expectations.length)
            .mapToDouble(Double::doubleValue)
            .toArray();
        return new CountCorrelationIndicator(expectations, randomBoolean() ? null : fractions, randomLongBetween(1, Long.MAX_VALUE - 1));
    }

    @Override
    protected CountCorrelationIndicator doParseInstance(XContentParser parser) throws IOException {
        return CountCorrelationIndicator.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<CountCorrelationIndicator> instanceReader() {
        return CountCorrelationIndicator::new;
    }

    @Override
    protected CountCorrelationIndicator createTestInstance() {
        return randomInstance();
    }

    @Override
    protected CountCorrelationIndicator mutateInstance(CountCorrelationIndicator instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
