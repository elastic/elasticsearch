/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FeatureImportanceTests extends AbstractSerializingTestCase<FeatureImportance> {

    public static FeatureImportance createRandomInstance() {
        return randomBoolean() ? randomClassification() : randomRegression();
    }

    static FeatureImportance randomRegression() {
        return FeatureImportance.forRegression(randomAlphaOfLength(10), randomDoubleBetween(-10.0, 10.0, false));
    }

    static FeatureImportance randomClassification() {
        return FeatureImportance.forClassification(
            randomAlphaOfLength(10),
            Stream.generate(() -> randomAlphaOfLength(10))
                .limit(randomLongBetween(2, 10))
                .collect(Collectors.toMap(Function.identity(), (k) -> randomDoubleBetween(-10, 10, false))));
    }

    @Override
    protected FeatureImportance createTestInstance() {
        return createRandomInstance();
    }

    @Override
    protected Writeable.Reader<FeatureImportance> instanceReader() {
        return FeatureImportance::new;
    }

    @Override
    protected FeatureImportance doParseInstance(XContentParser parser) throws IOException {
        return FeatureImportance.fromXContent(parser);
    }
}
