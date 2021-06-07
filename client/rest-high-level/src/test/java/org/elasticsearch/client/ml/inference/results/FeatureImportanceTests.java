/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml.inference.results;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FeatureImportanceTests extends AbstractXContentTestCase<FeatureImportance> {

    @Override
    @SuppressWarnings("unchecked")
    protected FeatureImportance createTestInstance() {
        Supplier<Object> classNameGenerator = randomFrom(
            () -> randomAlphaOfLength(10),
            ESTestCase::randomBoolean,
            () -> randomIntBetween(0, 10)
        );
        return new FeatureImportance(
            randomAlphaOfLength(10),
            randomBoolean() ? null : randomDoubleBetween(-10.0, 10.0, false),
            randomBoolean() ? null :
                Stream.generate(classNameGenerator)
                    .limit(randomLongBetween(2, 10))
                    .map(name -> new FeatureImportance.ClassImportance(name, randomDoubleBetween(-10, 10, false)))
                    .collect(Collectors.toList()));

    }

    @Override
    protected FeatureImportance doParseInstance(XContentParser parser) throws IOException {
        return FeatureImportance.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

}
