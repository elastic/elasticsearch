/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.inference;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class TrainedModelInputTests extends AbstractXContentTestCase<TrainedModelInput> {

    @Override
    protected TrainedModelInput doParseInstance(XContentParser parser) throws IOException {
        return TrainedModelInput.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.isEmpty() == false;
    }

    public static TrainedModelInput createRandomInput() {
        return new TrainedModelInput(Stream.generate(() -> randomAlphaOfLength(10))
            .limit(randomLongBetween(1, 10))
            .collect(Collectors.toList()));
    }

    @Override
    protected TrainedModelInput createTestInstance() {
        return createRandomInput();
    }
}
