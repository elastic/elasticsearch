/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.Before;

import java.io.IOException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TrainedModelInputTests extends AbstractSerializingTestCase<TrainedModelInput> {

    private boolean lenient;

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected TrainedModelInput doParseInstance(XContentParser parser) throws IOException {
        return TrainedModelInput.fromXContent(parser, lenient);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.isEmpty() == false;
    }

    public static TrainedModelInput createRandomInput() {
        return new TrainedModelInput(Stream.generate(() -> randomAlphaOfLength(10)).limit(randomInt(10)).collect(Collectors.toList()));
    }

    @Override
    protected TrainedModelInput createTestInstance() {
        return createRandomInput();
    }

    @Override
    protected Writeable.Reader<TrainedModelInput> instanceReader() {
        return TrainedModelInput::new;
    }

}
