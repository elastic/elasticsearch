/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class TrainedModelInputTests extends AbstractBWCSerializationTestCase<TrainedModelInput> {

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
        return field -> !field.isEmpty();
    }

    public static TrainedModelInput createRandomInput() {
        return new TrainedModelInput(Stream.generate(
            () -> new TrainedModelInput.InputObject(randomAlphaOfLength(10), randomFrom(ModelFieldType.values()).toString()))
            .limit(randomInt(10))
            .collect(Collectors.toList()));
    }

    @Override
    protected TrainedModelInput createTestInstance() {
        return createRandomInput();
    }

    @Override
    protected Writeable.Reader<TrainedModelInput> instanceReader() {
        return TrainedModelInput::new;
    }

    @Override
    protected TrainedModelInput mutateInstanceForVersion(TrainedModelInput instance, Version version) {
        if (version.before(Version.V_7_7_0)) {
            return new TrainedModelInput(instance.getFieldNames()
                .stream()
                .map(inputObject -> new TrainedModelInput.InputObject(inputObject.getFieldName(), null))
                .collect(Collectors.toList()));
        }
        return instance;
    }
}
