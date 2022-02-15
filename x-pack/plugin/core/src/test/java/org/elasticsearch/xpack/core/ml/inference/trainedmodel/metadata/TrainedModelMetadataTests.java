/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TrainedModelMetadataTests extends AbstractBWCSerializationTestCase<TrainedModelMetadata> {

    private boolean lenient;

    public static TrainedModelMetadata randomInstance() {
        return new TrainedModelMetadata(
            randomAlphaOfLength(10),
            Stream.generate(TotalFeatureImportanceTests::randomInstance).limit(randomIntBetween(1, 10)).collect(Collectors.toList()),
            randomBoolean() ? null : FeatureImportanceBaselineTests.randomInstance(),
            randomBoolean()
                ? null
                : Stream.generate(HyperparametersTests::randomInstance).limit(randomIntBetween(1, 10)).collect(Collectors.toList())
        );
    }

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected TrainedModelMetadata createTestInstance() {
        return randomInstance();
    }

    @Override
    protected Writeable.Reader<TrainedModelMetadata> instanceReader() {
        return TrainedModelMetadata::new;
    }

    @Override
    protected TrainedModelMetadata doParseInstance(XContentParser parser) throws IOException {
        return TrainedModelMetadata.fromXContent(parser, lenient);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected TrainedModelMetadata mutateInstanceForVersion(TrainedModelMetadata instance, Version version) {
        return instance;
    }
}
