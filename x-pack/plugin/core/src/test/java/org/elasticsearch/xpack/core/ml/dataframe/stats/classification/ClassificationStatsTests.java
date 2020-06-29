/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.stats.classification;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;

public class ClassificationStatsTests extends AbstractBWCSerializationTestCase<ClassificationStats> {

    private boolean lenient;

    @Before
    public void chooseLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected ClassificationStats mutateInstanceForVersion(ClassificationStats instance, Version version) {
        return instance;
    }

    @Override
    protected ClassificationStats doParseInstance(XContentParser parser) throws IOException {
        return lenient ? ClassificationStats.LENIENT_PARSER.apply(parser, null) : ClassificationStats.STRICT_PARSER.apply(parser, null);
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true"));
    }

    @Override
    protected Writeable.Reader<ClassificationStats> instanceReader() {
        return ClassificationStats::new;
    }

    @Override
    protected ClassificationStats createTestInstance() {
        return createRandom();
    }

    public static ClassificationStats createRandom() {
        return new ClassificationStats(
            randomAlphaOfLength(10),
            Instant.now(),
            randomIntBetween(1, Integer.MAX_VALUE),
            HyperparametersTests.createRandom(),
            TimingStatsTests.createRandom(),
            ValidationLossTests.createRandom()
        );
    }
}
