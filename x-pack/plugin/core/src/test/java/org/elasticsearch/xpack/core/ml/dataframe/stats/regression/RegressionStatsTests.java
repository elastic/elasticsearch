/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.stats.regression;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;

public class RegressionStatsTests extends AbstractBWCSerializationTestCase<RegressionStats> {

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
    protected RegressionStats mutateInstanceForVersion(RegressionStats instance, Version version) {
        return instance;
    }

    @Override
    protected RegressionStats doParseInstance(XContentParser parser) throws IOException {
        return lenient ? RegressionStats.LENIENT_PARSER.apply(parser, null) : RegressionStats.STRICT_PARSER.apply(parser, null);
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true"));
    }

    @Override
    protected Writeable.Reader<RegressionStats> instanceReader() {
        return RegressionStats::new;
    }

    @Override
    protected RegressionStats createTestInstance() {
        return createRandom();
    }

    @Override
    protected RegressionStats mutateInstance(RegressionStats instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public static RegressionStats createRandom() {
        return new RegressionStats(
            randomAlphaOfLength(10),
            Instant.now(),
            randomIntBetween(1, Integer.MAX_VALUE),
            HyperparametersTests.createRandom(),
            TimingStatsTests.createRandom(),
            ValidationLossTests.createRandom()
        );
    }
}
