/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.stats.outlierdetection;

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

public class OutlierDetectionStatsTests extends AbstractBWCSerializationTestCase<OutlierDetectionStats> {

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
    protected OutlierDetectionStats mutateInstanceForVersion(OutlierDetectionStats instance, Version version) {
        return instance;
    }

    @Override
    protected OutlierDetectionStats doParseInstance(XContentParser parser) throws IOException {
        return lenient ? OutlierDetectionStats.LENIENT_PARSER.apply(parser, null)
            : OutlierDetectionStats.STRICT_PARSER.apply(parser, null);
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true"));
    }

    @Override
    protected Writeable.Reader<OutlierDetectionStats> instanceReader() {
        return OutlierDetectionStats::new;
    }

    @Override
    protected OutlierDetectionStats createTestInstance() {
        return createRandom();
    }

    public static OutlierDetectionStats createRandom() {
        return new OutlierDetectionStats(
            randomAlphaOfLength(10),
            Instant.now(),
            ParametersTests.createRandom(),
            TimingStatsTests.createRandom()
        );
    }
}
