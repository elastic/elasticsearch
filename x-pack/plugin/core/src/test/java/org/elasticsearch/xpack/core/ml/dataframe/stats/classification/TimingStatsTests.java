/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.stats.classification;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.Before;

import java.io.IOException;

public class TimingStatsTests extends AbstractBWCSerializationTestCase<TimingStats> {

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
    protected TimingStats mutateInstanceForVersion(TimingStats instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected TimingStats doParseInstance(XContentParser parser) throws IOException {
        return TimingStats.fromXContent(parser, lenient);
    }

    @Override
    protected Writeable.Reader<TimingStats> instanceReader() {
        return TimingStats::new;
    }

    @Override
    protected TimingStats createTestInstance() {
        return createRandom();
    }

    @Override
    protected TimingStats mutateInstance(TimingStats instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public static TimingStats createRandom() {
        return new TimingStats(TimeValue.timeValueMillis(randomNonNegativeLong()), TimeValue.timeValueMillis(randomNonNegativeLong()));
    }
}
