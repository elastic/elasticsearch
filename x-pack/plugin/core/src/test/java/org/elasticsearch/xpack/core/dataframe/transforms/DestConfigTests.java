/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Before;

import java.io.IOException;

public class DestConfigTests extends AbstractSerializingDataFrameTestCase<DestConfig> {

    private boolean lenient;

    public static DestConfig randomDestConfig() {
        return new DestConfig(randomAlphaOfLength(10),
            randomBoolean() ? null : randomAlphaOfLength(10));
    }

    @Before
    public void setRandomFeatures() {
        lenient = randomBoolean();
    }

    @Override
    protected DestConfig doParseInstance(XContentParser parser) throws IOException {
        return DestConfig.fromXContent(parser, lenient);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected DestConfig createTestInstance() {
        return randomDestConfig();
    }

    @Override
    protected Reader<DestConfig> instanceReader() {
        return DestConfig::new;
    }

}
