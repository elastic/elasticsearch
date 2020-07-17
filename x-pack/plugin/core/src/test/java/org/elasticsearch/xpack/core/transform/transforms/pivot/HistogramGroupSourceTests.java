/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class HistogramGroupSourceTests extends AbstractSerializingTestCase<HistogramGroupSource> {

    public static HistogramGroupSource randomHistogramGroupSource() {
        String field = randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20);
        ScriptConfig scriptConfig = randomBoolean() ? null : ScriptConfigTests.randomScriptConfig();

        double interval = randomDoubleBetween(Math.nextUp(0), Double.MAX_VALUE, false);
        return new HistogramGroupSource(field, scriptConfig, interval);
    }

    @Override
    protected HistogramGroupSource doParseInstance(XContentParser parser) throws IOException {
        return HistogramGroupSource.fromXContent(parser, false);
    }

    @Override
    protected HistogramGroupSource createTestInstance() {
        return randomHistogramGroupSource();
    }

    @Override
    protected Reader<HistogramGroupSource> instanceReader() {
        return HistogramGroupSource::new;
    }

}
