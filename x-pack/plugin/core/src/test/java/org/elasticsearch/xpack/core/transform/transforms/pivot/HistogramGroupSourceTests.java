/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class HistogramGroupSourceTests extends AbstractSerializingTestCase<HistogramGroupSource> {

    public static HistogramGroupSource randomHistogramGroupSource() {
        return randomHistogramGroupSource(Version.CURRENT);
    }

    public static HistogramGroupSource randomHistogramGroupSourceNoScript() {
        return randomHistogramGroupSource(Version.CURRENT, false);
    }

    public static HistogramGroupSource randomHistogramGroupSourceNoScript(String fieldPrefix) {
        return randomHistogramGroupSource(Version.CURRENT, false, fieldPrefix);
    }

    public static HistogramGroupSource randomHistogramGroupSource(Version version) {
        return randomHistogramGroupSource(version, randomBoolean());
    }

    public static HistogramGroupSource randomHistogramGroupSource(Version version, boolean withScript) {
        return randomHistogramGroupSource(version, withScript, "");
    }

    public static HistogramGroupSource randomHistogramGroupSource(Version version, boolean withScript, String fieldPrefix) {
        ScriptConfig scriptConfig = null;
        String field;

        // either a field or a script must be specified, it's possible to have both, but disallowed to have none
        if (version.onOrAfter(Version.V_7_7_0) && withScript) {
            scriptConfig = ScriptConfigTests.randomScriptConfig();
            field = randomBoolean() ? null : fieldPrefix + randomAlphaOfLengthBetween(1, 20);
        } else {
            field = fieldPrefix + randomAlphaOfLengthBetween(1, 20);
        }

        boolean missingBucket = version.onOrAfter(Version.V_7_10_0) ? randomBoolean() : false;
        double interval = randomDoubleBetween(Math.nextUp(0), Double.MAX_VALUE, false);
        return new HistogramGroupSource(field, scriptConfig, missingBucket, interval);
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
