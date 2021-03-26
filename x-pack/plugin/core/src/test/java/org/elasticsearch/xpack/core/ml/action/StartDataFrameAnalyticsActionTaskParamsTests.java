
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

import static org.elasticsearch.test.VersionUtils.randomVersion;

public class StartDataFrameAnalyticsActionTaskParamsTests extends AbstractSerializingTestCase<StartDataFrameAnalyticsAction.TaskParams> {

    @Override
    protected StartDataFrameAnalyticsAction.TaskParams doParseInstance(XContentParser parser) throws IOException {
        return StartDataFrameAnalyticsAction.TaskParams.fromXContent(parser);
    }

    @Override
    protected StartDataFrameAnalyticsAction.TaskParams createTestInstance() {
        return new StartDataFrameAnalyticsAction.TaskParams(
            randomAlphaOfLength(10),
            randomVersion(random()),
            randomBoolean());
    }

    @Override
    protected Writeable.Reader<StartDataFrameAnalyticsAction.TaskParams> instanceReader() {
        return StartDataFrameAnalyticsAction.TaskParams::new;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
