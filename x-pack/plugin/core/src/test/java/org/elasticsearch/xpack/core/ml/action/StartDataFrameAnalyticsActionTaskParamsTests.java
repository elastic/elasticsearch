
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.MlConfigVersionUtils;

import java.io.IOException;

public class StartDataFrameAnalyticsActionTaskParamsTests extends AbstractXContentSerializingTestCase<
    StartDataFrameAnalyticsAction.TaskParams> {

    @Override
    protected StartDataFrameAnalyticsAction.TaskParams doParseInstance(XContentParser parser) throws IOException {
        return StartDataFrameAnalyticsAction.TaskParams.fromXContent(parser);
    }

    @Override
    protected StartDataFrameAnalyticsAction.TaskParams createTestInstance() {
        return new StartDataFrameAnalyticsAction.TaskParams(
            randomAlphaOfLength(10),
            MlConfigVersionUtils.randomVersion(random()),
            randomBoolean()
        );
    }

    @Override
    protected StartDataFrameAnalyticsAction.TaskParams mutateInstance(StartDataFrameAnalyticsAction.TaskParams instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
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
