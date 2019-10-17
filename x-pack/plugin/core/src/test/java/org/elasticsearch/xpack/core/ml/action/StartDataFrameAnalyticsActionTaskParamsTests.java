
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StartDataFrameAnalyticsActionTaskParamsTests extends AbstractSerializingTestCase<StartDataFrameAnalyticsAction.TaskParams> {

    @Override
    protected StartDataFrameAnalyticsAction.TaskParams doParseInstance(XContentParser parser) throws IOException {
        return StartDataFrameAnalyticsAction.TaskParams.fromXContent(parser);
    }

    @Override
    protected StartDataFrameAnalyticsAction.TaskParams createTestInstance() {
        int phaseCount = randomIntBetween(0, 5);
        List<PhaseProgress> progressOnStart = new ArrayList<>(phaseCount);
        for (int i = 0; i < phaseCount; i++) {
            progressOnStart.add(new PhaseProgress(randomAlphaOfLength(10), randomIntBetween(0, 100)));
        }
        return new StartDataFrameAnalyticsAction.TaskParams(randomAlphaOfLength(10), Version.CURRENT, progressOnStart, randomBoolean());
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
