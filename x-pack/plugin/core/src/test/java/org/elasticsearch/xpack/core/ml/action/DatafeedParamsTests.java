/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;

public class DatafeedParamsTests extends AbstractXContentSerializingTestCase<StartDatafeedAction.DatafeedParams> {
    @Override
    protected StartDatafeedAction.DatafeedParams doParseInstance(XContentParser parser) throws IOException {
        return StartDatafeedAction.DatafeedParams.parseRequest(null, parser);
    }

    public static StartDatafeedAction.DatafeedParams createDatafeedParams() {
        StartDatafeedAction.DatafeedParams params = new StartDatafeedAction.DatafeedParams(
            randomAlphaOfLength(10),
            randomNonNegativeLong()
        );
        if (randomBoolean()) {
            params.setEndTime(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            params.setTimeout(TimeValue.timeValueMillis(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            params.setJobId(randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            params.setDatafeedIndices(Arrays.asList(randomAlphaOfLength(10), randomAlphaOfLength(10)));
        }

        return params;
    }

    @Override
    protected StartDatafeedAction.DatafeedParams createTestInstance() {
        return createDatafeedParams();
    }

    @Override
    protected StartDatafeedAction.DatafeedParams mutateInstance(StartDatafeedAction.DatafeedParams instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<StartDatafeedAction.DatafeedParams> instanceReader() {
        return StartDatafeedAction.DatafeedParams::new;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
