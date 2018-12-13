/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class JobParamsTests extends AbstractSerializingTestCase<OpenJobAction.JobParams> {

    @Override
    protected OpenJobAction.JobParams doParseInstance(XContentParser parser) throws IOException {
        return OpenJobAction.JobParams.parseRequest(null, parser);
    }

    public static OpenJobAction.JobParams createJobParams() {
        OpenJobAction.JobParams params = new OpenJobAction.JobParams(randomAlphaOfLengthBetween(1, 20));
        if (randomBoolean()) {
            params.setTimeout(TimeValue.timeValueMillis(randomNonNegativeLong()));
        }
        return params;
    }

    @Override
    protected OpenJobAction.JobParams createTestInstance() {
        return createJobParams();
    }

    @Override
    protected Writeable.Reader<OpenJobAction.JobParams> instanceReader() {
        return OpenJobAction.JobParams::new;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
