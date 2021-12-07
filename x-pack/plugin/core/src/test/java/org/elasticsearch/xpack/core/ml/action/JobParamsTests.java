/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;

import java.io.IOException;
import java.util.function.Predicate;

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
        if (randomBoolean()) {
            params.setJob(JobTests.createRandomizedJob());
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

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // Don't insert random fields into the job object as the
        // custom_fields member accepts arbitrary fields and new
        // fields inserted there will result in object inequality
        return path -> path.startsWith(OpenJobAction.JobParams.JOB.getPreferredName());
    }
}
