/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.ml.action.PutJobAction.Request;
import org.elasticsearch.xpack.ml.job.config.Job;

import java.io.IOException;
import java.util.Date;

import static org.elasticsearch.xpack.ml.job.config.JobTests.buildJobBuilder;
import static org.elasticsearch.xpack.ml.job.config.JobTests.randomValidJobId;

public class PutJobActionRequestTests extends AbstractStreamableXContentTestCase<Request> {

    private final String jobId = randomValidJobId();

    @Override
    protected Request createTestInstance() {
        Job.Builder jobConfiguration = buildJobBuilder(jobId, null);
        return new Request(jobConfiguration);
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

    @Override
    protected Request doParseInstance(XContentParser parser) {
        return Request.parseRequest(jobId, parser);
    }

    public void testParseRequest_InvalidCreateSetting() throws IOException {
        Job.Builder jobConfiguration = buildJobBuilder(jobId, null);
        jobConfiguration.setLastDataTime(new Date());
        XContentBuilder xContentBuilder = toXContent(jobConfiguration, XContentType.JSON);
        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(NamedXContentRegistry.EMPTY, xContentBuilder.bytes());

        expectThrows(IllegalArgumentException.class, () -> Request.parseRequest(jobId, parser));
    }
}
