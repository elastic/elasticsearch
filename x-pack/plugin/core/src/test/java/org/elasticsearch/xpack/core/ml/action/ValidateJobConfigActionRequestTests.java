/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.action.ValidateJobConfigAction.Request;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;
import java.util.Date;

import static org.elasticsearch.xpack.core.ml.job.config.JobTests.buildJobBuilder;
import static org.elasticsearch.xpack.core.ml.job.config.JobTests.randomValidJobId;

public class ValidateJobConfigActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        return new Request(buildJobBuilder(randomValidJobId(), new Date()).build());
    }

    @Override
    protected Request mutateInstance(Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    public void testParseRequest_InvalidCreateSetting() throws IOException {
        String jobId = randomValidJobId();
        Job.Builder jobConfiguration = buildJobBuilder(jobId, null);
        jobConfiguration.setFinishedTime(new Date());

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        XContentBuilder xContentBuilder = jobConfiguration.build(new Date()).toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                BytesReference.bytes(xContentBuilder).streamInput()
            );

        expectThrows(IllegalArgumentException.class, () -> Request.parseRequest(parser));
    }
}
