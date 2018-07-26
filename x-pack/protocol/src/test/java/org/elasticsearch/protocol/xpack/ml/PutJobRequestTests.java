/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.protocol.xpack.ml;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.protocol.xpack.ml.job.config.Job;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

import java.io.IOException;
import java.util.Date;

import static org.elasticsearch.protocol.xpack.ml.job.config.JobTests.buildJobBuilder;
import static org.elasticsearch.protocol.xpack.ml.job.config.JobTests.randomValidJobId;

public class PutJobRequestTests extends AbstractStreamableXContentTestCase<PutJobRequest> {

    private final String jobId = randomValidJobId();

    @Override
    protected PutJobRequest createTestInstance() {
        Job.Builder jobConfiguration = buildJobBuilder(jobId, null);
        return new PutJobRequest(jobConfiguration);
    }

    @Override
    protected PutJobRequest createBlankInstance() {
        return new PutJobRequest();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected PutJobRequest doParseInstance(XContentParser parser) {
        return PutJobRequest.parseRequest(jobId, parser);
    }

    public void testParseRequest_InvalidCreateSetting() throws IOException {
        Job.Builder jobConfiguration = buildJobBuilder(jobId, null);
        jobConfiguration.setLastDataTime(new Date());
        BytesReference bytes = XContentHelper.toXContent(jobConfiguration, XContentType.JSON, false);
        XContentParser parser = createParser(XContentType.JSON.xContent(), bytes);
        expectThrows(IllegalArgumentException.class, () -> PutJobRequest.parseRequest(jobId, parser));
    }
}
