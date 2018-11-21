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
package org.elasticsearch.client.ml;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;


public class UpdateModelSnapshotRequestTests extends AbstractXContentTestCase<UpdateModelSnapshotRequest> {

    @Override
    protected UpdateModelSnapshotRequest createTestInstance() {
        String jobId = randomAlphaOfLengthBetween(1, 20);
        String snapshotId = randomAlphaOfLengthBetween(1, 20);
        UpdateModelSnapshotRequest request = new UpdateModelSnapshotRequest(jobId, snapshotId);
        if (randomBoolean()) {
            request.setDescription(String.valueOf(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            request.setRetain(randomBoolean());
        }

        return request;
    }

    @Override
    protected UpdateModelSnapshotRequest doParseInstance(XContentParser parser) throws IOException {
        return UpdateModelSnapshotRequest.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
