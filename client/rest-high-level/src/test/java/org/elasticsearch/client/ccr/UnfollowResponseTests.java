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

package org.elasticsearch.client.ccr;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class UnfollowResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(
                this::createParser,
                this::createTestInstance,
                UnfollowResponseTests::toXContent,
                UnfollowResponse::fromXContent).supportsUnknownFields(true)
                .test();
    }

    private UnfollowResponse createTestInstance() {
        final boolean retentionLeasesRemoved = randomBoolean();
        return new UnfollowResponse(
                randomBoolean(),
                retentionLeasesRemoved,
                retentionLeasesRemoved ? null : new ElasticsearchException("failure"));
    }

    public static void toXContent(final UnfollowResponse response, final XContentBuilder builder) throws IOException {
        builder.startObject();
        {
            builder.field(UnfollowResponse.ACKNOWLEDGED.getPreferredName(), response.isAcknowledged());
            builder.field(UnfollowResponse.RETENTION_LEASES_REMOVED.getPreferredName(), response.isRetentionLeasesRemoved());
            if (response.retentionLeasesRemovalFailureCause() != null) {
                builder.field(
                        UnfollowResponse.RETENTION_LEASES_REMOVAL_FAILURE_CAUSE.getPreferredName(),
                        response.retentionLeasesRemovalFailureCause());
            }
        }
        builder.endObject();
    }

}
