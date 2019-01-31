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
package org.elasticsearch.client.watcher;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class PutWatchResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser,
            PutWatchResponseTests::createTestInstance,
            PutWatchResponseTests::toXContent,
            PutWatchResponse::fromXContent)
            .supportsUnknownFields(true)
            .assertToXContentEquivalence(false)
            .test();
    }

    private static XContentBuilder toXContent(PutWatchResponse response, XContentBuilder builder) throws IOException {
        return builder.startObject()
            .field("_id", response.getId())
            .field("_version", response.getVersion())
            .field("_seq_no", response.getSeqNo())
            .field("_primary_term", response.getPrimaryTerm())
            .field("created", response.isCreated())
            .endObject();
    }

    private static PutWatchResponse createTestInstance() {
        String id = randomAlphaOfLength(10);
        long seqNo = randomNonNegativeLong();
        long primaryTerm = randomLongBetween(1, 200);
        long version = randomLongBetween(1, 10);
        boolean created = randomBoolean();
        return new PutWatchResponse(id, version, seqNo, primaryTerm, created);
    }
}
