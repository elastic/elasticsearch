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
package org.elasticsearch.client.watcher.hlrc;

import org.elasticsearch.client.watcher.PutWatchResponse;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.client.AbstractHlrcXContentTestCase;

import java.io.IOException;

public class PutWatchResponseTests extends AbstractHlrcXContentTestCase<
    org.elasticsearch.protocol.xpack.watcher.PutWatchResponse, PutWatchResponse> {

    @Override
    protected org.elasticsearch.protocol.xpack.watcher.PutWatchResponse createTestInstance() {
        String id = randomAlphaOfLength(10);
        long seqNo = randomNonNegativeLong();
        long primaryTerm = randomLongBetween(1, 20);
        long version = randomLongBetween(1, 10);
        boolean created = randomBoolean();
        return new org.elasticsearch.protocol.xpack.watcher.PutWatchResponse(id, version, seqNo, primaryTerm, created);
    }

    @Override
    protected org.elasticsearch.protocol.xpack.watcher.PutWatchResponse doParseInstance(XContentParser parser) throws IOException {
        return org.elasticsearch.protocol.xpack.watcher.PutWatchResponse.fromXContent(parser);
    }

    @Override
    public PutWatchResponse doHlrcParseInstance(XContentParser parser) throws IOException {
        return org.elasticsearch.client.watcher.PutWatchResponse.fromXContent(parser);
    }

    @Override
    public org.elasticsearch.protocol.xpack.watcher.PutWatchResponse convertHlrcToInternal(PutWatchResponse instance) {
        return new org.elasticsearch.protocol.xpack.watcher.PutWatchResponse(instance.getId(), instance.getVersion(),
            instance.getSeqNo(), instance.getPrimaryTerm(), instance.isCreated());
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
