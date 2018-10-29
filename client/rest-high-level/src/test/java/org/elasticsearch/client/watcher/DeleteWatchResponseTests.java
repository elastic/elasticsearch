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

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class DeleteWatchResponseTests extends AbstractXContentTestCase<DeleteWatchResponse> {

    @Override
    protected DeleteWatchResponse createTestInstance() {
        String id = randomAlphaOfLength(10);
        long version = randomLongBetween(1, 10);
        boolean found = randomBoolean();
        return new DeleteWatchResponse(id, version, found);
    }

    @Override
    protected DeleteWatchResponse doParseInstance(XContentParser parser) throws IOException {
        return DeleteWatchResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
