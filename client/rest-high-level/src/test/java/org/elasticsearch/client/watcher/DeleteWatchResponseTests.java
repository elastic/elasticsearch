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

public class DeleteWatchResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser,
            DeleteWatchResponseTests::createTestInstance,
            DeleteWatchResponseTests::toXContent,
            DeleteWatchResponse::fromXContent)
            .supportsUnknownFields(true)
            .assertToXContentEquivalence(false)
            .test();
    }

    private static XContentBuilder toXContent(DeleteWatchResponse response, XContentBuilder builder) throws IOException {
        return builder.startObject()
            .field("_id", response.getId())
            .field("_version", response.getVersion())
            .field("found", response.isFound())
            .endObject();
    }

    private static DeleteWatchResponse createTestInstance() {
        String id = randomAlphaOfLength(10);
        long version = randomLongBetween(1, 10);
        boolean found = randomBoolean();
        return new DeleteWatchResponse(id, version, found);
    }
}
