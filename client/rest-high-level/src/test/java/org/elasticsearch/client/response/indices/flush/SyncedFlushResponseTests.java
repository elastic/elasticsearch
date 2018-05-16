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
package org.elasticsearch.client.response.indices.flush;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

public class SyncedFlushResponseTests extends AbstractSyncedFlushTest {

    public void testXContentSerialization() throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());
        TestPlan plan = createTestPlan();

        XContentBuilder serverResponsebuilder = XContentBuilder.builder(xContentType.xContent());
        assertNotNull(plan.result);
        serverResponsebuilder.startObject();
        plan.result.toXContent(serverResponsebuilder, ToXContent.EMPTY_PARAMS);
        serverResponsebuilder.endObject();
        XContentBuilder clientResponsebuilder = XContentBuilder.builder(xContentType.xContent());
        assertNotNull(plan.result);
        clientResponsebuilder.startObject();
        plan.clientResult.toXContent(clientResponsebuilder, ToXContent.EMPTY_PARAMS);
        clientResponsebuilder.endObject();
        Map<String, Object> serverContentMap = convertFailureListToSet(
            serverResponsebuilder
                .generator()
                .contentType()
                .xContent()
                .createParser(
                    xContentRegistry(),
                    LoggingDeprecationHandler.INSTANCE,
                    BytesReference.bytes(serverResponsebuilder).streamInput()
                ).map()
        );
        Map<String, Object> clientContentMap = convertFailureListToSet(
            clientResponsebuilder
                .generator()
                .contentType()
                .xContent()
                .createParser(
                    xContentRegistry(),
                    LoggingDeprecationHandler.INSTANCE,
                    BytesReference.bytes(clientResponsebuilder).streamInput()
                )
                .map()
        );
        assertEquals(serverContentMap, clientContentMap);
    }

    public void testXContentDeserialization() throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());
        TestPlan plan = createTestPlan();
        XContentBuilder builder = XContentBuilder.builder(xContentType.xContent());
        builder.startObject();
        plan.result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        XContentParser parser = builder
            .generator()
            .contentType()
            .xContent()
            .createParser(
                xContentRegistry(), LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput()
            );
        SyncedFlushResponse originalResponse = plan.clientResult;
        SyncedFlushResponse parsedResponse = SyncedFlushResponse.fromXContent(parser);
        assertNotNull(parsedResponse);
        assertShardCounts(originalResponse.getShardCounts(), parsedResponse.getShardCounts());
        for (Map.Entry<String, SyncedFlushResponse.IndexResult> entry: originalResponse.getIndexResults().entrySet()) {
            String index = entry.getKey();
            SyncedFlushResponse.IndexResult responseResult = entry.getValue();
            SyncedFlushResponse.IndexResult parsedResult = parsedResponse.getIndexResults().get(index);
            assertNotNull(responseResult);
            assertNotNull(parsedResult);
            assertShardCounts(responseResult.getShardCounts(), parsedResult.getShardCounts());
            assertEquals(responseResult.failures().size(), parsedResult.failures().size());
            for (SyncedFlushResponse.ShardFailure responseShardFailure: responseResult.failures()) {
                assertTrue(containsFailure(parsedResult.failures(), responseShardFailure));
            }
        }
    }
}
