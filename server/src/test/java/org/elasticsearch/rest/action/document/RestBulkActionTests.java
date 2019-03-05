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

package org.elasticsearch.rest.action.document;

import java.util.HashMap;
import java.util.Map;
import org.elasticsearch.Version;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.hamcrest.CustomMatcher;
import org.mockito.Mockito;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link RestBulkAction}.
 */
public class RestBulkActionTests extends ESTestCase {

    public void testBulkPipelineUpsert() throws Exception {
        final NodeClient mockClient = mock(NodeClient.class);
        final Map<String, String> params = new HashMap<>();
        params.put("pipeline", "timestamps");
        new RestBulkAction(settings(Version.CURRENT).build(), mock(RestController.class))
            .handleRequest(
                new FakeRestRequest.Builder(
                    xContentRegistry()).withPath("my_index/_bulk").withParams(params)
                    .withContent(
                        new BytesArray(
                            "{\"index\":{\"_id\":\"1\"}}\n" +
                                "{\"field1\":\"val1\"}\n" +
                                "{\"update\":{\"_id\":\"2\"}}\n" +
                                "{\"script\":{\"source\":\"ctx._source.counter++;\"},\"upsert\":{\"field1\":\"upserted_val\"}}\n"
                        ),
                        XContentType.JSON
                    ).withMethod(RestRequest.Method.POST).build(),
                mock(RestChannel.class), mockClient
            );
        Mockito.verify(mockClient)
            .bulk(argThat(new CustomMatcher<BulkRequest>("Pipeline in upsert request") {
                @Override
                public boolean matches(final Object item) {
                    BulkRequest request = (BulkRequest) item;
                    UpdateRequest update = (UpdateRequest) request.requests().get(1);
                    return "timestamps".equals(update.upsertRequest().getPipeline());
                }
            }), any());
    }
}
