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

package org.elasticsearch.action.admin.indices.forcemerge;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.admin.indices.RestForceMergeAction;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Before;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class RestForceMergeActionTests extends RestActionTestCase {

    @Before
    public void setUpAction() {
        new RestForceMergeAction(controller());
    }

    public void testBodyRejection() throws Exception {
        String json = JsonXContent.contentBuilder().startObject().field("max_num_segments", 1).endObject().toString();
        final FakeRestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
                .withContent(new BytesArray(json), XContentType.JSON)
                .withMethod(RestRequest.Method.POST)
                .withPath("/_forcemerge")
                .build();

        final SetOnce<RestResponse> responseSetOnce = new SetOnce<>();
        dispatchRequest(request, new AbstractRestChannel(request, true) {
            @Override
            public void sendResponse(RestResponse response) {
                responseSetOnce.set(response);
            }
        });

        final RestResponse response = responseSetOnce.get();
        assertThat(response, notNullValue());
        assertThat(response.status(), is(RestStatus.BAD_REQUEST));
        assertThat(response.content().utf8ToString(), containsString("request [POST /_forcemerge] does not support having a body"));
    }

    protected void dispatchRequest(final RestRequest request, final RestChannel channel) {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        controller().dispatchRequest(request, channel, threadContext);
    }
}
