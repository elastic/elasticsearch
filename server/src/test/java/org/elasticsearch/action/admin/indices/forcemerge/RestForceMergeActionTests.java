/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.forcemerge;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.admin.indices.RestForceMergeAction;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class RestForceMergeActionTests extends RestActionTestCase {

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestForceMergeAction());
    }

    public void testBodyRejection() throws Exception {
        String json = JsonXContent.contentBuilder().startObject().field("max_num_segments", 1).endObject().toString();
        final FakeRestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(
            new BytesArray(json),
            XContentType.JSON
        ).withMethod(RestRequest.Method.POST).withPath("/_forcemerge").build();

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
