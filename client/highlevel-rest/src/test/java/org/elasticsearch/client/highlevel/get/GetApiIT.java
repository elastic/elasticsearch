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

package org.elasticsearch.client.highlevel.get;


import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.highlevel.ESHighLevelTestCase;
import org.elasticsearch.client.highlevel.RequestTestUtil;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.client.HighlevelClient.toGetRestResponse;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class GetApiIT extends ESHighLevelTestCase {

    public void testGetRequest() throws IOException {
        client().performRequest("PUT", "foo");

        try {
            aClient.get(GetRestRequest.builder().setIndex("foo").setType("bar").setId("1").build());
            fail("A 404 ResponseException should have been raised.");
        } catch (ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(404));

            GetRestResponse error = toGetRestResponse(e.getResponse());
            assertThat(error.isFound(), is(false));
            assertThat(error.getSource(), nullValue());
            assertThat(error.getIndex(), is("foo"));
            assertThat(error.getType(), is("bar"));
            assertThat(error.getId(), is("1"));
            assertThat(error.getVersion(), nullValue());
        }

        client().performRequest("PUT", "foo/bar/1", Collections.emptyMap(),
            new StringEntity("{\"foo\":\"bar\"}", ContentType.APPLICATION_JSON));

        GetRestResponse response = aClient.get(GetRestRequest.builder().setIndex("foo").setType("bar").setId("1").build());
        assertThat(response.isFound(), is(true));
        assertThat(response.getIndex(), is("foo"));
        assertThat(response.getType(), is("bar"));
        assertThat(response.getId(), is("1"));
        assertThat(response.getVersion(), is(1));
        assertThat(response.getSource(), hasEntry("foo", "bar"));
    }

    public void testGetRequestAsync() throws IOException, InterruptedException {
        client().performRequest("PUT", "foo");

        RequestTestUtil.MockConsumer<GetRestResponse> listenerResponse2 = new RequestTestUtil.MockConsumer<>();
        RequestTestUtil.MockConsumer<Exception> listenerException2 = new RequestTestUtil.MockConsumer<>();
        aClient.get(GetRestRequest.builder().setIndex("foo").setType("bar").setId("1").build(), listenerResponse2, listenerException2);

        awaitBusy(() -> listenerException2.get() != null);
        Exception exception = listenerException2.get();
        assertThat(exception, instanceOf(ResponseException.class));
        assertThat(((ResponseException)exception).getResponse().getStatusLine().getStatusCode(), is(404));

        client().performRequest("PUT", "foo/bar/1", Collections.emptyMap(),
            new StringEntity("{\"foo\":\"bar\"}", ContentType.APPLICATION_JSON));

        RequestTestUtil.MockConsumer<GetRestResponse> listenerResponse1 = new RequestTestUtil.MockConsumer<>();
        RequestTestUtil.MockConsumer<Exception> listenerException1 = new RequestTestUtil.MockConsumer<>();
        aClient.get(GetRestRequest.builder().setIndex("foo").setType("bar").setId("1").build(), listenerResponse1, listenerException1);

        awaitBusy(() -> listenerResponse1.get() != null);
        assertThat(listenerException1.get(), nullValue());

        GetRestResponse response = listenerResponse1.get();
        assertThat(response.isFound(), is(true));
        assertThat(response.getSource(), hasEntry("foo", "bar"));
        assertThat(response.getIndex(), is("foo"));
        assertThat(response.getType(), is("bar"));
        assertThat(response.getId(), is("1"));
        assertThat(response.getVersion(), is(1));
    }
}
