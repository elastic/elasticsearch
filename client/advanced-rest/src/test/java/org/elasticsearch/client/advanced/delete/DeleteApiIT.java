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

package org.elasticsearch.client.advanced.delete;


import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.advanced.AdvancedRestClient;
import org.elasticsearch.client.advanced.RequestTestUtil;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.client.advanced.AdvancedRestClient.toMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class DeleteApiIT extends ESRestTestCase {

    public void testDeleteRequest() throws IOException {
        client().performRequest("PUT", "foo");
        client().performRequest("PUT", "foo/bar/1", Collections.emptyMap(),
            new StringEntity("{\"foo\":\"bar\"}", ContentType.APPLICATION_JSON));

        AdvancedRestClient client = new AdvancedRestClient(client());
        DeleteRestResponse delete = client.delete(DeleteRestRequest.builder().setIndex("foo").setType("bar").setId("1").build());
        assertThat(delete.isFound(), is(true));

        try {
            client.delete(DeleteRestRequest.builder().setIndex("foo").setType("bar").setId("1").build());
            fail("A 404 ResponseException should have been raised.");
        } catch (ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(404));

            DeleteRestResponse error = DeleteRestOperation.toRestResponse(toMap(e.getResponse()));
            assertThat(error.isFound(), is(false));
        }
    }

    public void testDeleteRequestAsync() throws IOException, InterruptedException {
        client().performRequest("PUT", "foo");
        client().performRequest("PUT", "foo/bar/1", Collections.emptyMap(),
            new StringEntity("{\"foo\":\"bar\"}", ContentType.APPLICATION_JSON));

        AdvancedRestClient client = new AdvancedRestClient(client());
        RequestTestUtil.MockConsumer<DeleteRestResponse> listenerResponse1 = new RequestTestUtil.MockConsumer<>();
        RequestTestUtil.MockConsumer<Exception> listenerException1 = new RequestTestUtil.MockConsumer<>();
        client.delete(DeleteRestRequest.builder().setIndex("foo").setType("bar").setId("1").build(), listenerResponse1, listenerException1);

        awaitBusy(() -> listenerResponse1.get() != null);
        assertThat(listenerException1.get(), nullValue());

        DeleteRestResponse response = listenerResponse1.get();
        assertThat(response.isFound(), is(true));

        RequestTestUtil.MockConsumer<DeleteRestResponse> listenerResponse2 = new RequestTestUtil.MockConsumer<>();
        RequestTestUtil.MockConsumer<Exception> listenerException2 = new RequestTestUtil.MockConsumer<>();
        client.delete(DeleteRestRequest.builder().setIndex("foo").setType("bar").setId("1").build(), listenerResponse2, listenerException2);

        awaitBusy(() -> listenerException2.get() != null);
        Exception exception = listenerException2.get();
        assertThat(exception, instanceOf(ResponseException.class));
        assertThat(((ResponseException)exception).getResponse().getStatusLine().getStatusCode(), is(404));
    }
}
