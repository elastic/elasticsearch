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
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.advanced.AdvancedRestClient;
import org.elasticsearch.client.advanced.RequestTestUtil;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class DeleteApiIT extends ESRestTestCase {

    public void testDeleteRequestAsString() throws IOException {
        client().performRequest("PUT", "foo");
        client().performRequest("PUT", "foo/bar/1", Collections.emptyMap(),
            new StringEntity("{\"foo\":\"bar\"}", ContentType.APPLICATION_JSON));

        AdvancedRestClient client = new AdvancedRestClient(client());
        String delete = client.deleteAsString(DeleteRestRequest.builder().setIndex("foo").setType("bar").setId("1").build());
        assertThat(delete, containsString("\"found\":true"));

        try {
            client.delete(DeleteRestRequest.builder().setIndex("foo").setType("bar").setId("1").build());
            fail("A 404 ResponseException should have been raised.");
        } catch (ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(404));
            assertThat(EntityUtils.toString(e.getResponse().getEntity()), containsString("\"found\":false"));
        }
    }

    public void testDeleteRequestAsMap() throws IOException {
        client().performRequest("PUT", "foo");
        client().performRequest("PUT", "foo/bar/1", Collections.emptyMap(),
            new StringEntity("{\"foo\":\"bar\"}", ContentType.APPLICATION_JSON));

        AdvancedRestClient client = new AdvancedRestClient(client());
        Map<String, Object> delete = client.deleteAsMap(DeleteRestRequest.builder().setIndex("foo").setType("bar").setId("1").build());
        assertThat(delete, hasEntry("found", true));

        try {
            client.delete(DeleteRestRequest.builder().setIndex("foo").setType("bar").setId("1").build());
            fail("A 404 ResponseException should have been raised.");
        } catch (ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(404));

            Map<String, Object> error = new DeleteRestOperation().toMap(e.getResponse());
            assertThat(error, hasEntry("found", false));
        }
    }

    public void testDeleteRequestAsRestResponse() throws IOException {
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

            DeleteRestResponse error = new DeleteRestOperation().toRestResponse(e.getResponse());
            assertThat(error.isFound(), is(false));
        }
    }

    public void testDeleteRequestAsync() throws IOException, InterruptedException {
        client().performRequest("PUT", "foo");
        client().performRequest("PUT", "foo/bar/1", Collections.emptyMap(),
            new StringEntity("{\"foo\":\"bar\"}", ContentType.APPLICATION_JSON));

        AdvancedRestClient client = new AdvancedRestClient(client());
        RequestTestUtil.MockResponseListener<DeleteRestResponse> listener1 = new RequestTestUtil.MockResponseListener<>();
        client.delete(DeleteRestRequest.builder().setIndex("foo").setType("bar").setId("1").build(), listener1);

        awaitBusy(() -> listener1.getResponse() != null);
        assertThat(listener1.getException(), nullValue());

        DeleteRestResponse response = listener1.getResponse();
        assertThat(response.isFound(), is(false));

        RequestTestUtil.MockResponseListener<DeleteRestResponse> listener2 = new RequestTestUtil.MockResponseListener<>();
        client.delete(DeleteRestRequest.builder().setIndex("foo").setType("bar").setId("1").build(), listener2);

        awaitBusy(() -> listener2.getException() != null);
        Exception exception = listener2.getException();
        assertThat(exception, instanceOf(ResponseException.class));
        assertThat(((ResponseException)exception).getResponse().getStatusLine().getStatusCode(), is(404));
    }
}
