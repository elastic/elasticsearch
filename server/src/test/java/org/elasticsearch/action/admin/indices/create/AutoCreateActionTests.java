/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AutoCreateActionTests extends ESTestCase {

    public void testAutoCreate() {
        AutoCreateAction.Request request =
            new AutoCreateAction.Request(Set.of("index1", "index2", "index3"), "reason", randomBoolean() ? null : randomBoolean());
        Client client = mock(Client.class);
        Mockito.doAnswer(invocationOnMock -> {
            CreateIndexRequest createIndexRequest = (CreateIndexRequest) invocationOnMock.getArguments()[1];
            assertThat(createIndexRequest.index(), in(request.getNames()));
            assertThat(createIndexRequest.cause(), equalTo(request.getCause()));
            assertThat(createIndexRequest.preferV2Templates(), equalTo((request.getPreferV2Templates())));

            @SuppressWarnings("unchecked")
            ActionListener<CreateIndexResponse> listener = (ActionListener<CreateIndexResponse>) invocationOnMock.getArguments()[2];
            listener.onResponse(new CreateIndexResponse(true, true, createIndexRequest.index()));
            return invocationOnMock;
        }).when(client).execute(same(AutoCreateIndexAction.INSTANCE), any(), any());

        AutoCreateAction.Response[] holder = new AutoCreateAction.Response[1];
        AutoCreateAction.autoCreate(request,
            ActionListener.wrap(r -> holder[0] = r, e -> {throw new RuntimeException(e);}), client);
        AutoCreateAction.Response result = holder[0];

        assertThat(result.getFailureByNames().size(), equalTo(3));
        assertThat(result.getFailureByNames().containsKey("index1"), is(true));
        assertThat(result.getFailureByNames().get("index1"), nullValue());
        assertThat(result.getFailureByNames().containsKey("index2"), is(true));
        assertThat(result.getFailureByNames().get("index2"), nullValue());
        assertThat(result.getFailureByNames().containsKey("index3"), is(true));
        assertThat(result.getFailureByNames().get("index3"), nullValue());

        verify(client, times(3)).execute(same(AutoCreateIndexAction.INSTANCE), any(), any());
    }

    public void testAutoCreateFailure() {
        AutoCreateAction.Request request =
            new AutoCreateAction.Request(Set.of("index1", "index2", "index3"), "reason", randomBoolean() ? null : randomBoolean());
        Client client = mock(Client.class);
        Mockito.doAnswer(invocationOnMock -> {
            CreateIndexRequest createIndexRequest = (CreateIndexRequest) invocationOnMock.getArguments()[1];
            assertThat(createIndexRequest.index(), in(request.getNames()));
            assertThat(createIndexRequest.cause(), equalTo(request.getCause()));
            assertThat(createIndexRequest.preferV2Templates(), equalTo((request.getPreferV2Templates())));

            @SuppressWarnings("unchecked")
            ActionListener<CreateIndexResponse> listener = (ActionListener<CreateIndexResponse>) invocationOnMock.getArguments()[2];
            if ("index2".equals(createIndexRequest.index())) {
                listener.onFailure(new Exception("fail!"));
            } else {
                listener.onResponse(new CreateIndexResponse(true, true, createIndexRequest.index()));
            }

            return invocationOnMock;
        }).when(client).execute(same(AutoCreateIndexAction.INSTANCE), any(), any());

        AutoCreateAction.Response[] holder = new AutoCreateAction.Response[1];
        AutoCreateAction.autoCreate(request,
            ActionListener.wrap(r -> holder[0] = r, e -> {throw new RuntimeException(e);}), client);
        AutoCreateAction.Response result = holder[0];

        assertThat(result.getFailureByNames().size(), equalTo(3));
        assertThat(result.getFailureByNames().containsKey("index1"), is(true));
        assertThat(result.getFailureByNames().get("index1"), nullValue());
        assertThat(result.getFailureByNames().containsKey("index2"), is(true));
        assertThat(result.getFailureByNames().get("index2"), notNullValue());
        assertThat(result.getFailureByNames().get("index2").getMessage(), equalTo("fail!"));
        assertThat(result.getFailureByNames().containsKey("index3"), is(true));
        assertThat(result.getFailureByNames().get("index3"), nullValue());

        verify(client, times(3)).execute(same(AutoCreateIndexAction.INSTANCE), any(), any());
    }

}
