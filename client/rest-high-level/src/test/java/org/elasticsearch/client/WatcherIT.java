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
package org.elasticsearch.client;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.protocol.xpack.watcher.ActivateWatchRequest;
import org.elasticsearch.protocol.xpack.watcher.ActivateWatchResponse;
import org.elasticsearch.protocol.xpack.watcher.DeleteWatchRequest;
import org.elasticsearch.protocol.xpack.watcher.DeleteWatchResponse;
import org.elasticsearch.protocol.xpack.watcher.PutWatchRequest;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.rest.RestStatus;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class WatcherIT extends ESRestHighLevelClientTestCase {

    private static class CFListener<T> implements ActionListener<T> {
        private CompletableFuture<T> future = new CompletableFuture<>();

        @Override
        public void onResponse(T t) {
            future.complete(t);
        }

        @Override
        public void onFailure(Exception e) {
            future.completeExceptionally(e);
        }

        public T get() {
            try {
                return future.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public Throwable getFailure() {
            try {
                future.get();
            } catch (InterruptedException e) {
                throw new RuntimeException("Unexpected interrupt");
            } catch (ExecutionException e) {
                return e.getCause();
            }
            throw new AssertionError("Execution finished without failure");
        }
    }

    public void testPutWatch() throws Exception {
        String watchId = randomAlphaOfLength(10);
        PutWatchResponse putWatchResponse = createWatch(watchId);
        assertThat(putWatchResponse.isCreated(), is(true));
        assertThat(putWatchResponse.getId(), is(watchId));
        assertThat(putWatchResponse.getVersion(), is(1L));

        //cleanup
        deleteWatch(watchId);
    }

    private PutWatchResponse createWatch(String watchId) throws Exception {
        String json = "{ \n" +
            "  \"trigger\": { \"schedule\": { \"interval\": \"10h\" } },\n" +
            "  \"input\": { \"none\": {} },\n" +
            "  \"actions\": { \"logme\": { \"logging\": { \"text\": \"{{ctx.payload}}\" } } }\n" +
            "}";
        BytesReference bytesReference = new BytesArray(json);
        PutWatchRequest putWatchRequest = new PutWatchRequest(watchId, bytesReference, XContentType.JSON);
        return highLevelClient().watcher().putWatch(putWatchRequest, RequestOptions.DEFAULT);
    }

    private DeleteWatchResponse deleteWatch(String watchId) throws Exception {
        return highLevelClient().watcher().deleteWatch(new DeleteWatchRequest(watchId),
                RequestOptions.DEFAULT);
    }


    public void testDeleteWatch() throws Exception {
        // delete watch that exists
        {
            String watchId = randomAlphaOfLength(10);
            createWatch(watchId);
            DeleteWatchResponse deleteWatchResponse = highLevelClient().watcher().deleteWatch(new DeleteWatchRequest(watchId),
                RequestOptions.DEFAULT);
            assertThat(deleteWatchResponse.getId(), is(watchId));
            assertThat(deleteWatchResponse.getVersion(), is(2L));
            assertThat(deleteWatchResponse.isFound(), is(true));
        }

        // delete watch that does not exist
        {
            String watchId = randomAlphaOfLength(10);
            DeleteWatchResponse deleteWatchResponse = highLevelClient().watcher().deleteWatch(new DeleteWatchRequest(watchId),
                RequestOptions.DEFAULT);
            assertThat(deleteWatchResponse.getId(), is(watchId));
            assertThat(deleteWatchResponse.getVersion(), is(1L));
            assertThat(deleteWatchResponse.isFound(), is(false));
        }
    }


    public void testActivateNonExistingWatch() throws Exception {
        //given
        String watchId = randomAlphaOfLength(10);

        //when
        ElasticsearchStatusException e = expectThrows(
                ElasticsearchStatusException.class,
                () -> highLevelClient().watcher().activateWatch(new ActivateWatchRequest(watchId), RequestOptions.DEFAULT)
        );

        //then
        assertThat(e.status(), is(RestStatus.NOT_FOUND));
    }


    public void testActivateNonExistingWatchAsync() throws Exception {
        //given
        String watchId = randomAlphaOfLength(10);
        CFListener<ActivateWatchResponse> listener = new CFListener<>();

        //when
        highLevelClient().watcher().activateWatchAsync(new ActivateWatchRequest(watchId), RequestOptions.DEFAULT, listener);

        //then
        assertThat(listener.getFailure(), instanceOf(ElasticsearchStatusException.class));
        ElasticsearchStatusException e = (ElasticsearchStatusException)listener.getFailure();
        assertThat(e.status(), is(RestStatus.NOT_FOUND));

        //cleanup
        deleteWatch(watchId);
    }

    public void testActivateExistingWatch() throws Exception {
        //given
        String watchId = randomAlphaOfLength(10);
        createWatch(watchId);

        //when
        ActivateWatchResponse response =
                highLevelClient().watcher().activateWatch(new ActivateWatchRequest(watchId), RequestOptions.DEFAULT);

        //then
        assertThat(response.getStatus().state().isActive(), is(true));
        assertThat(response.getStatus().headers().isEmpty(), is(true));
        assertThat(response.getStatus().actions().size(), is(1));

        //cleanup
        deleteWatch(watchId);
    }

    public void testActivateExistingWatchAsync() throws Exception {
        //given
        String watchId = randomAlphaOfLength(10);
        createWatch(watchId);
        CFListener<ActivateWatchResponse> listener = new CFListener<>();

        //when
        highLevelClient().watcher().activateWatchAsync(new ActivateWatchRequest(watchId), RequestOptions.DEFAULT, listener);

        //then
        assertThat(listener.get().getStatus().state().isActive(), is(true));

        //cleanup
        deleteWatch(watchId);
    }
}
