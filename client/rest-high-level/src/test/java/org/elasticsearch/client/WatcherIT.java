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

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.protocol.xpack.watcher.PutWatchRequest;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;

import static org.hamcrest.Matchers.is;

public class WatcherIT extends ESRestHighLevelClientTestCase {

    public void testPutWatch() throws Exception {
        String watchId = randomAlphaOfLength(10);
        String json = "{ \n" +
            "  \"trigger\": { \"schedule\": { \"interval\": \"10h\" } },\n" +
            "  \"input\": { \"none\": {} },\n" +
            "  \"actions\": { \"logme\": { \"logging\": { \"text\": \"{{ctx.payload}}\" } } }\n" +
            "}";
        BytesReference bytesReference = new BytesArray(json);
        PutWatchRequest putWatchRequest = new PutWatchRequest(watchId, bytesReference, XContentType.JSON);
        PutWatchResponse putWatchResponse = highLevelClient().xpack().watcher().putWatch(putWatchRequest, RequestOptions.DEFAULT);
        assertThat(putWatchResponse.isCreated(), is(true));
        assertThat(putWatchResponse.getId(), is(watchId));
        assertThat(putWatchResponse.getVersion(), is(1L));
    }

}
