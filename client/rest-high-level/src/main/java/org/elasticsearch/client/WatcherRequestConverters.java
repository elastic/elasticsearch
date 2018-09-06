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

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.elasticsearch.client.watcher.AckWatchRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.protocol.xpack.watcher.DeleteWatchRequest;
import org.elasticsearch.protocol.xpack.watcher.PutWatchRequest;

public class WatcherRequestConverters {

    static Request putWatch(PutWatchRequest putWatchRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_xpack")
            .addPathPartAsIs("watcher")
            .addPathPartAsIs("watch")
            .addPathPart(putWatchRequest.getId())
            .build();

        Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params(request).withVersion(putWatchRequest.getVersion());
        if (putWatchRequest.isActive() == false) {
            params.putParam("active", "false");
        }
        ContentType contentType = RequestConverters.createContentType(putWatchRequest.xContentType());
        BytesReference source = putWatchRequest.getSource();
        request.setEntity(new ByteArrayEntity(source.toBytesRef().bytes, 0, source.length(), contentType));
        return request;
    }

    static Request deleteWatch(DeleteWatchRequest deleteWatchRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_xpack")
            .addPathPartAsIs("watcher")
            .addPathPartAsIs("watch")
            .addPathPart(deleteWatchRequest.getId())
            .build();

        Request request = new Request(HttpDelete.METHOD_NAME, endpoint);
        return request;
    }

    public static Request ackWatch(AckWatchRequest ackWatchRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_xpack")
            .addPathPartAsIs("watcher")
            .addPathPartAsIs("watch")
            .addPathPart(ackWatchRequest.getWatchId())
            .addPathPartAsIs("_ack")
            .addCommaSeparatedPathParts(ackWatchRequest.getActionIds())
            .build();
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        return request;
    }
}
