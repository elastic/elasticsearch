/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NByteArrayEntity;
import org.elasticsearch.client.watcher.AckWatchRequest;
import org.elasticsearch.client.watcher.ActivateWatchRequest;
import org.elasticsearch.client.watcher.DeactivateWatchRequest;
import org.elasticsearch.client.watcher.DeleteWatchRequest;
import org.elasticsearch.client.watcher.ExecuteWatchRequest;
import org.elasticsearch.client.watcher.GetWatchRequest;
import org.elasticsearch.client.watcher.PutWatchRequest;
import org.elasticsearch.client.watcher.StartWatchServiceRequest;
import org.elasticsearch.client.watcher.StopWatchServiceRequest;
import org.elasticsearch.client.watcher.WatcherStatsRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

final class WatcherRequestConverters {

    private WatcherRequestConverters() {}

    static Request startWatchService(StartWatchServiceRequest startWatchServiceRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
                .addPathPartAsIs("_watcher")
                .addPathPartAsIs("_start")
                .build();

        return new Request(HttpPost.METHOD_NAME, endpoint);
    }

    static Request stopWatchService(StopWatchServiceRequest stopWatchServiceRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
                .addPathPartAsIs("_watcher")
                .addPathPartAsIs("_stop")
                .build();

        return new Request(HttpPost.METHOD_NAME, endpoint);
    }

    static Request putWatch(PutWatchRequest putWatchRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_watcher", "watch")
            .addPathPart(putWatchRequest.getId())
            .build();

        Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params()
            .withIfSeqNo(putWatchRequest.ifSeqNo())
            .withIfPrimaryTerm(putWatchRequest.ifPrimaryTerm());
        if (putWatchRequest.isActive() == false) {
            params.putParam("active", "false");
        }
        request.addParameters(params.asMap());
        ContentType contentType = RequestConverters.createContentType(putWatchRequest.xContentType());
        BytesReference source = putWatchRequest.getSource();
        request.setEntity(new NByteArrayEntity(source.toBytesRef().bytes, 0, source.length(), contentType));
        return request;
    }


    static Request getWatch(GetWatchRequest getWatchRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_watcher", "watch")
            .addPathPart(getWatchRequest.getId())
            .build();

        return new Request(HttpGet.METHOD_NAME, endpoint);
    }

    static Request deactivateWatch(DeactivateWatchRequest deactivateWatchRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_watcher")
            .addPathPartAsIs("watch")
            .addPathPart(deactivateWatchRequest.getWatchId())
            .addPathPartAsIs("_deactivate")
            .build();
        return new Request(HttpPut.METHOD_NAME, endpoint);
    }

    static Request deleteWatch(DeleteWatchRequest deleteWatchRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_watcher", "watch")
            .addPathPart(deleteWatchRequest.getId())
            .build();

        Request request = new Request(HttpDelete.METHOD_NAME, endpoint);
        return request;
    }

    static Request executeWatch(ExecuteWatchRequest executeWatchRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_watcher", "watch")
            .addPathPart(executeWatchRequest.getId())       // will ignore if ID is null
            .addPathPartAsIs("_execute").build();

        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        if (executeWatchRequest.isDebug()) {
            params.putParam("debug", "true");
        }
        if (executeWatchRequest.ignoreCondition()) {
            params.putParam("ignore_condition", "true");
        }
        if (executeWatchRequest.recordExecution()) {
            params.putParam("record_execution", "true");
        }
        request.addParameters(params.asMap());
        request.setEntity(RequestConverters.createEntity(executeWatchRequest, XContentType.JSON));
        return request;
    }

    public static Request ackWatch(AckWatchRequest ackWatchRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_watcher", "watch")
            .addPathPart(ackWatchRequest.getWatchId())
            .addPathPartAsIs("_ack")
            .addCommaSeparatedPathParts(ackWatchRequest.getActionIds())
            .build();
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        return request;
    }

    static Request activateWatch(ActivateWatchRequest activateWatchRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_watcher", "watch")
            .addPathPart(activateWatchRequest.getWatchId())
            .addPathPartAsIs("_activate")
            .build();
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        return request;
    }

    static Request watcherStats(WatcherStatsRequest watcherStatsRequest) {
        RequestConverters.EndpointBuilder builder = new RequestConverters.EndpointBuilder().addPathPartAsIs("_watcher", "stats");
        String endpoint = builder.build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        RequestConverters.Params parameters = new RequestConverters.Params();
        StringBuilder metric = new StringBuilder();
        if (watcherStatsRequest.includeCurrentWatches()) {
            metric.append("current_watches");
        }
        if (watcherStatsRequest.includeQueuedWatches()) {
            if (metric.length() > 0) {
                metric.append(",");
            }
            metric.append("queued_watches");
        }
        if (metric.length() > 0) {
            parameters.putParam("metric", metric.toString());
        }
        request.addParameters(parameters.asMap());
        return request;
    }
}
