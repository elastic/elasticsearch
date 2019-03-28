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

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.dataframe.DeleteDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.GetDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.GetDataFrameTransformStatsRequest;
import org.elasticsearch.client.dataframe.PreviewDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.PutDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.StartDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.StopDataFrameTransformRequest;
import org.elasticsearch.common.Strings;

import java.io.IOException;

import static org.elasticsearch.client.RequestConverters.REQUEST_BODY_CONTENT_TYPE;
import static org.elasticsearch.client.RequestConverters.createEntity;

final class DataFrameRequestConverters {

    private DataFrameRequestConverters() {}

    static Request putDataFrameTransform(PutDataFrameTransformRequest putRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder()
                .addPathPartAsIs("_data_frame", "transforms")
                .addPathPart(putRequest.getConfig().getId())
                .build();
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        request.setEntity(createEntity(putRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request getDataFrameTransform(GetDataFrameTransformRequest getRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
                .addPathPartAsIs("_data_frame", "transforms")
                .addPathPart(Strings.collectionToCommaDelimitedString(getRequest.getId()))
                .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        if (getRequest.getFrom() != null) {
            request.addParameter("from", getRequest.getFrom().toString());
        }
        if (getRequest.getSize() != null) {
            request.addParameter("size", getRequest.getSize().toString());
        }
        return request;
    }

    static Request deleteDataFrameTransform(DeleteDataFrameTransformRequest request) {
        String endpoint = new RequestConverters.EndpointBuilder()
                .addPathPartAsIs("_data_frame", "transforms")
                .addPathPart(request.getId())
                .build();
        return new Request(HttpDelete.METHOD_NAME, endpoint);
    }

    static Request startDataFrameTransform(StartDataFrameTransformRequest startRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
                .addPathPartAsIs("_data_frame", "transforms")
                .addPathPart(startRequest.getId())
                .addPathPartAsIs("_start")
                .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params(request);
        if (startRequest.getTimeout() != null) {
            params.withTimeout(startRequest.getTimeout());
        }
        return request;
    }

    static Request stopDataFrameTransform(StopDataFrameTransformRequest stopRequest) {
            String endpoint = new RequestConverters.EndpointBuilder()
                    .addPathPartAsIs("_data_frame", "transforms")
                    .addPathPart(stopRequest.getId())
                    .addPathPartAsIs("_stop")
                    .build();
            Request request = new Request(HttpPost.METHOD_NAME, endpoint);
            RequestConverters.Params params = new RequestConverters.Params(request);
            if (stopRequest.getWaitForCompletion() != null) {
                params.withWaitForCompletion(stopRequest.getWaitForCompletion());
            }
            if (stopRequest.getTimeout() != null) {
                params.withTimeout(stopRequest.getTimeout());
            }
            return request;
    }

    static Request previewDataFrameTransform(PreviewDataFrameTransformRequest previewRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder()
                .addPathPartAsIs("_data_frame", "transforms", "_preview")
                .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        request.setEntity(createEntity(previewRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request getDataFrameTransformStats(GetDataFrameTransformStatsRequest statsRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
                .addPathPartAsIs("_data_frame", "transforms")
                .addPathPart(statsRequest.getId())
                .addPathPartAsIs("_stats")
                .build();
        return new Request(HttpGet.METHOD_NAME, endpoint);
    }
}
