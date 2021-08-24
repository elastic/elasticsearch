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
import org.elasticsearch.client.core.PageParams;
import org.elasticsearch.client.transform.DeleteTransformRequest;
import org.elasticsearch.client.transform.GetTransformRequest;
import org.elasticsearch.client.transform.GetTransformStatsRequest;
import org.elasticsearch.client.transform.PreviewTransformRequest;
import org.elasticsearch.client.transform.PutTransformRequest;
import org.elasticsearch.client.transform.StartTransformRequest;
import org.elasticsearch.client.transform.StopTransformRequest;
import org.elasticsearch.client.transform.UpdateTransformRequest;
import org.elasticsearch.common.Strings;

import java.io.IOException;

import static org.elasticsearch.client.RequestConverters.REQUEST_BODY_CONTENT_TYPE;
import static org.elasticsearch.client.RequestConverters.createEntity;
import static org.elasticsearch.client.transform.DeleteTransformRequest.FORCE;
import static org.elasticsearch.client.transform.GetTransformRequest.ALLOW_NO_MATCH;
import static org.elasticsearch.client.transform.GetTransformRequest.EXCLUDE_GENERATED;
import static org.elasticsearch.client.transform.PutTransformRequest.DEFER_VALIDATION;
import static org.elasticsearch.client.transform.StopTransformRequest.WAIT_FOR_CHECKPOINT;

final class TransformRequestConverters {

    private TransformRequestConverters() {}

    static Request putTransform(PutTransformRequest putRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder()
                .addPathPartAsIs("_transform")
                .addPathPart(putRequest.getConfig().getId())
                .build();
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        request.setEntity(createEntity(putRequest, REQUEST_BODY_CONTENT_TYPE));
        if (putRequest.getDeferValidation() != null) {
            request.addParameter(DEFER_VALIDATION, Boolean.toString(putRequest.getDeferValidation()));
        }
        return request;
    }

    static Request updateTransform(UpdateTransformRequest updateDataFrameTransformRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_transform")
            .addPathPart(updateDataFrameTransformRequest.getId())
            .addPathPart("_update")
            .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        request.setEntity(createEntity(updateDataFrameTransformRequest, REQUEST_BODY_CONTENT_TYPE));
        if (updateDataFrameTransformRequest.getDeferValidation() != null) {
            request.addParameter(DEFER_VALIDATION, Boolean.toString(updateDataFrameTransformRequest.getDeferValidation()));
        }
        return request;
    }

    static Request getTransform(GetTransformRequest getRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
                .addPathPartAsIs("_transform")
                .addPathPart(Strings.collectionToCommaDelimitedString(getRequest.getId()))
                .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        if (getRequest.getPageParams() != null && getRequest.getPageParams().getFrom() != null) {
            request.addParameter(PageParams.FROM.getPreferredName(), getRequest.getPageParams().getFrom().toString());
        }
        if (getRequest.getPageParams() != null && getRequest.getPageParams().getSize() != null) {
            request.addParameter(PageParams.SIZE.getPreferredName(), getRequest.getPageParams().getSize().toString());
        }
        if (getRequest.getAllowNoMatch() != null) {
            request.addParameter(ALLOW_NO_MATCH, getRequest.getAllowNoMatch().toString());
        }
        if (getRequest.getExcludeGenerated() != null) {
            request.addParameter(EXCLUDE_GENERATED, getRequest.getExcludeGenerated().toString());
        }
        return request;
    }

    static Request deleteTransform(DeleteTransformRequest deleteRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
                .addPathPartAsIs("_transform")
                .addPathPart(deleteRequest.getId())
                .build();
        Request request = new Request(HttpDelete.METHOD_NAME, endpoint);
        if (deleteRequest.getForce() != null) {
            request.addParameter(FORCE, Boolean.toString(deleteRequest.getForce()));
        }
        return request;
    }

    static Request startTransform(StartTransformRequest startRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
                .addPathPartAsIs("_transform")
                .addPathPart(startRequest.getId())
                .addPathPartAsIs("_start")
                .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        if (startRequest.getTimeout() != null) {
            params.withTimeout(startRequest.getTimeout());
        }
        request.addParameters(params.asMap());
        return request;
    }

    static Request stopTransform(StopTransformRequest stopRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_transform")
            .addPathPart(stopRequest.getId())
            .addPathPartAsIs("_stop")
            .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        if (stopRequest.getWaitForCompletion() != null) {
            params.withWaitForCompletion(stopRequest.getWaitForCompletion());
        }
        if (stopRequest.getTimeout() != null) {
            params.withTimeout(stopRequest.getTimeout());
        }
        if (stopRequest.getAllowNoMatch() != null) {
            request.addParameter(ALLOW_NO_MATCH, stopRequest.getAllowNoMatch().toString());
        }
        if (stopRequest.getWaitForCheckpoint() != null) {
            request.addParameter(WAIT_FOR_CHECKPOINT, stopRequest.getWaitForCheckpoint().toString());
        }
        request.addParameters(params.asMap());
        return request;
    }

    static Request previewTransform(PreviewTransformRequest previewRequest) throws IOException {
        RequestConverters.EndpointBuilder endpointBuilder = new RequestConverters.EndpointBuilder().addPathPartAsIs("_transform");
        if (previewRequest.getTransformId() != null) {
            endpointBuilder.addPathPartAsIs(previewRequest.getTransformId());
        }
        endpointBuilder.addPathPartAsIs("_preview");
        String endpoint = endpointBuilder.build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        if (previewRequest.getTransformId() == null) {
            request.setEntity(createEntity(previewRequest, REQUEST_BODY_CONTENT_TYPE));
        }
        return request;
    }

    static Request getTransformStats(GetTransformStatsRequest statsRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
                .addPathPartAsIs("_transform")
                .addPathPart(statsRequest.getId())
                .addPathPartAsIs("_stats")
                .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        if (statsRequest.getPageParams() != null && statsRequest.getPageParams().getFrom() != null) {
            request.addParameter(PageParams.FROM.getPreferredName(), statsRequest.getPageParams().getFrom().toString());
        }
        if (statsRequest.getPageParams() != null && statsRequest.getPageParams().getSize() != null) {
            request.addParameter(PageParams.SIZE.getPreferredName(), statsRequest.getPageParams().getSize().toString());
        }
        if (statsRequest.getAllowNoMatch() != null) {
            request.addParameter(ALLOW_NO_MATCH, statsRequest.getAllowNoMatch().toString());
        }
        return request;
    }
}
