/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.rest.action;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction;
import org.elasticsearch.xpack.core.transform.action.PreviewTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestPreviewTransformAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, TransformField.REST_BASE_PATH_TRANSFORMS + "_preview"),
            new Route(POST, TransformField.REST_BASE_PATH_TRANSFORMS_BY_ID + "_preview"));
    }

    @Override
    public String getName() {
        return "transform_preview_transform_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String transformId = restRequest.param(TransformField.ID.getPreferredName());

        if (Strings.isNullOrEmpty(transformId) && restRequest.hasContentOrSourceParam() == false) {
            throw ExceptionsHelper.badRequestException(
                "Please provide a transform [{}] or the config object",
                TransformField.ID.getPreferredName());
        }

        if (Strings.isNullOrEmpty(transformId) == false && restRequest.hasContentOrSourceParam()) {
            throw ExceptionsHelper.badRequestException(
                "Please provide either a transform [{}] or the config object but not both",
                TransformField.ID.getPreferredName()
            );
        }

        SetOnce<PreviewTransformAction.Request> previewRequestHolder = new SetOnce<>();
        if (Strings.isNullOrEmpty(transformId)) {
            previewRequestHolder.set(PreviewTransformAction.Request.fromXContent(restRequest.contentOrSourceParamParser()));
        }

        return channel -> {
            RestToXContentListener<PreviewTransformAction.Response> listener = new RestToXContentListener<>(channel);

            if (Strings.isNullOrEmpty(transformId)) {
                PreviewTransformAction.Request previewRequest = previewRequestHolder.get();
                client.execute(PreviewTransformAction.INSTANCE, previewRequest, listener);
            } else {
                GetTransformAction.Request getRequest = new GetTransformAction.Request(transformId);
                getRequest.setAllowNoResources(false);
                client.execute(GetTransformAction.INSTANCE, getRequest, ActionListener.wrap(getResponse -> {
                    List<TransformConfig> transforms = getResponse.getResources().results();
                    if (transforms.size() > 1) {
                        listener.onFailure(
                            ExceptionsHelper.badRequestException(
                                "expected only one config but matched {}",
                                transforms.stream().map(TransformConfig::getId).collect(Collectors.toList())
                            )
                        );
                    } else {
                        PreviewTransformAction.Request previewRequest = new PreviewTransformAction.Request(transforms.get(0));
                        client.execute(PreviewTransformAction.INSTANCE, previewRequest, listener);
                    }
                }, listener::onFailure));
            }
        };
    }
}
