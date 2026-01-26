/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.checkpoint;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;

public class GetTransformCrossProjectHeadersAction extends ActionType<GetTransformCrossProjectHeadersAction.Response> {

    public static final String NAME = "indices:data/cross_project/headers/get";
    public static final GetTransformCrossProjectHeadersAction INSTANCE = new GetTransformCrossProjectHeadersAction(NAME);

    public GetTransformCrossProjectHeadersAction(String name) {
        super(name);
    }

    public static class Request extends ActionRequest implements IndicesRequest.Replaceable {
        private final TransformConfig transformConfig;
        private final IndicesOptions indicesOptions = IndicesOptions.builder(IndicesOptions.lenientExpandOpen())
            .crossProjectModeOptions(new IndicesOptions.CrossProjectModeOptions(true))
            .build();
        private ResolvedIndexExpressions resolvedIndexExpressions = null;

        public Request(TransformConfig transformConfig) {
            this.transformConfig = transformConfig;
        }

        public TransformConfig getTransformConfig() {
            return transformConfig;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) {
            TransportAction.localOnly();
        }

        @Override
        public IndicesRequest indices(String... indices) {
            return null;
        }

        @Override
        public String[] indices() {
            return new String[0];
        }

        @Override
        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }

        @Override
        public boolean allowsRemoteIndices() {
            return true;
        }

        @Override
        public boolean allowsCrossProject() {
            return true;
        }

        @Override
        public void setResolvedIndexExpressions(ResolvedIndexExpressions expressions) {
            this.resolvedIndexExpressions = expressions;
        }

        @Override
        public ResolvedIndexExpressions getResolvedIndexExpressions() {
            return resolvedIndexExpressions;
        }

        @Override
        public boolean includeDataStreams() {
            return true;
        }
    }

    public static class Response extends ActionResponse {
        public static final Response INSTANCE = new Response();

        private Response() {}

        @Override
        public void writeTo(StreamOutput out) {
            TransportAction.localOnly();
        }
    }
}
