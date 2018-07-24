/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

//public class FeatureIndexBuildAction extends Action<FeatureIndexBuildAction.Request, FeatureIndexBuildAction.Response> {
    public class FeatureIndexBuildAction extends Action<FeatureIndexBuildAction.Response> {
    
    protected FeatureIndexBuildAction(String name) {
        super(name);
    }

    public static final FeatureIndexBuildAction INSTANCE = new FeatureIndexBuildAction("");
    
    public static class Request extends ActionRequest implements ToXContentObject {
        public Request() {
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return null;
        }

    }

    public static class Response extends ActionResponse implements ToXContentObject {
        public Response() {
            super();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return null;
        }

    }

    @Override
    public Response newResponse() {
        return null;
    }
}
