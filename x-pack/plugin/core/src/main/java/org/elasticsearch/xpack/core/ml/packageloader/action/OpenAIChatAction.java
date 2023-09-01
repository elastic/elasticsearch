/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.packageloader.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * Internal action to load a packaged model into an index, this can be a download or loading from a file.
 *
 * Note: This is a master node action, because the model could be loaded from a file.
 */
public class OpenAIChatAction extends ActionType<AcknowledgedResponse> {

    public static final String NAME = "cluster:internal/xpack/ml/trained_models/package_loader/chat";
    public static final OpenAIChatAction INSTANCE = new OpenAIChatAction();

    private OpenAIChatAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends MasterNodeRequest<OpenAIChatAction.Request> {

        private final String modelId;

        public Request(String modelId) {
            this.modelId = modelId;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.modelId = in.readString();
        }

        public String getModelId() {
            return modelId;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(modelId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(modelId);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            OpenAIChatAction.Request other = (OpenAIChatAction.Request) obj;
            return Objects.equals(modelId, other.modelId);
        }
    }
}
