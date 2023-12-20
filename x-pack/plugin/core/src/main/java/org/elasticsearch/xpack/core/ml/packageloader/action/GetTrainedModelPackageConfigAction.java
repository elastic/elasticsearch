/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.packageloader.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ModelPackageConfig;

import java.io.IOException;
import java.util.Objects;

/**
 * Internal action to retrieve metadata for a packaged model.
 *
 * Note: This is a master node action, because the model could be loaded from a file.
 */
public class GetTrainedModelPackageConfigAction extends ActionType<GetTrainedModelPackageConfigAction.Response> {

    public static final String NAME = "cluster:internal/xpack/ml/trained_models/package_loader/get_config";
    public static final GetTrainedModelPackageConfigAction INSTANCE = new GetTrainedModelPackageConfigAction();

    private GetTrainedModelPackageConfigAction() {
        super(NAME, GetTrainedModelPackageConfigAction.Response::new);
    }

    public static class Request extends MasterNodeRequest<GetTrainedModelPackageConfigAction.Request> {

        private final String packagedModelId;

        public Request(String packagedModelId) {
            this.packagedModelId = packagedModelId;
        }

        public Request(StreamInput in) throws IOException {
            this.packagedModelId = in.readString();
        }

        public String getPackagedModelId() {
            return packagedModelId;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(packagedModelId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(packagedModelId);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            GetTrainedModelPackageConfigAction.Request other = (GetTrainedModelPackageConfigAction.Request) obj;
            return Objects.equals(packagedModelId, other.packagedModelId);
        }
    }

    public static class Response extends ActionResponse {

        private final ModelPackageConfig modelPackageConfig;

        public Response(ModelPackageConfig modelPackageConfig) {
            this.modelPackageConfig = modelPackageConfig;
        }

        public Response(StreamInput in) throws IOException {
            this.modelPackageConfig = new ModelPackageConfig(in);
        }

        public ModelPackageConfig getModelPackageConfig() {
            return modelPackageConfig;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            modelPackageConfig.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GetTrainedModelPackageConfigAction.Response response = (GetTrainedModelPackageConfigAction.Response) o;
            return Objects.equals(modelPackageConfig, response.modelPackageConfig);
        }

        @Override
        public int hashCode() {
            return Objects.hash(modelPackageConfig);
        }
    }

}
