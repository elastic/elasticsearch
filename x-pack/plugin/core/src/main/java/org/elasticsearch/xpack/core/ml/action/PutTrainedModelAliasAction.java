/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xpack.core.ml.job.messages.Messages.INVALID_MODEL_ALIAS;

public class PutTrainedModelAliasAction extends ActionType<AcknowledgedResponse> {

    // NOTE this is similar to our valid ID check. The difference here is that model_aliases cannot end in numbers
    // This is to protect our automatic model naming conventions from hitting weird model_alias conflicts
    private static final Pattern VALID_MODEL_ALIAS_CHAR_PATTERN = Pattern.compile("[a-z0-9](?:[a-z0-9_\\-\\.]*[a-z])?");

    public static final PutTrainedModelAliasAction INSTANCE = new PutTrainedModelAliasAction();
    public static final String NAME = "cluster:admin/xpack/ml/inference/model_aliases/put";

    private PutTrainedModelAliasAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        public static final String MODEL_ALIAS = "model_alias";
        public static final String REASSIGN = "reassign";

        private final String modelAlias;
        private final String modelId;
        private final boolean reassign;

        public Request(String modelAlias, String modelId, boolean reassign) {
            this.modelAlias = ExceptionsHelper.requireNonNull(modelAlias, MODEL_ALIAS);
            this.modelId = ExceptionsHelper.requireNonNull(modelId, TrainedModelConfig.MODEL_ID);
            this.reassign = reassign;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.modelAlias = in.readString();
            this.modelId = in.readString();
            this.reassign = in.readBoolean();
        }

        public String getModelAlias() {
            return modelAlias;
        }

        public String getModelId() {
            return modelId;
        }

        public boolean isReassign() {
            return reassign;
        }

        @Override
        public void writeTo(StreamOutput out) throws  IOException {
            super.writeTo(out);
            out.writeString(modelAlias);
            out.writeString(modelId);
            out.writeBoolean(reassign);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (modelAlias.equals(modelId)) {
                validationException = addValidationError(
                    String.format(
                        Locale.ROOT,
                        "model_alias [%s] cannot equal model_id [%s]",
                        modelAlias,
                        modelId
                    ),
                    validationException
                );
            }
            if (VALID_MODEL_ALIAS_CHAR_PATTERN.matcher(modelAlias).matches() == false) {
                validationException = addValidationError(Messages.getMessage(INVALID_MODEL_ALIAS, modelAlias), validationException);
            }
            return validationException;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(modelAlias, request.modelAlias)
                && Objects.equals(modelId, request.modelId)
                && Objects.equals(reassign, request.reassign);
        }

        @Override
        public int hashCode() {
            return Objects.hash(modelAlias, modelId, reassign);
        }

    }
}
