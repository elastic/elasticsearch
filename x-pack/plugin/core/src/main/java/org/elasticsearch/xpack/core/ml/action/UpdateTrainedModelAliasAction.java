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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xpack.core.ml.job.messages.Messages.INVALID_MODEL_ALIAS;

public class UpdateTrainedModelAliasAction extends ActionType<AcknowledgedResponse> {

    // NOTE this is similar to our valid ID check. The difference here is that model_aliases cannot end in numbers
    // This is to protect our automatic model naming conventions from hitting weird model_alias conflicts
    private static final Pattern VALID_MODEL_ALIAS_CHAR_PATTERN = Pattern.compile("[a-z0-9](?:[a-z0-9_\\-\\.]*[a-z])?");

    public static final UpdateTrainedModelAliasAction INSTANCE = new UpdateTrainedModelAliasAction();
    public static final String NAME = "cluster:admin/ml/update_model_alias";

    private UpdateTrainedModelAliasAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {

        public static final ParseField MODEL_ALIAS = new ParseField("model_alias");
        public static final ParseField NEW_MODEL_ID = new ParseField("new_model_id");
        public static final ParseField OLD_MODEL_ID = new ParseField("old_model_id");
        private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>(
            "update_trained_model_alias_request",
            Request.Builder::new
        );
        static {
            PARSER.declareString(Request.Builder::setNewModelId, NEW_MODEL_ID);
            PARSER.declareString(Request.Builder::setOldModelId, OLD_MODEL_ID);
            PARSER.declareString(Request.Builder::setModelAlias, MODEL_ALIAS);
        }

        public static Request fromXContent(String modelAlias, XContentParser parser) {
            Request.Builder builder = PARSER.apply(parser, null);
            if (builder.getModelAlias() != null && (builder.getModelAlias().equals(modelAlias) == false)) {
                throw new IllegalArgumentException("both model_alias provided in the body and in the URL must be equal");
            }
            return builder.setModelAlias(modelAlias).build();
        }

        private final String modelAlias;
        private final String newModelId;
        private final String oldModelId;

        public Request(String modelAlias, String newModelId, String oldModelId) {
            this.modelAlias = ExceptionsHelper.requireNonNull(modelAlias, MODEL_ALIAS);
            this.newModelId = ExceptionsHelper.requireNonNull(newModelId, NEW_MODEL_ID);
            this.oldModelId = oldModelId;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.modelAlias = in.readString();
            this.newModelId = in.readString();
            this.oldModelId = in.readOptionalString();
        }

        public String getModelAlias() {
            return modelAlias;
        }

        public String getNewModelId() {
            return newModelId;
        }

        public String getOldModelId() {
            return oldModelId;
        }

        @Override
        public void writeTo(StreamOutput out) throws  IOException {
            super.writeTo(out);
            out.writeString(modelAlias);
            out.writeString(newModelId);
            out.writeOptionalString(oldModelId);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (newModelId.equals(oldModelId)) {
                validationException = addValidationError(
                    String.format(Locale.ROOT,"old_model_id [%s] cannot equal new_model_id [%s]", oldModelId, newModelId),
                    validationException
                );
            }
            if (modelAlias.equals(newModelId) || modelAlias.equals(oldModelId)) {
                validationException = addValidationError(
                    String.format(
                        Locale.ROOT,
                        "model_alias [%s] cannot equal new_model_id [%s] nor old_model_id [%s]",
                        modelAlias,
                        newModelId,
                        oldModelId
                    ),
                    validationException
                );
            }
            if (VALID_MODEL_ALIAS_CHAR_PATTERN.matcher(modelAlias).matches() == false) {
                validationException = addValidationError(
                    Messages.getMessage(INVALID_MODEL_ALIAS, MODEL_ALIAS.getPreferredName(), modelAlias),
                    validationException
                );
            }
            return validationException;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(modelAlias, request.modelAlias)
                && Objects.equals(newModelId, request.newModelId)
                && Objects.equals(oldModelId, request.oldModelId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(modelAlias, oldModelId, newModelId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MODEL_ALIAS.getPreferredName(), modelAlias);
            builder.field(NEW_MODEL_ID.getPreferredName(), newModelId);
            if (oldModelId != null) {
                builder.field(OLD_MODEL_ID.getPreferredName(), oldModelId);
            }
            builder.endObject();
            return builder;
        }

        static class Builder {
            private String oldModelId;
            private String newModelId;
            private String modelAlias;

            public Builder setOldModelId(String oldModelId) {
                this.oldModelId = oldModelId;
                return this;
            }

            public Builder setNewModelId(String newModelId) {
                this.newModelId = newModelId;
                return this;
            }

            public Builder setModelAlias(String modelAlias) {
                this.modelAlias = modelAlias;
                return this;
            }

            public String getModelAlias() {
                return modelAlias;
            }

            public Request build() {
                return new Request(modelAlias, newModelId, oldModelId);
            }
        }
    }
}
