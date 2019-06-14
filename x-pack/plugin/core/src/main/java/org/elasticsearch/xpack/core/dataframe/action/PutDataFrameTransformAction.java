/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.utils.DataFrameStrings;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.cluster.metadata.MetaDataCreateIndexService.validateIndexOrAliasName;

public class PutDataFrameTransformAction extends Action<AcknowledgedResponse> {

    public static final PutDataFrameTransformAction INSTANCE = new PutDataFrameTransformAction();
    public static final String NAME = "cluster:admin/data_frame/put";

    private PutDataFrameTransformAction() {
        super(NAME);
    }

    @Override
    public AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {

        private final DataFrameTransformConfig config;

        public Request(DataFrameTransformConfig config) {
            this.config = config;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.config = new DataFrameTransformConfig(in);
        }

        public static Request fromXContent(final XContentParser parser, final String id) throws IOException {
            return new Request(DataFrameTransformConfig.fromXContent(parser, id, false));
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if(config.getPivotConfig() != null
                && config.getPivotConfig().getMaxPageSearchSize() != null
                && (config.getPivotConfig().getMaxPageSearchSize() < 10 || config.getPivotConfig().getMaxPageSearchSize() > 10_000)) {
                validationException = addValidationError(
                    "pivot.max_page_search_size [" +
                        config.getPivotConfig().getMaxPageSearchSize() + "] must be greater than 10 and less than 10,000",
                    validationException);
            }
            for(String failure : config.getPivotConfig().aggFieldValidation()) {
                validationException = addValidationError(failure, validationException);
            }
            String destIndex = config.getDestination().getIndex();
            try {
                validateIndexOrAliasName(destIndex, InvalidIndexNameException::new);
                if (!destIndex.toLowerCase(Locale.ROOT).equals(destIndex)) {
                    validationException = addValidationError("dest.index [" + destIndex +"] must be lowercase", validationException);
                }
            } catch (InvalidIndexNameException ex) {
                validationException = addValidationError(ex.getMessage(), validationException);
            }
            if (DataFrameStrings.isValidId(config.getId()) == false) {
                validationException = addValidationError(
                    DataFrameMessages.getMessage(DataFrameMessages.INVALID_ID, DataFrameField.ID.getPreferredName(), config.getId()),
                    validationException);
            }
            if (DataFrameStrings.hasValidLengthForId(config.getId()) == false) {
                validationException = addValidationError(
                    DataFrameMessages.getMessage(DataFrameMessages.ID_TOO_LONG, DataFrameStrings.ID_LENGTH_LIMIT),
                    validationException);
            }
            return validationException;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return this.config.toXContent(builder, params);
        }

        public DataFrameTransformConfig getConfig() {
            return config;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            this.config.writeTo(out);
        }

        @Override
        public int hashCode() {
            return Objects.hash(config);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(config, other.config);
        }
    }

}
