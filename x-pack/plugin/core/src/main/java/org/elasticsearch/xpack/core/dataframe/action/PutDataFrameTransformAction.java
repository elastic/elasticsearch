/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.utils.DataFrameStrings;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.cluster.metadata.MetaDataCreateIndexService.validateIndexOrAliasName;

public class PutDataFrameTransformAction extends ActionType<AcknowledgedResponse> {

    public static final PutDataFrameTransformAction INSTANCE = new PutDataFrameTransformAction();
    public static final String NAME = "cluster:admin/data_frame/put";

    private static final TimeValue MIN_FREQUENCY = TimeValue.timeValueSeconds(1);
    private static final TimeValue MAX_FREQUENCY = TimeValue.timeValueHours(1);

    private PutDataFrameTransformAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private final DataFrameTransformConfig config;
        private final boolean deferValidation;

        public Request(DataFrameTransformConfig config, boolean deferValidation) {
            this.config = config;
            this.deferValidation = deferValidation;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.config = new DataFrameTransformConfig(in);
            if (in.getVersion().onOrAfter(Version.V_7_4_0)) {
                this.deferValidation = in.readBoolean();
            } else {
                this.deferValidation = false;
            }
        }

        public static Request fromXContent(final XContentParser parser, final String id, final boolean deferValidation) {
            return new Request(DataFrameTransformConfig.fromXContent(parser, id, false), deferValidation);
        }

        /**
         * More complex validations with how {@link DataFrameTransformConfig#getDestination()} and
         * {@link DataFrameTransformConfig#getSource()} relate are done in the transport handler.
         */
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
            TimeValue frequency = config.getFrequency();
            if (frequency != null) {
                if (frequency.compareTo(MIN_FREQUENCY) < 0) {
                    validationException = addValidationError(
                        "minimum permitted [" + DataFrameField.FREQUENCY + "] is [" + MIN_FREQUENCY.getStringRep() + "]",
                        validationException);
                } else if (frequency.compareTo(MAX_FREQUENCY) > 0) {
                    validationException = addValidationError(
                        "highest permitted [" + DataFrameField.FREQUENCY + "] is [" + MAX_FREQUENCY.getStringRep() + "]",
                        validationException);
                }
            }

            return validationException;
        }

        public DataFrameTransformConfig getConfig() {
            return config;
        }

        public boolean isDeferValidation() {
            return deferValidation;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            this.config.writeTo(out);
            if (out.getVersion().onOrAfter(Version.V_7_4_0)) {
                out.writeBoolean(this.deferValidation);
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(config, deferValidation);
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
            return Objects.equals(config, other.config) && this.deferValidation == other.deferValidation;
        }
    }

}
