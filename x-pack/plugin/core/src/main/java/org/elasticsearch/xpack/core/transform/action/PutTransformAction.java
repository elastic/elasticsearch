/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.utils.TransformStrings;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PutTransformAction extends ActionType<AcknowledgedResponse> {

    public static final PutTransformAction INSTANCE = new PutTransformAction();
    public static final String NAME = "cluster:admin/transform/put";

    private static final TimeValue MIN_FREQUENCY = TimeValue.timeValueSeconds(1);
    private static final TimeValue MAX_FREQUENCY = TimeValue.timeValueHours(1);

    private PutTransformAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private final TransformConfig config;
        private final boolean deferValidation;

        public Request(TransformConfig config, boolean deferValidation, TimeValue timeout) {
            super(timeout);
            this.config = config;
            this.deferValidation = deferValidation;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.config = new TransformConfig(in);
            this.deferValidation = in.readBoolean();
        }

        public static Request fromXContent(
            final XContentParser parser,
            final String id,
            final boolean deferValidation,
            final TimeValue timeout
        ) {
            return new Request(TransformConfig.fromXContent(parser, id, false), deferValidation, timeout);
        }

        /**
         * More complex validations with how {@link TransformConfig#getDestination()} and
         * {@link TransformConfig#getSource()} relate are done in the transport handler.
         */
        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            validationException = config.validate(validationException);
            validationException = SourceDestValidator.validateRequest(validationException, config.getDestination().getIndex());

            if (TransformStrings.isValidId(config.getId()) == false) {
                validationException = addValidationError(
                    TransformMessages.getMessage(TransformMessages.INVALID_ID, TransformField.ID.getPreferredName(), config.getId()),
                    validationException
                );
            }
            if (TransformStrings.hasValidLengthForId(config.getId()) == false) {
                validationException = addValidationError(
                    TransformMessages.getMessage(TransformMessages.ID_TOO_LONG, TransformStrings.ID_LENGTH_LIMIT),
                    validationException
                );
            }
            TimeValue frequency = config.getFrequency();
            if (frequency != null) {
                if (frequency.compareTo(MIN_FREQUENCY) < 0) {
                    validationException = addValidationError(
                        "minimum permitted [" + TransformField.FREQUENCY + "] is [" + MIN_FREQUENCY.getStringRep() + "]",
                        validationException
                    );
                } else if (frequency.compareTo(MAX_FREQUENCY) > 0) {
                    validationException = addValidationError(
                        "highest permitted [" + TransformField.FREQUENCY + "] is [" + MAX_FREQUENCY.getStringRep() + "]",
                        validationException
                    );
                }
            }

            return validationException;
        }

        public TransformConfig getConfig() {
            return config;
        }

        public boolean isDeferValidation() {
            return deferValidation;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            this.config.writeTo(out);
            out.writeBoolean(this.deferValidation);
        }

        @Override
        public int hashCode() {
            // the base class does not implement hashCode, therefore we need to hash timeout ourselves
            return Objects.hash(timeout(), config, deferValidation);
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

            // the base class does not implement equals, therefore we need to check timeout ourselves
            return Objects.equals(config, other.config)
                && this.deferValidation == other.deferValidation
                && timeout().equals(other.timeout());
        }
    }

}
