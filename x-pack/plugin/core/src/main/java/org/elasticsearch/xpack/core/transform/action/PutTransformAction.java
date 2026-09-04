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
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator;
import org.elasticsearch.xpack.core.security.cloud.CloudCredential;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformParsingContext;
import org.elasticsearch.xpack.core.transform.utils.TransformStrings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xpack.core.transform.transforms.TransformConfig.TRANSFORM_CLOUD_CREDENTIAL_ON_REQUEST;

public class PutTransformAction extends ActionType<AcknowledgedResponse> {

    public static final PutTransformAction INSTANCE = new PutTransformAction();
    public static final String NAME = "cluster:admin/transform/put";

    /**
     * Minimum transform frequency used for validation.
     *
     * Note: Depending on the environment (on-prem or serverless) the minimum frequency used by scheduler can be higher than this constant.
     * The actual value used by scheduler is specified by the {@code TransformExtension.getMinFrequency} method.
     *
     * Example:
     * If the user configures transform with frequency=3s but the TransformExtension.getMinFrequency method returns 5s, the validation will
     * pass but the scheduler will silently use 5s instead of 3s.
     */
    private static final TimeValue MIN_FREQUENCY = TimeValue.timeValueSeconds(1);
    /**
     * Maximum transform frequency used for validation.
     */
    private static final TimeValue MAX_FREQUENCY = TimeValue.timeValueHours(1);

    private PutTransformAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements Releasable {

        private final TransformConfig config;
        private final boolean deferValidation;

        // Caller's UIAM cloud credential carried on the request so it survives coordinator -> master
        // transport, where the AUTHENTICATING_CLOUD_TOKEN_THREAD_CONTEXT transient is no longer present.
        @Nullable
        private CloudCredential cloudCredential;

        public Request(TransformConfig config, boolean deferValidation, TimeValue timeout) {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, timeout);
            this.config = config;
            this.deferValidation = deferValidation;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.config = new TransformConfig(in);
            this.deferValidation = in.readBoolean();
            if (in.getTransportVersion().supports(TRANSFORM_CLOUD_CREDENTIAL_ON_REQUEST)) {
                this.cloudCredential = in.readOptionalWriteable(CloudCredential::new);
            } else {
                this.cloudCredential = null;
            }
        }

        public static Request fromXContent(
            final XContentParser parser,
            final String id,
            final boolean deferValidation,
            final TimeValue timeout,
            TransformParsingContext transformParsingContext
        ) {
            return new Request(TransformConfig.fromXContent(parser, id, false, transformParsingContext), deferValidation, timeout);
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

        @Nullable
        public CloudCredential getCloudCredential() {
            return cloudCredential;
        }

        /**
         * Sets the credential this request carries and hands ownership of the previously-held
         * credential back to the caller, which is responsible for closing it. Returns {@code null}
         * when the credential is unchanged.
         */
        @Nullable
        public CloudCredential setCloudCredential(@Nullable CloudCredential cloudCredential) {
            var previous = this.cloudCredential == cloudCredential ? null : this.cloudCredential;
            this.cloudCredential = cloudCredential;
            return previous;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            this.config.writeTo(out);
            out.writeBoolean(this.deferValidation);
            if (out.getTransportVersion().supports(TRANSFORM_CLOUD_CREDENTIAL_ON_REQUEST)) {
                out.writeOptionalWriteable(this.cloudCredential);
            }
        }

        @Override
        public void close() {
            IOUtils.closeWhileHandlingException(cloudCredential);
        }

        @Override
        public int hashCode() {
            // the base class does not implement hashCode, therefore we need to hash timeout ourselves
            // cloudCredential is intentionally excluded: request-scoped secret carrier, not logical identity.
            return Objects.hash(ackTimeout(), config, deferValidation);
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
            // cloudCredential is intentionally excluded: request-scoped secret carrier, not logical identity.
            return Objects.equals(config, other.config)
                && this.deferValidation == other.deferValidation
                && ackTimeout().equals(other.ackTimeout());
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }
    }
}
