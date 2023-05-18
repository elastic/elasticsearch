/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesRequest;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesResponse;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.core.Strings.format;

public class GetTransformAction extends ActionType<GetTransformAction.Response> {

    public static final GetTransformAction INSTANCE = new GetTransformAction();
    public static final String NAME = "cluster:monitor/transform/get";

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(GetTransformAction.class);

    private GetTransformAction() {
        super(NAME, GetTransformAction.Response::new);
    }

    public static class Request extends AbstractGetResourcesRequest {

        private static final int MAX_SIZE_RETURN = 1000;

        public Request(String id) {
            super(id, PageParams.defaultParams(), true);
        }

        public Request() {
            super(null, PageParams.defaultParams(), true);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        public String getId() {
            return getResourceId();
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException exception = null;
            if (getPageParams() != null && getPageParams().getSize() > MAX_SIZE_RETURN) {
                exception = addValidationError(
                    "Param [" + PageParams.SIZE.getPreferredName() + "] has a max acceptable value of [" + MAX_SIZE_RETURN + "]",
                    exception
                );
            }
            return exception;
        }

        @Override
        public String getCancelableTaskDescription() {
            return format("get_transforms[%s]", getResourceId());
        }

        @Override
        public String getResourceIdField() {
            return TransformField.ID.getPreferredName();
        }
    }

    public static class Response extends AbstractGetResourcesResponse<TransformConfig> implements ToXContentObject {

        public static class Error implements Writeable, ToXContentObject {
            private static final ParseField TYPE = new ParseField("type");
            private static final ParseField REASON = new ParseField("reason");

            private final String type;
            private final String reason;

            public Error(String type, String reason) {
                this.type = Objects.requireNonNull(type);
                this.reason = Objects.requireNonNull(reason);
            }

            public Error(StreamInput in) throws IOException {
                this.type = in.readString();
                this.reason = in.readString();
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field(TYPE.getPreferredName(), type);
                builder.field(REASON.getPreferredName(), reason);
                builder.endObject();
                return builder;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(type);
                out.writeString(reason);
            }
        }

        public static final String INVALID_TRANSFORMS_DEPRECATION_WARNING = "Found [{}] invalid transforms";
        private static final ParseField INVALID_TRANSFORMS = new ParseField("invalid_transforms");
        private static final ParseField ERRORS = new ParseField("errors");

        private final List<Error> errors;

        public Response(List<TransformConfig> transformConfigs, long count, List<Error> errors) {
            super(new QueryPage<>(transformConfigs, count, TransformField.TRANSFORMS));
            this.errors = errors;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_1_0)) {
                if (in.readBoolean()) {
                    this.errors = in.readList(Error::new);
                } else {
                    this.errors = null;
                }
            } else {
                this.errors = null;
            }
        }

        public List<TransformConfig> getTransformConfigurations() {
            return getResources().results();
        }

        public long getTransformConfigurationCount() {
            return getResources().count();
        }

        public List<Error> getErrors() {
            return errors;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            List<String> invalidTransforms = new ArrayList<>();
            builder.startObject();
            builder.field(TransformField.COUNT.getPreferredName(), getResources().count());
            // XContentBuilder does not support passing the params object for Iterables
            builder.field(TransformField.TRANSFORMS.getPreferredName());
            builder.startArray();
            for (TransformConfig configResponse : getResources().results()) {
                configResponse.toXContent(builder, params);
                ValidationException validationException = configResponse.validate(null);
                if (validationException != null) {
                    invalidTransforms.add(configResponse.getId());
                }
            }
            builder.endArray();
            if (invalidTransforms.isEmpty() == false) {
                builder.startObject(INVALID_TRANSFORMS.getPreferredName());
                builder.field(TransformField.COUNT.getPreferredName(), invalidTransforms.size());
                builder.field(TransformField.TRANSFORMS.getPreferredName(), invalidTransforms);
                builder.endObject();
                deprecationLogger.warn(
                    DeprecationCategory.OTHER,
                    "invalid_transforms",
                    INVALID_TRANSFORMS_DEPRECATION_WARNING,
                    invalidTransforms.size()
                );
            }
            if (errors != null) {
                builder.field(ERRORS.getPreferredName(), errors);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_1_0)) {
                if (errors != null) {
                    out.writeBoolean(true);
                    out.writeList(errors);
                } else {
                    out.writeBoolean(false);
                }
            }
        }

        @Override
        protected Reader<TransformConfig> getReader() {
            return TransformConfig::new;
        }
    }
}
