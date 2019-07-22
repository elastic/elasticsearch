/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.action;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesRequest;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesResponse;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class GetDataFrameTransformsAction extends ActionType<GetDataFrameTransformsAction.Response> {

    public static final GetDataFrameTransformsAction INSTANCE = new GetDataFrameTransformsAction();
    public static final String NAME = "cluster:monitor/data_frame/get";

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(
            LogManager.getLogger(GetDataFrameTransformsAction.class));

    private GetDataFrameTransformsAction() {
        super(NAME, GetDataFrameTransformsAction.Response::new);
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
                exception = addValidationError("Param [" + PageParams.SIZE.getPreferredName() +
                    "] has a max acceptable value of [" + MAX_SIZE_RETURN + "]", exception);
            }
            return exception;
        }

        @Override
        public String getResourceIdField() {
            return DataFrameField.ID.getPreferredName();
        }
    }

    public static class Response extends AbstractGetResourcesResponse<DataFrameTransformConfig> implements Writeable, ToXContentObject {

        public static final String INVALID_TRANSFORMS_DEPRECATION_WARNING = "Found [{}] invalid transforms";
        private static final ParseField INVALID_TRANSFORMS = new ParseField("invalid_transforms");

        public Response(List<DataFrameTransformConfig> transformConfigs, long count) {
            super(new QueryPage<>(transformConfigs, count, DataFrameField.TRANSFORMS));
        }

        public Response() {
            super();
        }

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public List<DataFrameTransformConfig> getTransformConfigurations() {
            return getResources().results();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            List<String> invalidTransforms = new ArrayList<>();
            builder.startObject();
            builder.field(DataFrameField.COUNT.getPreferredName(), getResources().count());
            // XContentBuilder does not support passing the params object for Iterables
            builder.field(DataFrameField.TRANSFORMS.getPreferredName());
            builder.startArray();
            for (DataFrameTransformConfig configResponse : getResources().results()) {
                configResponse.toXContent(builder, params);
                if (configResponse.isValid() == false) {
                    invalidTransforms.add(configResponse.getId());
                }
            }
            builder.endArray();
            if (invalidTransforms.isEmpty() == false) {
                builder.startObject(INVALID_TRANSFORMS.getPreferredName());
                builder.field(DataFrameField.COUNT.getPreferredName(), invalidTransforms.size());
                builder.field(DataFrameField.TRANSFORMS.getPreferredName(), invalidTransforms);
                builder.endObject();
                deprecationLogger.deprecated(INVALID_TRANSFORMS_DEPRECATION_WARNING, invalidTransforms.size());
            }

            builder.endObject();
            return builder;
        }

        @Override
        protected Reader<DataFrameTransformConfig> getReader() {
            return DataFrameTransformConfig::new;
        }
    }
}
