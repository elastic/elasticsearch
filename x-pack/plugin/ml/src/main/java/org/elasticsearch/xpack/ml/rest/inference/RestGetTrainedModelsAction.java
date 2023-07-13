/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.inference;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction.Includes.DEFINITION;
import static org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction.Request.ALLOW_NO_MATCH;
import static org.elasticsearch.xpack.core.ml.utils.ToXContentParams.EXCLUDE_GENERATED;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

@ServerlessScope(Scope.PUBLIC)
public class RestGetTrainedModelsAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestGetTrainedModelsAction.class);
    private static final String INCLUDE_MODEL_DEFINITION = "include_model_definition";

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(GET, BASE_PATH + "trained_models/{" + TrainedModelConfig.MODEL_ID + "}")
                .replaces(GET, BASE_PATH + "inference/{" + TrainedModelConfig.MODEL_ID + "}", RestApiVersion.V_8)
                .build(),
            Route.builder(GET, BASE_PATH + "trained_models").replaces(GET, BASE_PATH + "inference", RestApiVersion.V_8).build()
        );
    }

    private static final Map<String, String> DEFAULT_TO_XCONTENT_VALUES = Collections.singletonMap(
        TrainedModelConfig.DECOMPRESS_DEFINITION,
        Boolean.toString(true)
    );

    @Override
    public String getName() {
        return "ml_get_trained_models_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String modelId = restRequest.param(TrainedModelConfig.MODEL_ID.getPreferredName());
        if (Strings.isNullOrEmpty(modelId)) {
            modelId = Metadata.ALL;
        }
        List<String> tags = asList(restRequest.paramAsStringArray(TrainedModelConfig.TAGS.getPreferredName(), Strings.EMPTY_ARRAY));
        Set<String> includes = new HashSet<>(
            asList(restRequest.paramAsStringArray(GetTrainedModelsAction.Request.INCLUDE.getPreferredName(), Strings.EMPTY_ARRAY))
        );
        final GetTrainedModelsAction.Request request;
        if (restRequest.hasParam(INCLUDE_MODEL_DEFINITION)) {
            deprecationLogger.warn(
                DeprecationCategory.API,
                INCLUDE_MODEL_DEFINITION,
                "[{}] parameter is deprecated! Use [include=definition] instead.",
                INCLUDE_MODEL_DEFINITION
            );
            request = new GetTrainedModelsAction.Request(
                modelId,
                tags,
                restRequest.paramAsBoolean(INCLUDE_MODEL_DEFINITION, false) ? Set.of(DEFINITION) : Set.of()
            );
        } else {
            request = new GetTrainedModelsAction.Request(modelId, tags, includes);
        }
        if (restRequest.hasParam(PageParams.FROM.getPreferredName()) || restRequest.hasParam(PageParams.SIZE.getPreferredName())) {
            request.setPageParams(
                new PageParams(
                    restRequest.paramAsInt(PageParams.FROM.getPreferredName(), PageParams.DEFAULT_FROM),
                    restRequest.paramAsInt(PageParams.SIZE.getPreferredName(), PageParams.DEFAULT_SIZE)
                )
            );
        }
        request.setAllowNoResources(restRequest.paramAsBoolean(ALLOW_NO_MATCH.getPreferredName(), request.isAllowNoResources()));
        return channel -> new RestCancellableNodeClient(client, restRequest.getHttpChannel()).execute(
            GetTrainedModelsAction.INSTANCE,
            request,
            new RestToXContentListenerWithDefaultValues<>(channel, DEFAULT_TO_XCONTENT_VALUES, includes)
        );
    }

    @Override
    protected Set<String> responseParams() {
        return Set.of(TrainedModelConfig.DECOMPRESS_DEFINITION, EXCLUDE_GENERATED);
    }

    private static class RestToXContentListenerWithDefaultValues<T extends ToXContentObject> extends RestToXContentListener<T> {
        private final Map<String, String> defaultToXContentParamValues;
        private final Set<String> includes;

        private RestToXContentListenerWithDefaultValues(
            RestChannel channel,
            Map<String, String> defaultToXContentParamValues,
            Set<String> includes
        ) {
            super(channel);
            this.defaultToXContentParamValues = defaultToXContentParamValues;
            this.includes = includes;
        }

        @Override
        public RestResponse buildResponse(T response, XContentBuilder builder) throws Exception {
            assert response.isFragment() == false; // would be nice if we could make default methods final
            Map<String, String> params = new HashMap<>(channel.request().params());
            defaultToXContentParamValues.forEach((k, v) -> params.computeIfAbsent(k, defaultToXContentParamValues::get));
            includes.forEach(include -> params.put(include, "true"));
            params.put(ToXContentParams.FOR_INTERNAL_STORAGE, "false");
            response.toXContent(builder, new ToXContent.MapParams(params));
            return new RestResponse(getStatus(response), builder);
        }
    }
}
