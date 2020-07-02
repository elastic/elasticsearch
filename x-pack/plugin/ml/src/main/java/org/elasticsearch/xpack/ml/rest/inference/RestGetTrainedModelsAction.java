/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.inference;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction.Request.ALLOW_NO_MATCH;

public class RestGetTrainedModelsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, MachineLearning.BASE_PATH + "inference/{" + TrainedModelConfig.MODEL_ID.getPreferredName() + "}"),
            new Route(GET, MachineLearning.BASE_PATH + "inference"));
    }

    private static final Map<String, String> DEFAULT_TO_XCONTENT_VALUES =
        Collections.singletonMap(TrainedModelConfig.DECOMPRESS_DEFINITION, Boolean.toString(true));
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
        boolean includeModelDefinition = restRequest.paramAsBoolean(
            GetTrainedModelsAction.Request.INCLUDE_MODEL_DEFINITION.getPreferredName(),
            false
        );
        List<String> tags = asList(restRequest.paramAsStringArray(TrainedModelConfig.TAGS.getPreferredName(), Strings.EMPTY_ARRAY));
        GetTrainedModelsAction.Request request = new GetTrainedModelsAction.Request(modelId, includeModelDefinition, tags);
        if (restRequest.hasParam(PageParams.FROM.getPreferredName()) || restRequest.hasParam(PageParams.SIZE.getPreferredName())) {
            request.setPageParams(new PageParams(restRequest.paramAsInt(PageParams.FROM.getPreferredName(), PageParams.DEFAULT_FROM),
                restRequest.paramAsInt(PageParams.SIZE.getPreferredName(), PageParams.DEFAULT_SIZE)));
        }
        request.setAllowNoResources(restRequest.paramAsBoolean(ALLOW_NO_MATCH.getPreferredName(), request.isAllowNoResources()));
        return channel -> client.execute(GetTrainedModelsAction.INSTANCE,
            request,
            new RestToXContentListenerWithDefaultValues<>(channel, DEFAULT_TO_XCONTENT_VALUES));
    }

    @Override
    protected Set<String> responseParams() {
        return Set.of(TrainedModelConfig.DECOMPRESS_DEFINITION, TrainedModelConfig.FOR_EXPORT);
    }

    private static class RestToXContentListenerWithDefaultValues<T extends ToXContentObject> extends RestToXContentListener<T> {
        private final Map<String, String> defaultToXContentParamValues;

        private RestToXContentListenerWithDefaultValues(RestChannel channel, Map<String, String> defaultToXContentParamValues) {
            super(channel);
            this.defaultToXContentParamValues = defaultToXContentParamValues;
        }

        @Override
        public RestResponse buildResponse(T response, XContentBuilder builder) throws Exception {
            assert response.isFragment() == false; //would be nice if we could make default methods final
            Map<String, String> params = new HashMap<>(channel.request().params());
            defaultToXContentParamValues.forEach((k, v) ->
                params.computeIfAbsent(k, defaultToXContentParamValues::get)
            );
            response.toXContent(builder, new ToXContent.MapParams(params));
            return new BytesRestResponse(getStatus(response), builder);
        }
    }
}
