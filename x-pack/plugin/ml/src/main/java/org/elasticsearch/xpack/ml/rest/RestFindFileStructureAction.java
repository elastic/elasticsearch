/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.FindFileStructureAction;
import org.elasticsearch.xpack.core.ml.filestructurefinder.FileStructure;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.filestructurefinder.FileStructureFinderManager;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

public class RestFindFileStructureAction extends BaseRestHandler {

    public RestFindFileStructureAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.POST, MachineLearning.BASE_PATH + "find_file_structure", this);
    }

    @Override
    public String getName() {
        return "xpack_ml_find_file_structure_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {

        FindFileStructureAction.Request request = new FindFileStructureAction.Request();
        request.setLinesToSample(restRequest.paramAsInt(FindFileStructureAction.Request.LINES_TO_SAMPLE.getPreferredName(),
            FileStructureFinderManager.DEFAULT_IDEAL_SAMPLE_LINE_COUNT));
        if (restRequest.hasContent()) {
            request.setSample(restRequest.content());
        } else {
            throw new ElasticsearchParseException("request body is required");
        }

        return channel -> client.execute(FindFileStructureAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    @Override
    protected Set<String> responseParams() {
        return Collections.singleton(FileStructure.EXPLAIN);
    }
}
