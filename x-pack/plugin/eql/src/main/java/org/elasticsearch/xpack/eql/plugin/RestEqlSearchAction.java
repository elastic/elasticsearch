/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.plugin;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestEqlSearchAction extends BaseRestHandler {
    private static Logger logger = LogManager.getLogger(RestEqlSearchAction.class);
    private static final String SEARCH_PATH = "/{index}/_eql/search";

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, SEARCH_PATH),
            new Route(POST, SEARCH_PATH)
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client)
        throws IOException {

        EqlSearchRequest eqlRequest;
        String indices;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            eqlRequest = EqlSearchRequest.fromXContent(parser);
            indices = request.param("index");
            eqlRequest.indices(Strings.splitStringByCommaToArray(indices));
            eqlRequest.indicesOptions(IndicesOptions.fromRequest(request, eqlRequest.indicesOptions()));
            if (request.hasParam("wait_for_completion_timeout")) {
                eqlRequest.waitForCompletionTimeout(
                    request.paramAsTime("wait_for_completion_timeout", eqlRequest.waitForCompletionTimeout()));
            }
            if (request.hasParam("keep_alive")) {
                eqlRequest.keepAlive(request.paramAsTime("keep_alive", eqlRequest.keepAlive()));
            }
            eqlRequest.keepOnCompletion(request.paramAsBoolean("keep_on_completion", eqlRequest.keepOnCompletion()));
            eqlRequest.ccsMinimizeRoundtrips(request.paramAsBoolean("ccs_minimize_roundtrips", eqlRequest.ccsMinimizeRoundtrips()));
        }

        return channel -> {
            RestCancellableNodeClient cancellableClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancellableClient.execute(EqlSearchAction.INSTANCE, eqlRequest, new ActionListener<>() {
                @Override
                public void onResponse(EqlSearchResponse response) {
                    try {
                        XContentBuilder builder = channel.newBuilder(request.getXContentType(), XContentType.JSON, true);
                        response.toXContent(builder, request);
                        channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    Exception finalException = e;
                    /*
                     * In a scenario when Security is enabled and a wildcarded pattern gets resolved to no index, the original error
                     * message will not contain the initial pattern, but "*,-*". So, we'll throw a INFE from the PreAnalyzer that will
                     * contain as cause the VerificationException with "*,-*" pattern but we'll rewrite the INFE here with the initial
                     * pattern that failed resolving. More details here https://github.com/elastic/elasticsearch/issues/63529
                     */
                    if (e instanceof IndexNotFoundException) {
                        IndexNotFoundException infe = (IndexNotFoundException) e;
                        if (infe.getIndex() != null && infe.getIndex().getName().equals("Unknown index [*,-*]")) {
                            finalException = new IndexNotFoundException(indices, infe.getCause());
                        }
                    }
                    try {
                        channel.sendResponse(new BytesRestResponse(channel, finalException));
                    } catch (Exception inner) {
                        inner.addSuppressed(finalException);
                        logger.error("failed to send failure response", inner);
                    }
                }
            });
        };
    }

    @Override
    public String getName() {
        return "eql_search";
    }
}
