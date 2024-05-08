/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.esql.version.EsqlVersion;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.esql.formatter.TextFormat.URL_PARAM_DELIMITER;

@ServerlessScope(Scope.PUBLIC)
public class RestEsqlQueryAction extends BaseRestHandler {
    private static final Logger LOGGER = LogManager.getLogger(RestEsqlQueryAction.class);

    @Override
    public String getName() {
        return "esql_query";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_query"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        EsqlQueryRequest esqlRequest;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            esqlRequest = RequestXContent.parseSync(parser);
        }

        defaultVersionForOldClients(esqlRequest, request);
        LOGGER.debug("Beginning execution of ESQL query.\nQuery string: [{}]", esqlRequest.query());

        return channel -> {
            RestCancellableNodeClient cancellableClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancellableClient.execute(
                EsqlQueryAction.INSTANCE,
                esqlRequest,
                new EsqlResponseListener(channel, request, esqlRequest).wrapWithLogging()
            );
        };
    }

    @Override
    protected Set<String> responseParams() {
        return Set.of(URL_PARAM_DELIMITER, EsqlQueryResponse.DROP_NULL_COLUMNS_OPTION);
    }

    static final String PRODUCT_ORIGIN = "x-elastic-product-origin";
    static final String CLIENT_META = "x-elastic-client-meta";

    /**
     * Default the {@link EsqlQueryRequest#esqlVersion()} to the oldest version
     * if we can detect that the request comes from an older version of the
     * official client or an older version of kibana. These versions supported
     * ESQL but ESQL was not GA, so, <strong>technically</strong> we can break
     * them. But it's not hugely complicated to make them work smoothly on the
     * upgrade that starts to require the {@code version} field. This does
     * just that.
     */
    static void defaultVersionForOldClients(EsqlQueryRequest esqlRequest, RestRequest restRequest) {
        if (esqlRequest.esqlVersion() != null) {
            return;
        }
        String clientMeta = restRequest.header(CLIENT_META);
        if (clientMeta == null) {
            return;
        }
        String product = restRequest.header(PRODUCT_ORIGIN);
        if ("kibana".equals(product)) {
            /*
             * Kibana 8.11 to 8.13 used the 8.9 version of the javascript client.
             * Kibana 8.14, the version we *want* to send the versions is on the
             * 8.13 version of the javascript client.
             */
            if (clientMeta.contains("es=8.9")) {
                esqlRequest.esqlVersion(EsqlVersion.ROCKET.versionStringWithoutEmoji());
            }
            return;
        }
        if (clientMeta.contains("es=8.13") || clientMeta.contains("es=8.12") || clientMeta.contains("es=8.11")) {
            esqlRequest.esqlVersion(EsqlVersion.ROCKET.versionStringWithoutEmoji());
        }
    }
}
