package org.elasticsearch.graphql.api;

import org.elasticsearch.action.main.MainAction;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.client.node.NodeClient;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import org.elasticsearch.rest.action.cat.RestIndicesAction;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class GqlElasticsearchApi implements GqlApi {
    NodeClient client;

    public GqlElasticsearchApi(NodeClient client) {
        this.client = client;
    }

    @Override
    public CompletableFuture<Map<String, Object>> getHello() throws Exception {
        return GqlApiUtils.executeAction(client, MainAction.INSTANCE, new MainRequest());
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<List<Object>> getIndices() throws Exception {
        return GqlApiUtils.executeRestHandler(client, RestIndicesAction.INSTANCE, GET, "/_cat/indices?format=json");
    }
}
