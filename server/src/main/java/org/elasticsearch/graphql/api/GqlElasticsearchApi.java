package org.elasticsearch.graphql.api;

import org.elasticsearch.action.main.MainAction;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentBuilder;

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
        XContentBuilder builder = GqlApiUtils.createJavaUtilBuilder();
        builder
            .startArray()
                .startObject()
                    .field("health", "green")
                    .field("status", "open")
                    .field("index", ".security-7")
                    .field("uuid", "Osf5_rsiQzmJnjvXsYz5HQ")
                    .field("pri", "1")
                    .field("rep", "0")
                    .field("docs.count", "6")
                    .field("docs.deleted", "0")
                    .field("store.size", "19.7Kb")
                    .field("pri.store.size", "19.7Kb")
                .endObject()
            .endArray();

        List data = (List) GqlApiUtils.getJavaUtilBuilderResult(builder);
        System.out.println("data: " + data);
        return CompletableFuture.completedFuture(data);
    }
}
