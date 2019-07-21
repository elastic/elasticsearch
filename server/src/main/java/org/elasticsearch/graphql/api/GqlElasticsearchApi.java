package org.elasticsearch.graphql.api;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.main.MainAction;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.client.node.NodeClient;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class GqlElasticsearchApi implements GqlApi {
    NodeClient client;

    public GqlElasticsearchApi(NodeClient client) {
        this.client = client;
    }

    @Override
    public CompletableFuture<Map<String, Object>> getHello() {
        CompletableFuture<Map<String, Object>> future = new CompletableFuture();
        client.execute(MainAction.INSTANCE, new MainRequest(), new ActionListener<MainResponse>() {
            @Override
            public void onResponse(MainResponse mainResponse) {
                LinkedHashMap<String, Object> result = new LinkedHashMap();
                result.put("name", "some-name");
                result.put("cluster_name", "elasticsearch");
                future.complete(result);
            }

            @Override
            public void onFailure(Exception e) {
                LinkedHashMap<String, Object> result = new LinkedHashMap();
                result.put("name", "some-name");
                result.put("cluster_name", "elasticsearch");
                future.complete(result);
            }
        });
        return future;
    }
}
