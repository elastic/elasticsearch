package org.elasticsearch.graphql.api;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.main.MainAction;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.client.node.NodeClient;

import java.util.*;
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
                try {
//                    ByteArrayOutputStream os = new ByteArrayOutputStream();
//                    XContentBuilder builder = new XContentBuilder(JsonXContent.jsonXContent, os);
//                    mainResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
//                    builder.close();
//                    String json = os.toString(StandardCharsets.UTF_8);
//                    System.out.println("JSON: " + json);
//                    Map<String, Object> map = new Gson().fromJson(json, HashMap.class);
                    future.complete(mainResponse.toMap());
                } catch (Exception e) {
                    future.completeExceptionally(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }
}
