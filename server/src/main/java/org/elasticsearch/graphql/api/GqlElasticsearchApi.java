package org.elasticsearch.graphql.api;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.main.MainAction;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.javautil.JavaUtilXContent;
import org.elasticsearch.common.xcontent.javautil.JavaUtilXContentGenerator;

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
            @SuppressWarnings("unchecked")
            public void onResponse(MainResponse mainResponse) {
                try {
                    XContentBuilder builder = new XContentBuilder(JavaUtilXContent.javaUtilXContent, null);
                    mainResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
                    builder.close();
                    JavaUtilXContentGenerator generator = (JavaUtilXContentGenerator) builder.generator();
                    future.complete((Map) generator.getResult());
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
