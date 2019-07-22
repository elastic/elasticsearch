package org.elasticsearch.graphql.api;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.javautil.JavaUtilXContent;
import org.elasticsearch.common.xcontent.javautil.JavaUtilXContentGenerator;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class GqlApiUtils {

    static XContentBuilder createJavaUtilBuilder() throws IOException  {
        XContentBuilder builder = new XContentBuilder(JavaUtilXContent.javaUtilXContent, null);
        return builder;
    }

    static Object getJavaUtilBuilderResult(XContentBuilder builder) {
        builder.close();
        JavaUtilXContentGenerator generator = (JavaUtilXContentGenerator) builder.generator();
        return generator.getResult();
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> toMap(ToXContentObject response) throws IOException {
        XContentBuilder builder = createJavaUtilBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return (Map) getJavaUtilBuilderResult(builder);
    }

    static <Request extends ActionRequest, Response extends ActionResponse>
    CompletableFuture<Map<String, Object>> executeAction(NodeClient client, ActionType<Response> action, Request request) {
        CompletableFuture<Map<String, Object>> future = new CompletableFuture<Map<String, Object>>();
        client.execute(action, request, new ActionListener<Response>() {
            @Override
            public void onResponse(ActionResponse response) {
                try {
                    if (response instanceof ToXContentObject) {
                        future.complete(toMap((ToXContentObject) response));
                    }
                    throw new Exception("Response does not implement ToXContentObject.");
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
