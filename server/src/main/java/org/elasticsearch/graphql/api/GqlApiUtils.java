package org.elasticsearch.graphql.api;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.common.xcontent.javautil.JavaUtilXContent;
import org.elasticsearch.common.xcontent.javautil.JavaUtilXContentGenerator;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.cat.RestIndicesAction;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class GqlApiUtils {

    static public XContentBuilder createJavaUtilBuilder() throws IOException  {
        BytesStreamOutput bos = new BytesStreamOutput();
        XContentBuilder builder = new XContentBuilder(JavaUtilXContent.javaUtilXContent, bos);
        return builder;
    }

    static public Object getJavaUtilBuilderResult(XContentBuilder builder) throws Exception {
        builder.close();
        JavaUtilXContentGenerator generator = (JavaUtilXContentGenerator) builder.generator();
        return generator.getResult();
    }

    @SuppressWarnings("unchecked")
    static public Map<String, Object> toMap(ToXContentObject response) throws Exception {
        XContentBuilder builder = createJavaUtilBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return (Map) getJavaUtilBuilderResult(builder);
    }

    static public <Request extends ActionRequest, Response extends ActionResponse>
            CompletableFuture<Map<String, Object>> executeAction(NodeClient client,
                                                                 ActionType<Response> action,
                                                                 Request request) {
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

    @SuppressWarnings("unchecked")
    static public CompletableFuture<List<Object>> executeRestHandler(NodeClient client,
                                                                     BaseRestHandler handler,
                                                                     RestRequest.Method method,
                                                                     String uri) throws Exception {
        CompletableFuture<List<Object>> promise = new CompletableFuture<>();
        XContentBuilder builder = GqlApiUtils.createJavaUtilBuilder();

        GqlApiFakeHttpRequest internalHttpRequest = new GqlApiFakeHttpRequest(method, uri, BytesArray.EMPTY, new HashMap<>());
        GqlApiFakeHttpChannel internalHttpChannel = new GqlApiFakeHttpChannel(null);
        RestRequest innerRequest = RestRequest.request(NamedXContentRegistry.EMPTY, internalHttpRequest, internalHttpChannel);

        handler.handleRequest(innerRequest, new RestChannel() {
            BytesStreamOutput bytesStreamOutput = new BytesStreamOutput();

            @Override
            public XContentBuilder newBuilder() throws IOException {
                return builder;
            }

            @Override
            public XContentBuilder newErrorBuilder() throws IOException {
                return builder;
            }

            @Override
            public XContentBuilder newBuilder(XContentType xContentType, boolean useFiltering) throws IOException {
                return builder;
            }

            @Override
            public XContentBuilder newBuilder(XContentType xContentType, XContentType responseContentType, boolean useFiltering) throws IOException {
                return builder;
            }

            @Override
            public BytesStreamOutput bytesOutput() {
                return bytesStreamOutput;
            }

            @Override
            public RestRequest request() {
                return innerRequest;
            }

            @Override
            public boolean detailedErrorsEnabled() {
                return false;
            }

            @Override
            public void sendResponse(RestResponse response) {
                try {
                    List<Object> result = (List) GqlApiUtils.getJavaUtilBuilderResult(builder);
                    System.out.println(result);
                    promise.complete(result);
                } catch (Exception e) {
                    promise.completeExceptionally(e);
                }
            }
        }, client);

        return promise;
    }
}
