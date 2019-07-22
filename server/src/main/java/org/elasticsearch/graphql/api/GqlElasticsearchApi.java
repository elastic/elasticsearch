package org.elasticsearch.graphql.api;

import org.elasticsearch.action.main.MainAction;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.cat.RestIndicesAction;

import java.io.IOException;
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
        CompletableFuture<List<Object>> promise = new CompletableFuture<>();
        XContentBuilder builder = GqlApiUtils.createJavaUtilBuilder();

        GqlApiFakeHttpRequest internalHttpRequest = new GqlApiFakeHttpRequest(RestRequest.Method.GET, "/_cat/indices?format=json", BytesArray.EMPTY, new HashMap<>());
        GqlApiFakeHttpChannel internalHttpChannel = new GqlApiFakeHttpChannel(null);
        RestRequest innerRequest = RestRequest.request(NamedXContentRegistry.EMPTY, internalHttpRequest, internalHttpChannel);

        BaseRestHandler.RestChannelConsumer channelConsumer = RestIndicesAction.INSTANCE.doCatRequest(innerRequest, client);
        channelConsumer.accept(new RestChannel() {
            BytesStreamOutput bytes1 = new BytesStreamOutput();

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
                return bytes1;
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
                System.out.println("SENDIN RESPONSE");
                System.out.println(response);
                try {
                    List<Object> result = (List) GqlApiUtils.getJavaUtilBuilderResult(builder);
                    System.out.println(result);
                    promise.complete(result);
                } catch (Exception e) {
                    promise.completeExceptionally(e);
                }
            }
        });

        return promise;
    }
}
