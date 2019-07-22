package org.elasticsearch.graphql.api;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface GqlApi {
    public CompletableFuture<Map<String, Object>> getHello() throws Exception;
    public CompletableFuture<List<Object>> getIndices() throws Exception;
}
