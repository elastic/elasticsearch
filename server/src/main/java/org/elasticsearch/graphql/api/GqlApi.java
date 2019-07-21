package org.elasticsearch.graphql.api;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.main.MainResponse;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface GqlApi {
    public CompletableFuture<Map<String, Object>> getHello();
}
