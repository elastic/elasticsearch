package org.elasticsearch.graphql.gql;

import graphql.ExecutionResult;
import org.reactivestreams.Publisher;

import java.util.Map;

public interface GqlResult {

    Map<String, Object> getSpecification();

    boolean hasDeferredResults();

    Publisher<Map<String, Object>> getDeferredResults();

    boolean isSubscription();

    Publisher<Map<String, Object>> getSubscription();
}
