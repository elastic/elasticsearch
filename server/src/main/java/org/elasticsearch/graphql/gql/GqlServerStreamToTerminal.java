package org.elasticsearch.graphql.gql;

import graphql.ExecutionResult;
import graphql.GraphQL;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Map;

public class GqlServerStreamToTerminal implements Runnable {
    ExecutionResult result;

    public GqlServerStreamToTerminal(ExecutionResult result) {
        this.result = result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() {
        Map<Object, Object> extensions = result.getExtensions();
        Publisher<ExecutionResult> deferredResults = (Publisher<ExecutionResult>) extensions.get(GraphQL.DEFERRED_RESULTS);
        if (deferredResults != null) {
            processDeferredResults(deferredResults);
        }
    }

    private void processDeferredResults(Publisher<ExecutionResult> deferredResults) {
        deferredResults.subscribe(new Subscriber<ExecutionResult>() {

            Subscription subscription;

            @Override
            public void onSubscribe(Subscription s) {
                subscription = s;
                subscription.request(10);
            }

            @Override
            public void onNext(ExecutionResult executionResult) {
                streamResult(executionResult.toSpecification());
                subscription.request(10);
            }

            @Override
            public void onError(Throwable t) {
                streamResult(t);
            }

            @Override
            public void onComplete() {
                streamResult("CLOSING STREAM");
            }
        });
    }

    private void streamResult(Object data) {
        System.out.println("---------------------------------------------------------------");
        System.out.println("");
        System.out.println("This should be streamed as HTTP 1.1 Transfer-Encoding: chunked:");
        System.out.println("");
        System.out.println(data);
        System.out.println("");
    }
}
