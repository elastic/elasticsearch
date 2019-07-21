package graphql.execution.defer;

import graphql.DeferredExecutionResult;
import graphql.DeferredExecutionResultImpl;
import graphql.ExecutionResult;
import graphql.GraphQLError;
import graphql.Internal;
import graphql.execution.ExecutionPath;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * This represents a deferred call (aka @defer) to get an execution result sometime after
 * the initial query has returned
 */
@Internal
public class DeferredCall {
    private final ExecutionPath path;
    private final Supplier<CompletableFuture<ExecutionResult>> call;
    private final DeferredErrorSupport errorSupport;

    public DeferredCall(ExecutionPath path, Supplier<CompletableFuture<ExecutionResult>> call, DeferredErrorSupport deferredErrorSupport) {
        this.path = path;
        this.call = call;
        this.errorSupport = deferredErrorSupport;
    }

    CompletableFuture<DeferredExecutionResult> invoke() {
        CompletableFuture<ExecutionResult> future = call.get();
        return future.thenApply(this::transformToDeferredResult);
    }

    private DeferredExecutionResult transformToDeferredResult(ExecutionResult executionResult) {
        List<GraphQLError> errorsEncountered = errorSupport.getErrors();
        DeferredExecutionResultImpl.Builder builder = DeferredExecutionResultImpl.newDeferredExecutionResult().from(executionResult);
        return builder
                .addErrors(errorsEncountered)
                .path(path)
                .build();
    }
}
