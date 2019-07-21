package graphql.execution;

import graphql.ExecutionResult;
import graphql.ExecutionResultImpl;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;


public abstract class AbstractAsyncExecutionStrategy extends ExecutionStrategy {

    public AbstractAsyncExecutionStrategy() {
    }

    public AbstractAsyncExecutionStrategy(DataFetcherExceptionHandler dataFetcherExceptionHandler) {
        super(dataFetcherExceptionHandler);
    }

    protected BiConsumer<List<ExecutionResult>, Throwable> handleResults(ExecutionContext executionContext, List<String> fieldNames, CompletableFuture<ExecutionResult> overallResult) {
        return (List<ExecutionResult> results, Throwable exception) -> {
            if (exception != null) {
                handleNonNullException(executionContext, overallResult, exception);
                return;
            }
            Map<String, Object> resolvedValuesByField = new LinkedHashMap<>();
            int ix = 0;
            for (ExecutionResult executionResult : results) {

                String fieldName = fieldNames.get(ix++);
                resolvedValuesByField.put(fieldName, executionResult.getData());
            }
            overallResult.complete(new ExecutionResultImpl(resolvedValuesByField, executionContext.getErrors()));
        };
    }
}
