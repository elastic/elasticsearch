package graphql.execution;

import graphql.ExecutionResult;
import graphql.ExecutionResultImpl;
import graphql.GraphQLException;
import graphql.PublicApi;
import graphql.execution.instrumentation.Instrumentation;
import graphql.execution.instrumentation.InstrumentationContext;
import graphql.execution.instrumentation.parameters.InstrumentationExecutionStrategyParameters;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 *
 * <p>Deprecation Notice : This execution strategy does not support all of the graphql-java capabilities
 * such as data loader or @defer fields.  Since its so easy to create a data fetcher that uses
 * {@link java.util.concurrent.CompletableFuture#supplyAsync(java.util.function.Supplier, java.util.concurrent.Executor)}
 * to make field fetching happen off thread we recommend that you use that instead of this class.  This class
 * will be removed in a future version.
 * </p>
 *
 * <p>ExecutorServiceExecutionStrategy uses an {@link ExecutorService} to parallelize the resolve.</p>
 *
 * Due to the nature of {@link #execute(ExecutionContext, ExecutionStrategyParameters)}  implementation, {@link ExecutorService}
 * MUST have the following 2 characteristics:
 * <ul>
 * <li>1. The underlying {@link java.util.concurrent.ThreadPoolExecutor} MUST have a reasonable {@code maximumPoolSize}
 * <li>2. The underlying {@link java.util.concurrent.ThreadPoolExecutor} SHALL NOT use its task queue.
 * </ul>
 *
 * <p>Failure to follow 1. and 2. can result in a very large number of threads created or hanging. (deadlock)</p>
 *
 * See {@code graphql.execution.ExecutorServiceExecutionStrategyTest} for example usage.
 *
 * @deprecated Use {@link graphql.execution.AsyncExecutionStrategy} and {@link java.util.concurrent.CompletableFuture#supplyAsync(java.util.function.Supplier, java.util.concurrent.Executor)}
 * in your data fetchers instead.
 */
@PublicApi
@Deprecated
public class ExecutorServiceExecutionStrategy extends ExecutionStrategy {

    final ExecutorService executorService;

    public ExecutorServiceExecutionStrategy(ExecutorService executorService) {
        this(executorService, new SimpleDataFetcherExceptionHandler());
    }

    public ExecutorServiceExecutionStrategy(ExecutorService executorService, DataFetcherExceptionHandler dataFetcherExceptionHandler) {
        super(dataFetcherExceptionHandler);
        this.executorService = executorService;
    }


    @Override
    public CompletableFuture<ExecutionResult> execute(final ExecutionContext executionContext, final ExecutionStrategyParameters parameters) {
        if (executorService == null) {
            return new AsyncExecutionStrategy().execute(executionContext, parameters);
        }

        Instrumentation instrumentation = executionContext.getInstrumentation();
        InstrumentationExecutionStrategyParameters instrumentationParameters = new InstrumentationExecutionStrategyParameters(executionContext, parameters);
        InstrumentationContext<ExecutionResult> executionStrategyCtx = instrumentation.beginExecutionStrategy(instrumentationParameters);

        MergedSelectionSet fields = parameters.getFields();
        Map<String, Future<CompletableFuture<ExecutionResult>>> futures = new LinkedHashMap<>();
        for (String fieldName : fields.keySet()) {
            final MergedField currentField = fields.getSubField(fieldName);

            ExecutionPath fieldPath = parameters.getPath().segment(mkNameForPath(currentField));
            ExecutionStrategyParameters newParameters = parameters
                    .transform(builder -> builder.field(currentField).path(fieldPath));

            Callable<CompletableFuture<ExecutionResult>> resolveField = () -> resolveField(executionContext, newParameters);
            futures.put(fieldName, executorService.submit(resolveField));
        }

        CompletableFuture<ExecutionResult> overallResult = new CompletableFuture<>();
        executionStrategyCtx.onDispatched(overallResult);

        try {
            Map<String, Object> results = new LinkedHashMap<>();
            for (String fieldName : futures.keySet()) {
                ExecutionResult executionResult;
                try {
                    executionResult = futures.get(fieldName).get().join();
                } catch (CompletionException e) {
                    if (e.getCause() instanceof NonNullableFieldWasNullException) {
                        assertNonNullFieldPrecondition((NonNullableFieldWasNullException) e.getCause());
                        results = null;
                        break;
                    } else {
                        throw e;
                    }
                }
                results.put(fieldName, executionResult != null ? executionResult.getData() : null);
            }

            ExecutionResultImpl executionResult = new ExecutionResultImpl(results, executionContext.getErrors());
            overallResult.complete(executionResult);

            overallResult = overallResult.whenComplete(executionStrategyCtx::onCompleted);
            return overallResult;
        } catch (InterruptedException | ExecutionException e) {
            executionStrategyCtx.onCompleted(null, e);
            throw new GraphQLException(e);
        }
    }
}
