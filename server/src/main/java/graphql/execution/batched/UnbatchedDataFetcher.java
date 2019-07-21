package graphql.execution.batched;


import graphql.execution.Async;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static graphql.schema.DataFetchingEnvironmentImpl.newDataFetchingEnvironment;

/**
 * Given a normal data fetcher as a delegate,
 * uses that fetcher in a batched context by iterating through each source value and calling
 * the delegate.
 *
 * @deprecated This has been deprecated in favour of using {@link graphql.execution.AsyncExecutionStrategy} and {@link graphql.execution.instrumentation.dataloader.DataLoaderDispatcherInstrumentation}
 */
@Deprecated
public class UnbatchedDataFetcher implements BatchedDataFetcher {

    private final DataFetcher delegate;

    public UnbatchedDataFetcher(DataFetcher delegate) {
        this.delegate = delegate;
    }


    @Override
    public CompletableFuture<List<Object>> get(DataFetchingEnvironment environment) throws Exception {
        List<Object> sources = environment.getSource();
        List<CompletableFuture<Object>> results = new ArrayList<>();
        for (Object source : sources) {

            DataFetchingEnvironment singleEnv = newDataFetchingEnvironment(environment)
                    .source(source).build();
            CompletableFuture<Object> cf = Async.toCompletableFuture(delegate.get(singleEnv));
            results.add(cf);
        }
        return Async.each(results);
    }
}
