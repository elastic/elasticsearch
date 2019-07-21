package graphql.execution.batched;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

import java.lang.reflect.Method;

/**
 * Produces a BatchedDataFetcher for a given DataFetcher.
 * If that fetcher is already a BatchedDataFetcher we return it.
 * If that fetcher's get method is annotated @Batched then we delegate to it directly.
 * Otherwise we wrap the fetcher in a BatchedDataFetcher that iterates over the sources and invokes the delegate
 * on each source. Note that this forgoes any performance benefits of batching,
 * so regular DataFetchers should normally only be used if they are in-memory.
 *
 * @deprecated This has been deprecated in favour of using {@link graphql.execution.AsyncExecutionStrategy} and {@link graphql.execution.instrumentation.dataloader.DataLoaderDispatcherInstrumentation}
 */
@Deprecated
public class BatchedDataFetcherFactory {
    public BatchedDataFetcher create(final DataFetcher supplied) {
        if (supplied instanceof BatchedDataFetcher) {
            return (BatchedDataFetcher) supplied;
        }
        try {
            Method getMethod = supplied.getClass().getMethod("get", DataFetchingEnvironment.class);
            Batched batched = getMethod.getAnnotation(Batched.class);
            if (batched != null) {
                return supplied::get;
            }
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(e);
        }
        return new UnbatchedDataFetcher(supplied);
    }
}
