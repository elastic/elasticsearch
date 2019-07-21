package graphql.schema;


import graphql.PublicSpi;

/**
 * A data fetcher is responsible for returning a data value back for a given graphql field.  The graphql engine
 * uses data fetchers to resolve / fetch a logical field into a runtime object that will be sent back as part
 * of the overall graphql {@link graphql.ExecutionResult}
 *
 * In other implementations, these are sometimes called "Resolvers" or "Field Resolvers", because that is there function,
 * they resolve a logical graphql field into an actual data value.
 *
 * @param <T> the type of object returned. May also be wrapped in a {@link graphql.execution.DataFetcherResult}
 */
@PublicSpi
public interface DataFetcher<T> {

    /**
     * This is called by the graphql engine to fetch the value.  The {@link graphql.schema.DataFetchingEnvironment} is a composite
     * context object that tells you all you need to know about how to fetch a data value in graphql type terms.
     *
     * @param environment this is the data fetching environment which contains all the context you need to fetch a value
     *
     * @return a value of type T. May be wrapped in a {@link graphql.execution.DataFetcherResult}
     *
     * @throws Exception to relieve the implementations from having to wrap checked exceptions. Any exception thrown
     *                   from a {@code DataFetcher} will eventually be handled by the registered {@link graphql.execution.DataFetcherExceptionHandler}
     *                   and the related field will have a value of {@code null} in the result.
     */
    T get(DataFetchingEnvironment environment) throws Exception;


}
