package graphql.execution;

import graphql.PublicSpi;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

/**
 * This is called when an exception is thrown during {@link graphql.schema.DataFetcher#get(DataFetchingEnvironment)} execution
 */
@PublicSpi
public interface DataFetcherExceptionHandler {

    /**
     * When an exception during a call to a {@link DataFetcher} then this handler
     * is called back to shape the error that should be placed in the list of errors
     *
     * @param handlerParameters the parameters to this callback
     *
     * @return a result that can contain custom formatted {@link graphql.GraphQLError}s
     */
    DataFetcherExceptionHandlerResult onException(DataFetcherExceptionHandlerParameters handlerParameters);
}
