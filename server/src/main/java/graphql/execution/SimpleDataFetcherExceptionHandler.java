package graphql.execution;

import graphql.ExceptionWhileDataFetching;
import graphql.language.SourceLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The standard handling of data fetcher error involves placing a {@link ExceptionWhileDataFetching} error
 * into the error collection
 */
public class SimpleDataFetcherExceptionHandler implements DataFetcherExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(SimpleDataFetcherExceptionHandler.class);

    @Override
    public DataFetcherExceptionHandlerResult onException(DataFetcherExceptionHandlerParameters handlerParameters) {
        Throwable exception = handlerParameters.getException();
        SourceLocation sourceLocation = handlerParameters.getSourceLocation();
        ExecutionPath path = handlerParameters.getPath();

        ExceptionWhileDataFetching error = new ExceptionWhileDataFetching(path, exception, sourceLocation);
        log.warn(error.getMessage(), exception);

        return DataFetcherExceptionHandlerResult.newResult().error(error).build();
    }
}
