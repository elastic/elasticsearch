package graphql.execution.defer;

import graphql.ExceptionWhileDataFetching;
import graphql.GraphQLError;
import graphql.Internal;
import graphql.execution.ExecutionStrategyParameters;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * This captures errors that occur while a deferred call is being made
 */
@Internal
public class DeferredErrorSupport {

    private final List<GraphQLError> errors = new CopyOnWriteArrayList<>();

    public void onFetchingException(ExecutionStrategyParameters parameters, Throwable e) {
        ExceptionWhileDataFetching error = new ExceptionWhileDataFetching(parameters.getPath(), e, parameters.getField().getSingleField().getSourceLocation());
        onError(error);
    }

    public void onError(GraphQLError gError) {
        errors.add(gError);
    }

    public List<GraphQLError> getErrors() {
        return errors;
    }
}
