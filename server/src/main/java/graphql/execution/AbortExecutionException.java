package graphql.execution;

import graphql.ErrorType;
import graphql.ExecutionResult;
import graphql.ExecutionResultImpl;
import graphql.GraphQLError;
import graphql.GraphQLException;
import graphql.PublicApi;
import graphql.language.SourceLocation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static graphql.Assert.assertNotNull;
import static java.util.Collections.emptyList;

/**
 * This Exception indicates that the current execution should be aborted.
 */
@PublicApi
public class AbortExecutionException extends GraphQLException implements GraphQLError {

    private final List<GraphQLError> underlyingErrors;

    public AbortExecutionException() {
        this.underlyingErrors = emptyList();
    }

    public AbortExecutionException(Collection<GraphQLError> underlyingErrors) {
        this.underlyingErrors = new ArrayList<>(assertNotNull(underlyingErrors));
    }

    public AbortExecutionException(String message) {
        super(message);
        this.underlyingErrors = emptyList();
    }

    public AbortExecutionException(String message, Throwable cause) {
        super(message, cause);
        this.underlyingErrors = emptyList();
    }

    public AbortExecutionException(Throwable cause) {
        super(cause);
        this.underlyingErrors = emptyList();
    }

    @Override
    public List<SourceLocation> getLocations() {
        return null;
    }

    @Override
    public ErrorType getErrorType() {
        return ErrorType.ExecutionAborted;
    }

    /**
     * @return a list of underlying errors, which may be empty
     */
    public List<GraphQLError> getUnderlyingErrors() {
        return underlyingErrors;
    }

    /**
     * This is useful for turning this abort signal into an execution result which
     * is an error state with the underlying errors in it.
     *
     * @return an excution result with the errors from this exception
     */
    public ExecutionResult toExecutionResult() {
        ExecutionResult executionResult = new ExecutionResultImpl(this);
        if (!this.getUnderlyingErrors().isEmpty()) {
            executionResult = new ExecutionResultImpl(this.getUnderlyingErrors());
        }
        return executionResult;
    }
}
