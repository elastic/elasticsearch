package graphql.execution;

import graphql.ErrorType;
import graphql.GraphQLError;
import graphql.GraphqlErrorHelper;
import graphql.language.SourceLocation;

import java.util.List;

/**
 * This is the base error that indicates that a non null field value was in fact null.
 *
 * @see graphql.execution.NonNullableFieldWasNullException for details
 */
public class NonNullableFieldWasNullError implements GraphQLError {

    private final String message;
    private final List<Object> path;

    public NonNullableFieldWasNullError(NonNullableFieldWasNullException exception) {
        this.message = exception.getMessage();
        this.path = exception.getPath().toList();
    }

    @Override
    public String getMessage() {
        return message;
    }

    @Override
    public List<Object> getPath() {
        return path;
    }

    @Override
    public List<SourceLocation> getLocations() {
        return null;
    }

    @Override
    public ErrorType getErrorType() {
        return ErrorType.DataFetchingException;
    }

    @Override
    public String toString() {
        return "NonNullableFieldWasNullError{" +
                "message='" + message + '\'' +
                ", path=" + path +
                '}';
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object o) {
        return GraphqlErrorHelper.equals(this, o);
    }

    @Override
    public int hashCode() {
        return GraphqlErrorHelper.hashCode(this);
    }
}
