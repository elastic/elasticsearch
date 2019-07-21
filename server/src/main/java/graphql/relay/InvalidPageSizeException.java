package graphql.relay;

import graphql.ErrorType;
import graphql.GraphQLError;
import graphql.GraphqlErrorHelper;
import graphql.language.SourceLocation;

import java.util.List;

import static graphql.ErrorType.DataFetchingException;

public class InvalidPageSizeException extends RuntimeException implements GraphQLError {

    InvalidPageSizeException(String message) {
        this(message, null);
    }

    InvalidPageSizeException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public List<SourceLocation> getLocations() {
        return null;
    }

    @Override
    public ErrorType getErrorType() {
        return DataFetchingException;
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
