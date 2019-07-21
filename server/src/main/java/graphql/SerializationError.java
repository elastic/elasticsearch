package graphql;


import graphql.execution.ExecutionPath;
import graphql.language.SourceLocation;
import graphql.schema.CoercingSerializeException;

import java.util.List;

import static graphql.Assert.assertNotNull;
import static java.lang.String.format;

@PublicApi
public class SerializationError implements GraphQLError {

    private final String message;
    private final List<Object> path;
    private final CoercingSerializeException exception;

    public SerializationError(ExecutionPath path, CoercingSerializeException exception) {
        this.path = assertNotNull(path).toList();
        this.exception = assertNotNull(exception);
        this.message = mkMessage(path, exception);
    }

    private String mkMessage(ExecutionPath path, CoercingSerializeException exception) {
        return format("Can't serialize value (%s) : %s", path, exception.getMessage());
    }

    public CoercingSerializeException getException() {
        return exception;
    }


    @Override
    public String getMessage() {
        return message;
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
    public List<Object> getPath() {
        return path;
    }

    @Override
    public String toString() {
        return "SerializationError{" +
                "path=" + path +
                "exception=" + exception +
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
