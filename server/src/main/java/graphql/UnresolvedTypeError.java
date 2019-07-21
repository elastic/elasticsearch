package graphql;

import graphql.execution.ExecutionPath;
import graphql.execution.ExecutionStepInfo;
import graphql.execution.UnresolvedTypeException;
import graphql.language.SourceLocation;

import java.util.List;

import static graphql.Assert.assertNotNull;
import static java.lang.String.format;

@PublicApi
public class UnresolvedTypeError implements GraphQLError {

    private final String message;
    private final List<Object> path;
    private final UnresolvedTypeException exception;

    public UnresolvedTypeError(ExecutionPath path, ExecutionStepInfo info,
                               UnresolvedTypeException exception) {
        this.path = assertNotNull(path).toList();
        this.exception = assertNotNull(exception);
        this.message = mkMessage(path, exception, assertNotNull(info));
    }

    private String mkMessage(ExecutionPath path, UnresolvedTypeException exception, ExecutionStepInfo info) {
        return format("Can't resolve '%s'. Abstract type '%s' must resolve to an Object type at runtime for field '%s.%s'. %s",
                path,
                exception.getInterfaceOrUnionType().getName(),
                info.getParent().getUnwrappedNonNullType().getName(),
                info.getFieldDefinition().getName(),
                exception.getMessage());
    }

    public UnresolvedTypeException getException() {
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
        return "UnresolvedTypeError{" +
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