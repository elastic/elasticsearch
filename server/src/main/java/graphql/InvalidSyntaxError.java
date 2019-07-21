package graphql;


import graphql.language.SourceLocation;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;

public class InvalidSyntaxError implements GraphQLError {

    private final String message;
    private final String sourcePreview;
    private final String offendingToken;
    private final List<SourceLocation> locations = new ArrayList<>();

    public InvalidSyntaxError(SourceLocation sourceLocation, String msg) {
        this(singletonList(sourceLocation), msg);
    }

    public InvalidSyntaxError(List<SourceLocation> sourceLocations, String msg) {
        this(sourceLocations, msg, null, null);
    }

    public InvalidSyntaxError(List<SourceLocation> sourceLocations, String msg, String sourcePreview, String offendingToken) {
        this.message = msg;
        this.sourcePreview = sourcePreview;
        this.offendingToken = offendingToken;
        if (sourceLocations != null) {
            this.locations.addAll(sourceLocations);
        }
    }

    @Override
    public String getMessage() {
        return message;
    }

    @Override
    public List<SourceLocation> getLocations() {
        return locations;
    }

    public String getSourcePreview() {
        return sourcePreview;
    }

    public String getOffendingToken() {
        return offendingToken;
    }

    @Override
    public ErrorType getErrorType() {
        return ErrorType.InvalidSyntax;
    }

    @Override
    public String toString() {
        return "InvalidSyntaxError{" +
                " message=" + message +
                " ,offendingToken=" + offendingToken +
                " ,locations=" + locations +
                " ,sourcePreview=" + sourcePreview +
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
