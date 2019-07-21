package graphql.parser;


import graphql.GraphQLException;
import graphql.Internal;
import graphql.InvalidSyntaxError;
import graphql.language.SourceLocation;

import java.util.Collections;
import java.util.List;

@Internal
public class InvalidSyntaxException extends GraphQLException {

    private final String message;
    private final String sourcePreview;
    private final String offendingToken;
    private final SourceLocation location;

    InvalidSyntaxException(SourceLocation location, String msg, String sourcePreview, String offendingToken, Exception cause) {
        super(cause);
        this.message = mkMessage(msg, offendingToken, location);
        this.sourcePreview = sourcePreview;
        this.offendingToken = offendingToken;
        this.location = location;
    }

    private String mkMessage(String msg, String offendingToken, SourceLocation location) {
        StringBuilder sb = new StringBuilder();
        sb.append("Invalid Syntax :");
        if (msg != null) {
            sb.append(" ").append(msg);
        }
        if (offendingToken != null) {
            sb.append(String.format(" offending token '%s'", offendingToken));
        }
        if (location != null) {
            sb.append(String.format(" at line %d column %d", location.getLine(), location.getColumn()));
        }
        return sb.toString();
    }

    public InvalidSyntaxError toInvalidSyntaxError() {
        List<SourceLocation> sourceLocations = location == null ? null : Collections.singletonList(location);
        return new InvalidSyntaxError(sourceLocations, message, sourcePreview, offendingToken);
    }


    @Override
    public String getMessage() {
        return message;
    }

    public SourceLocation getLocation() {
        return location;
    }

    public String getSourcePreview() {
        return sourcePreview;
    }

    public String getOffendingToken() {
        return offendingToken;
    }

}

