package graphql;

import graphql.execution.instrumentation.DocumentAndVariables;
import graphql.language.Document;
import graphql.parser.InvalidSyntaxException;

import java.util.Map;

@Internal
public class ParseResult {
    private final DocumentAndVariables documentAndVariables;
    private final InvalidSyntaxException exception;

    public ParseResult(DocumentAndVariables documentAndVariables, InvalidSyntaxException exception) {
        this.documentAndVariables = documentAndVariables;
        this.exception = exception;
    }

    public  boolean isFailure() {
        return documentAndVariables == null;
    }

    public  Document getDocument() {
        return documentAndVariables.getDocument();
    }

    public  Map<String, Object> getVariables() {
        return documentAndVariables.getVariables();
    }

    public  InvalidSyntaxException getException() {
        return exception;
    }

    public  static ParseResult of(DocumentAndVariables document) {
        return new ParseResult(document, null);
    }

    public  static ParseResult ofError(InvalidSyntaxException e) {
        return new ParseResult(null, e);
    }
}
