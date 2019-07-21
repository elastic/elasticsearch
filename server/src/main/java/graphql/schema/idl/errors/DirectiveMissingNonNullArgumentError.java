package graphql.schema.idl.errors;

import graphql.language.Node;

import static java.lang.String.format;

public class DirectiveMissingNonNullArgumentError extends BaseError {

    public DirectiveMissingNonNullArgumentError(Node element, String elementName, String directiveName, String argumentName) {
        super(element,
                format("'%s' %s failed to provide a value for the non null argument '%s' on directive '%s'",
                        elementName, BaseError.lineCol(element), argumentName, directiveName
                ));
    }
}
