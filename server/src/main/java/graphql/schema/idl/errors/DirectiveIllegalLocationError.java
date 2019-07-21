package graphql.schema.idl.errors;

import graphql.language.DirectiveDefinition;
import graphql.language.Node;

import static java.lang.String.format;

public class DirectiveIllegalLocationError extends BaseError {

    public DirectiveIllegalLocationError(Node element, String elementName, String directiveName, String locationName) {
        super(element,
                format("'%s' %s tried to use a directive '%s' in the '%s' location but that is illegal",
                        elementName, BaseError.lineCol(element), directiveName, locationName
                ));
    }

    public DirectiveIllegalLocationError(DirectiveDefinition element, String locationName) {
        super(element,
                format("'%s' %s tried to use a location '%s' but that is illegal",
                        element.getName(), BaseError.lineCol(element), locationName
                ));
    }
}
