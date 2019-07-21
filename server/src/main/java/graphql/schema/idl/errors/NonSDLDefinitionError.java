package graphql.schema.idl.errors;

import graphql.language.Definition;

import static java.lang.String.format;

public class NonSDLDefinitionError extends BaseError {

    public NonSDLDefinitionError(Definition definition) {
        super(definition, format("The schema definition text contains a non schema definition language (SDL) element '%s'",
                definition.getClass().getSimpleName(), lineCol(definition), lineCol(definition)));
    }
}
