package graphql.schema.idl.errors;

import graphql.language.Type;
import graphql.language.TypeDefinition;

import static java.lang.String.format;

public class NotAnOutputTypeError extends BaseError {

    public NotAnOutputTypeError(Type rawType, TypeDefinition typeDefinition) {
        super(rawType, format("The type '%s' %s is not an output type, but was used to declare the output type of a field %s", typeDefinition.getName(), lineCol(typeDefinition), lineCol(rawType)));
    }
}
