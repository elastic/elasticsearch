package graphql.schema.idl.errors;

import graphql.language.TypeDefinition;
import graphql.language.TypeName;

import static java.lang.String.format;

public class MissingInterfaceTypeError extends BaseError {

    public MissingInterfaceTypeError(String typeOfType, TypeDefinition typeDefinition, TypeName typeName) {
        super(typeDefinition, format("The %s type '%s' is not present when resolving type '%s' %s",
                typeOfType, typeName.getName(), typeDefinition.getName(), lineCol(typeDefinition)));
    }
}
