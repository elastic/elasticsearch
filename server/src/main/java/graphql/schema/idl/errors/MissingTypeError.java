package graphql.schema.idl.errors;

import graphql.language.Node;
import graphql.language.TypeDefinition;
import graphql.language.TypeName;

import static java.lang.String.format;

public class MissingTypeError extends BaseError {

    public MissingTypeError(String typeOfType, TypeDefinition typeDefinition, TypeName typeName) {
        super(typeDefinition, format("The %s type '%s' is not present when resolving type '%s' %s",
                typeOfType, typeName.getName(), typeDefinition.getName(), lineCol(typeDefinition)));
    }

    public MissingTypeError(String typeOfType, Node node, String name, TypeName typeName) {
        super(node, format("The %s type '%s' is not present when resolving type '%s' %s",
                typeOfType, typeName.getName(), name, lineCol(node)));
    }
}
