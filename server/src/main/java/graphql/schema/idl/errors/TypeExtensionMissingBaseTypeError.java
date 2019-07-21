package graphql.schema.idl.errors;

import graphql.language.TypeDefinition;

import static java.lang.String.format;

public class TypeExtensionMissingBaseTypeError extends BaseError {

    public TypeExtensionMissingBaseTypeError(TypeDefinition typeExtensionDefinition) {
        super(typeExtensionDefinition,
                format("The extension '%s' type %s is missing its base underlying type",
                        typeExtensionDefinition.getName(), BaseError.lineCol(typeExtensionDefinition)
                ));
    }
}
