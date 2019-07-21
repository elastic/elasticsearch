package graphql.schema.idl.errors;

import graphql.language.EnumValueDefinition;
import graphql.language.TypeDefinition;

import static java.lang.String.format;

public class TypeExtensionEnumValueRedefinitionError extends BaseError {

    public TypeExtensionEnumValueRedefinitionError(TypeDefinition typeDefinition, EnumValueDefinition enumValueDefinition) {
        super(typeDefinition,
                format("'%s' extension type %s tried to redefine enum value '%s' %s",
                        typeDefinition.getName(), BaseError.lineCol(typeDefinition), enumValueDefinition.getName(), BaseError.lineCol(enumValueDefinition)
                ));
    }
}
