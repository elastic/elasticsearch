package graphql.schema.idl.errors;

import graphql.language.AbstractNode;
import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.TypeDefinition;

import static java.lang.String.format;

public class TypeExtensionFieldRedefinitionError extends BaseError {

    public TypeExtensionFieldRedefinitionError(TypeDefinition typeDefinition, FieldDefinition fieldDefinition) {
        super(typeDefinition,
                formatMessage(typeDefinition, fieldDefinition.getName(), fieldDefinition));
    }

    public TypeExtensionFieldRedefinitionError(TypeDefinition typeDefinition, InputValueDefinition fieldDefinition) {
        super(typeDefinition,
                formatMessage(typeDefinition, fieldDefinition.getName(), fieldDefinition));
    }

    private static String formatMessage(TypeDefinition typeDefinition, String fieldName, AbstractNode<?> fieldDefinition) {
        return format("'%s' extension type %s tried to redefine field '%s' %s",
                typeDefinition.getName(), BaseError.lineCol(typeDefinition), fieldName, BaseError.lineCol(fieldDefinition)
        );
    }
}
