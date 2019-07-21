package graphql.schema.idl.errors;

import graphql.language.FieldDefinition;
import graphql.language.InterfaceTypeDefinition;
import graphql.language.ObjectTypeDefinition;

import static java.lang.String.format;

public class MissingInterfaceFieldArgumentsError extends BaseError {
    public MissingInterfaceFieldArgumentsError(String typeOfType, ObjectTypeDefinition objectTypeDef, InterfaceTypeDefinition interfaceTypeDef, FieldDefinition objectFieldDef) {
        super(objectTypeDef, format("The %s type '%s' %s field '%s' does not have the same number of arguments as specified via interface '%s' %s",
                typeOfType, objectTypeDef.getName(), lineCol(objectTypeDef), objectFieldDef.getName(), interfaceTypeDef.getName(), lineCol(interfaceTypeDef)));
    }
}
