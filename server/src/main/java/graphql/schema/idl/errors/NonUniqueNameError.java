package graphql.schema.idl.errors;

import graphql.language.DirectiveDefinition;
import graphql.language.EnumValueDefinition;
import graphql.language.FieldDefinition;
import graphql.language.InputObjectTypeDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.TypeDefinition;
import graphql.language.UnionTypeDefinition;

import static java.lang.String.format;

public class NonUniqueNameError extends BaseError {

    public NonUniqueNameError(TypeDefinition typeDefinition, FieldDefinition fieldDefinition) {
        super(typeDefinition, format("The type '%s' %s has declared a field with a non unique name '%s'",
                typeDefinition.getName(), lineCol(typeDefinition), fieldDefinition.getName()));
    }

    public NonUniqueNameError(TypeDefinition typeDefinition, InputValueDefinition inputValueDefinition) {
        super(typeDefinition, format("The type '%s' %s has declared an argument with a non unique name '%s'",
                typeDefinition.getName(), lineCol(typeDefinition), inputValueDefinition.getName()));
    }

    public NonUniqueNameError(InputObjectTypeDefinition typeDefinition, InputValueDefinition inputValueDefinition) {
        super(typeDefinition, format("The type '%s' %s has declared an input field with a non unique name '%s'",
                typeDefinition.getName(), lineCol(typeDefinition), inputValueDefinition.getName()));
    }

    public NonUniqueNameError(TypeDefinition typeDefinition, EnumValueDefinition enumValueDefinition) {
        super(typeDefinition, format("The type '%s' %s has declared an enum value with a non unique name '%s'",
                typeDefinition.getName(), lineCol(typeDefinition), enumValueDefinition.getName()));
    }

    public NonUniqueNameError(UnionTypeDefinition typeDefinition, String memberName) {
        super(typeDefinition, format("The type '%s' %s has declared an union member with a non unique name '%s'",
                typeDefinition.getName(), lineCol(typeDefinition), memberName));
    }

    public NonUniqueNameError(DirectiveDefinition typeDefinition, InputValueDefinition inputValueDefinition) {
        super(typeDefinition, format("The directive definition '%s' %s has declared an argument with a non unique name '%s'",
                typeDefinition.getName(), lineCol(typeDefinition), inputValueDefinition.getName()));
    }

}
