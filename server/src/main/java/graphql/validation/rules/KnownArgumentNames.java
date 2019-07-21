package graphql.validation.rules;

import graphql.language.Argument;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLFieldDefinition;
import graphql.validation.AbstractRule;
import graphql.validation.ValidationContext;
import graphql.validation.ValidationErrorCollector;
import graphql.validation.ValidationErrorType;


public class KnownArgumentNames extends AbstractRule {

    public KnownArgumentNames(ValidationContext validationContext, ValidationErrorCollector validationErrorCollector) {
        super(validationContext, validationErrorCollector);
    }


    @Override
    public void checkArgument(Argument argument) {
        GraphQLDirective directiveDef = getValidationContext().getDirective();
        if (directiveDef != null) {
            GraphQLArgument directiveArgument = directiveDef.getArgument(argument.getName());
            if (directiveArgument == null) {
                String message = String.format("Unknown directive argument %s", argument.getName());
                addError(ValidationErrorType.UnknownDirective, argument.getSourceLocation(), message);
            }

            return;
        }

        GraphQLFieldDefinition fieldDef = getValidationContext().getFieldDef();
        if (fieldDef == null) return;
        GraphQLArgument fieldArgument = fieldDef.getArgument(argument.getName());
        if (fieldArgument == null) {
            String message = String.format("Unknown field argument %s", argument.getName());
            addError(ValidationErrorType.UnknownArgument, argument.getSourceLocation(), message);
        }
    }
}
