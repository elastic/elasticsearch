package graphql.validation.rules;


import graphql.language.Argument;
import graphql.schema.GraphQLArgument;
import graphql.validation.AbstractRule;
import graphql.validation.ArgumentValidationUtil;
import graphql.validation.ValidationContext;
import graphql.validation.ValidationErrorCollector;
import graphql.validation.ValidationErrorType;

public class ArgumentsOfCorrectType extends AbstractRule {

    public ArgumentsOfCorrectType(ValidationContext validationContext, ValidationErrorCollector validationErrorCollector) {
        super(validationContext, validationErrorCollector);
    }

    @Override
    public void checkArgument(Argument argument) {
        GraphQLArgument fieldArgument = getValidationContext().getArgument();
        if (fieldArgument == null) return;
        ArgumentValidationUtil validationUtil = new ArgumentValidationUtil(argument);
        if (!validationUtil.isValidLiteralValue(argument.getValue(), fieldArgument.getType(), getValidationContext().getSchema())) {
            addError(ValidationErrorType.WrongType, argument.getSourceLocation(), validationUtil.getMessage());
        }
    }
}
